use cadence_macros::{statsd_count, statsd_gauge, statsd_time};
use solana_client::{
    connection_cache::ConnectionCache, 
    rpc_client::RpcClient,
    nonblocking::tpu_connection::TpuConnection
};
use solana_program_runtime::compute_budget::{ComputeBudget, MAX_COMPUTE_UNIT_LIMIT};
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::Hash, signature::Signature, transaction::{TransactionError, VersionedTransaction}
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
    str::FromStr,
};
use tokio::{
    runtime::{Builder, Runtime}, sync::RwLock, time::{sleep, timeout}
};
use tonic::async_trait;
use tracing::{error, warn};
use std::sync::atomic::{AtomicUsize, Ordering};


use crate::{
    invalid_blockhash_cache::InvalidBlockhashCache, leader_tracker::LeaderTracker, solana_rpc::{SolanaRpc, TxnCommitment}, transaction_store::{get_signature, TransactionData, TransactionStore}
};
use solana_program_runtime::compute_budget::DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT;
use solana_sdk::borsh0_10::try_from_slice_unchecked;
use solana_sdk::compute_budget::ComputeBudgetInstruction;

const RETRY_COUNT_BINS: [i32; 6] = [0, 1, 2, 5, 10, 25];
const MAX_RETRIES_BINS: [i32; 5] = [0, 1, 5, 10, 30];
const MAX_TIMEOUT_SEND_DATA: Duration = Duration::from_millis(500);
const MAX_TIMEOUT_SEND_DATA_BATCH: Duration = Duration::from_millis(500);
const SEND_TXN_RETRIES: usize = 10;

struct TransactionCheckResult {
    is_processed: bool,
    is_invalid_blockhash: bool
}

#[async_trait]
pub trait TxnSender: Send + Sync {
    fn send_transaction(&self, txn: TransactionData);
    fn send_transaction_bundle(&self, transactions: Vec<TransactionData>);
}

pub struct TxnSenderImpl {
    leader_tracker: Arc<dyn LeaderTracker>,
    transaction_store: Arc<dyn TransactionStore>,
    connection_cache: Arc<ConnectionCache>,
    solana_rpc: Arc<dyn SolanaRpc>,
    rpc_client: Arc<RpcClient>,
    invalid_blockhash_cache: Arc<RwLock<InvalidBlockhashCache>>,
    txn_sender_runtime: Arc<Runtime>,
    txn_send_retry_interval_seconds: usize,
    max_retry_queue_size: Option<usize>,
}

impl TxnSenderImpl {
    pub fn new(
        leader_tracker: Arc<dyn LeaderTracker>,
        transaction_store: Arc<dyn TransactionStore>,
        connection_cache: Arc<ConnectionCache>,
        solana_rpc: Arc<dyn SolanaRpc>,
        rpc_client: Arc<RpcClient>,
        invalid_blockhash_cache: Arc<RwLock<InvalidBlockhashCache>>,
        txn_sender_threads: usize,
        txn_send_retry_interval_seconds: usize,
        max_retry_queue_size: Option<usize>,
    ) -> Self {
        let txn_sender_runtime = Builder::new_multi_thread()
            .worker_threads(txn_sender_threads)
            .enable_all()
            .build()
            .unwrap();
        let txn_sender = Self {
            leader_tracker,
            transaction_store,
            connection_cache,
            solana_rpc,
            rpc_client,
            invalid_blockhash_cache,
            txn_sender_runtime: Arc::new(txn_sender_runtime),
            txn_send_retry_interval_seconds,
            max_retry_queue_size,
        };
        txn_sender.retry_transactions();
        txn_sender
    }

    fn retry_transactions(&self) {
        let leader_tracker = self.leader_tracker.clone();
        let transaction_store = self.transaction_store.clone();
        let connection_cache = self.connection_cache.clone();
        let txn_sender_runtime = self.txn_sender_runtime.clone();
        let txn_send_retry_interval_seconds = self.txn_send_retry_interval_seconds.clone();
        let max_retry_queue_size = self.max_retry_queue_size.clone();
        tokio::spawn(async move {
            loop {
                let mut transactions_reached_max_retries = vec![];
                let transaction_map = transaction_store.get_transactions();
                let queue_length = transaction_map.len();
                statsd_gauge!("transaction_retry_queue_length", queue_length as u64);

                // Shed transactions by retry_count, if necessary.
                if let Some(max_size) = max_retry_queue_size {
                    if queue_length > max_size {
                        warn!(
                            "Transaction retry queue length is over the limit of {}: {}. Load shedding transactions with highest retry count.", 
                            max_size,
                            queue_length
                        );
                        let mut transactions: Vec<(String, TransactionData)> = transaction_map
                            .iter()
                            .map(|x| (x.key().to_owned(), x.value().to_owned()))
                            .collect();
                        transactions.sort_by(|(_, a), (_, b)| a.retry_count.cmp(&b.retry_count));
                        let transactions_to_remove = transactions[(max_size + 1)..].to_vec();
                        for (signature, _) in transactions_to_remove {
                            transaction_store.remove_transaction(signature.clone());
                            transaction_map.remove(&signature);
                        }
                        let records_dropped = queue_length - max_size;
                        statsd_gauge!("transactions_retry_queue_dropped", records_dropped as u64);
                    }
                }

                let mut wire_transactions = vec![];
                for mut transaction_data in transaction_map.iter_mut() {
                    wire_transactions.push(transaction_data.wire_transaction.clone());
                    if transaction_data.retry_count >= transaction_data.max_retries {
                        transactions_reached_max_retries
                            .push(get_signature(&transaction_data).unwrap());
                    } else {
                        transaction_data.retry_count += 1;
                    }
                }
                for wire_transaction in wire_transactions.iter() {
                    let mut leader_num = 0;
                    for leader in leader_tracker.get_leaders() {
                        if leader.tpu_quic.is_none() {
                            error!("leader {:?} has no tpu_quic", leader);
                            continue;
                        }
                        let connection_cache = connection_cache.clone();
                        let sent_at = Instant::now();
                        let leader = Arc::new(leader.clone());
                        let wire_transaction = wire_transaction.clone();
                        txn_sender_runtime.spawn(async move {
                        // retry unless its a timeout
                        for i in 0..SEND_TXN_RETRIES {
                            let conn = connection_cache 
                                .get_nonblocking_connection(&leader.tpu_quic.unwrap());
                            if let Ok(result) = timeout(MAX_TIMEOUT_SEND_DATA_BATCH, conn.send_data(&wire_transaction)).await {
                                if let Err(e) = result {
                                    if i == SEND_TXN_RETRIES-1 {
                                        error!(
                                            retry = "true",
                                            "Failed to send transaction batch to {:?}: {}",
                                            leader, e
                                        );
                                        statsd_count!("transaction_send_error", 1, "retry" => "true", "last_attempt" => "true");
                                    } else {
                                        statsd_count!("transaction_send_error", 1, "retry" => "true", "last_attempt" => "false");
                                    }
                                } else {
                                    let leader_num_str = leader_num.to_string();
                                    statsd_time!(
                                        "transaction_received_by_leader",
                                        sent_at.elapsed(), "leader_num" => &leader_num_str, "api_key" => "not_applicable", "retry" => "true");
                                    return;
                                }
                            } else {
                                // Note: This is far too frequent to log. It will fill the disks on the host and cost too much on DD.
                                statsd_count!("transaction_send_timeout", 1);
                            }
                        }
                    });
                        leader_num += 1;
                    }
                }
                // remove transactions that reached max retries
                for signature in transactions_reached_max_retries {
                    let _ = transaction_store.remove_transaction(signature);
                    statsd_count!("transactions_reached_max_retries", 1);
                }
                sleep(Duration::from_secs(txn_send_retry_interval_seconds as u64)).await;
            }
        });
    }

    async fn detect_invalid_txn(
                rpc_client: &RpcClient,
                signature_typed: Signature,
                blockhash: &str
            ) -> TransactionCheckResult { 
            let mut check_attempts = 0;
            const MAX_CHECK_ATTEMPTS: usize = 20;
            const CHECK_INTERVAL_MS: u64 = 40;

            let mut is_processed = false;
            let mut is_invalid_blockhash = false;

            // Quick polling loop for immediate rejection or processing detection
            while check_attempts < MAX_CHECK_ATTEMPTS && !is_invalid_blockhash {
                match rpc_client.get_signature_statuses(&[signature_typed]) {
                    Ok(response) => {
                        dbg!(response.value.first());
                
                        if let Some(Some(status)) = response.value.first() {
                            dbg!("Status value: {:?}", status);
                            if let Some(err) = &status.err {
                                if matches!(err, TransactionError::BlockhashNotFound) {
                                    warn!("Detected invalid blockhash early: {:?}", blockhash);
                                    is_invalid_blockhash = true;
                                } else {
                                    warn!("Transaction failed with error: {:?}", err);
                                }
                                break;
                            } else if status.slot > 0 {
                                warn!("Transaction processed at slot: {:?}", status.slot);
                                // Transaction has been processed if we have a slot value
                                is_processed = true;
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        if err.to_string().to_lowercase().contains("blockhash not found") {
                        // TODO: remove this once we're done testing
                            warn!("Blockhash not found error response: {:?}", blockhash);
                            is_invalid_blockhash = true;
                        } else {
                            warn!("Error checking transaction status: {:?}", err);
                        }
                        break;
                    }
                }
                sleep(Duration::from_millis(CHECK_INTERVAL_MS)).await;
                check_attempts += 1;
        }
        if !is_invalid_blockhash && !is_processed {
             is_invalid_blockhash = match rpc_client.is_blockhash_valid(
                &Hash::from_str(&blockhash).unwrap(),
                CommitmentConfig::processed()
            ) {
                Ok(false) => {
                    warn!("Final gate check. Blockhash is invalid: {:?}", blockhash);
                    true
                },
                _ => false
            }
        }
       TransactionCheckResult { 
            is_processed,
            is_invalid_blockhash
        }
    }

    fn track_transaction(transaction_data: &TransactionData, transaction_store: Arc<dyn TransactionStore>, 
        solana_rpc: Arc<dyn SolanaRpc>, rpc_client: Arc<RpcClient>, invalid_blockhash_cache: Arc<RwLock<InvalidBlockhashCache>>, txn_sender_runtime: &Runtime) {
        let sent_at = transaction_data.sent_at.clone();
        let signature = get_signature(transaction_data);
        if signature.is_none() {
            return;
        }
        let signature = signature.unwrap();
        // TODO: redesign to prevent txn redundance - this allows resends of same txn
        transaction_store.add_transaction(transaction_data.clone());
        let PriorityDetails {
            fee,
            cu_limit,
            priority,
        } = compute_priority_details(&transaction_data.versioned_transaction);
        let priority_fees_enabled = (fee > 0).to_string();
        let api_key = transaction_data
            .request_metadata
            .clone()
            .map(|m| m.api_key.clone())
            .unwrap_or("none".to_string());
        let blockhash = transaction_data.versioned_transaction.message.recent_blockhash().to_string();
        
        txn_sender_runtime.spawn(async move {
            let signature_typed = Signature::from_str(&signature).unwrap();
            // Early error detection for invalid blockhash
            let TransactionCheckResult { is_processed, is_invalid_blockhash} = 
                Self::detect_invalid_txn(&rpc_client, signature_typed, &blockhash).await;

            if is_invalid_blockhash {
                invalid_blockhash_cache.write().await.set_invalid(&blockhash);
                warn!("Invalid blockhash detected for transaction {}, updating cache", signature);
            }

            // Skip confirmation if we already detected an invalid blockhash or checked on slot for a bit
            let confirmed_at = if is_invalid_blockhash || !is_processed {
                None
            } else {
                solana_rpc.verify_transaction(signature.clone(), TxnCommitment::Confirmed).await
            };

            // Remove from transaction store and collect metrics
            let transaction_data = transaction_store.remove_transaction(signature);
            let mut retries = None;
            let mut max_retries = None;
            if let Some(transaction_data) = transaction_data {
                retries = Some(transaction_data.retry_count as i32);
                max_retries = Some(transaction_data.max_retries as i32);
            }
    
            let retries_tag = bin_counter_to_tag(retries, &RETRY_COUNT_BINS.to_vec());
            let max_retries_tag: String = bin_counter_to_tag(max_retries, &MAX_RETRIES_BINS.to_vec());
    
            // Collect metrics
            // We separate the retry metrics to reduce the cardinality with API key and price.
            let landed = if let Some(_) = confirmed_at {
                statsd_count!("transactions_landed", 1, "priority_fees_enabled" => &priority_fees_enabled, "retries" => &retries_tag, "max_retries_tag" => &max_retries_tag);
                statsd_count!("transactions_landed_by_key", 1, "api_key" => &api_key);
                statsd_time!("transaction_land_time", sent_at.elapsed(), "api_key" => &api_key, "priority_fees_enabled" => &priority_fees_enabled);
                "true"
            } else {
                statsd_count!("transactions_not_landed", 1, "priority_fees_enabled" => &priority_fees_enabled, "retries" => &retries_tag, "max_retries_tag" => &max_retries_tag);
                statsd_count!("transactions_not_landed_by_key", 1, "api_key" => &api_key);
                statsd_count!("transactions_not_landed_retries", 1, "priority_fees_enabled" => &priority_fees_enabled, "retries" => &retries_tag, "max_retries_tag" => &max_retries_tag);
                "false"
            };
            statsd_time!("transaction_priority", priority, "landed" => &landed);
            statsd_time!("transaction_priority_fee", fee, "landed" => &landed);
            statsd_time!("transaction_compute_limit", cu_limit as u64, "landed" => &landed);
        });
    }
}

pub struct PriorityDetails {
    pub fee: u64,
    pub cu_limit: u32,
    pub priority: u64,
}

pub fn compute_priority_details(transaction: &VersionedTransaction) -> PriorityDetails {
    let mut cu_limit = DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT;
    let mut compute_budget = ComputeBudget::new(DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT as u64);
    if let Err(_e) = transaction.sanitize() {
        return PriorityDetails {
            fee: 0,
            priority: 0,
            cu_limit,
        };
    }
    let instructions = transaction.message.instructions().iter().map(|ix| {
        match try_from_slice_unchecked(&ix.data) {
            Ok(ComputeBudgetInstruction::SetComputeUnitLimit(compute_unit_limit)) => {
                cu_limit = compute_unit_limit.min(MAX_COMPUTE_UNIT_LIMIT);
            }
            _ => {}
        }
        (
            transaction
                .message
                .static_account_keys()
                .get(usize::from(ix.program_id_index))
                .expect("program id index is sanitized"),
            ix,
        )
    });
    let compute_budget = compute_budget.process_instructions(instructions, true, true);
    match compute_budget {
        Ok(compute_budget) => PriorityDetails {
            fee: compute_budget.get_fee(),
            priority: compute_budget.get_priority(),
            cu_limit,
        },
        Err(_e) => PriorityDetails {
            fee: 0,
            priority: 0,
            cu_limit,
        },
    }
}

#[async_trait]
impl TxnSender for TxnSenderImpl {
    fn send_transaction(&self, transaction_data: TransactionData) {
        TxnSenderImpl::track_transaction(&transaction_data, self.transaction_store.clone(), self.solana_rpc.clone(), self.rpc_client.clone(), self.invalid_blockhash_cache.clone(), &self.txn_sender_runtime);

        let api_key = transaction_data
            .request_metadata
            .map(|m| m.api_key)
            .unwrap_or("none".to_string());
        let mut leader_num = 0;

        for leader in self.leader_tracker.get_leaders() {
            // TODO: Check a tpu legacy flag for allowed fallback to tpu regular
            if leader.tpu_quic.is_none() {
                error!("leader {:?} has no tpu_quic", leader);
                continue;
            }
            let connection_cache = self.connection_cache.clone();
            let wire_transaction = transaction_data.wire_transaction.clone();
            let api_key = api_key.clone();
            self.txn_sender_runtime.spawn(async move {
                for i in 0..SEND_TXN_RETRIES {
                    let conn =
                        connection_cache.get_nonblocking_connection(&leader.tpu_quic.unwrap());
                    if let Ok(result) = timeout(MAX_TIMEOUT_SEND_DATA, conn.send_data(&wire_transaction)).await {
                        if let Err(e) = result {
                            if i == SEND_TXN_RETRIES-1 {
                                error!(
                                    retry = "false",
                                    "Failed to send transaction to {:?}: {}",
                                    leader, e
                                );
                                statsd_count!("transaction_send_error", 1, "retry" => "false", "last_attempt" => "true");
                            } else {
                                statsd_count!("transaction_send_error", 1, "retry" => "false", "last_attempt" => "false");
                            }
                        } else {
                            let leader_num_str = leader_num.to_string();
                            statsd_time!(
                                "transaction_received_by_leader",
                                transaction_data.sent_at.elapsed(), "leader_num" => &leader_num_str, "api_key" => &api_key, "retry" => "false");
                            return;
                        }
                    } else {
                        // Note: This is far too frequent to log. It will fill the disks on the host and cost too much on DD.
                        statsd_count!("transaction_send_timeout", 1);
                    }
                }
            });
            leader_num += 1;
        }
    }

    // TODO: refactor shared logic with send_transaction
    fn send_transaction_bundle(&self, transactions: Vec<TransactionData>) {
        let connection_cache = self.connection_cache.clone();
        let leader_tracker = self.leader_tracker.clone();
        let solana_rpc = self.solana_rpc.clone();
        let txn_sender_runtime = self.txn_sender_runtime.clone();
        let invalid_blockhash_cache = self.invalid_blockhash_cache.clone();
        let transaction_store = self.transaction_store.clone();
        let rpc_client = self.rpc_client.clone();

        // TODO: remove logs
        tokio::spawn(async move { 
            for transaction_data in transactions {
                TxnSenderImpl::track_transaction(&transaction_data, transaction_store.clone(), solana_rpc.clone(), rpc_client.clone(), invalid_blockhash_cache.clone(), &txn_sender_runtime);

                let signature = get_signature(&transaction_data).unwrap();
                let leaders = leader_tracker.get_leaders();
                let total_leaders = leaders.len();
                warn!("sending transaction bundle with {} leaders", total_leaders);
                let failed_leaders = Arc::new(AtomicUsize::new(0));
                let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                let wire_transaction = transaction_data.wire_transaction.clone();
                let mut leader_num = 0;

                for leader in leaders {
                    if leader.tpu_quic.is_none() {
                        error!("leader {:?} has no tpu_quic", leader);
                        continue;
                    }
                    let connection_cache = connection_cache.clone();
                    let wire_transaction = wire_transaction.clone();
                    let leader = Arc::new(leader.clone());
                    let failed_leaders = failed_leaders.clone();
                    let tx = tx.clone();
                    let sent_at = transaction_data.sent_at.clone();
                    let leader_num_str = leader_num.to_string();

                    txn_sender_runtime.spawn(async move {
                        let mut succeeded = false;
                        for i in 0..SEND_TXN_RETRIES {
                            let conn = connection_cache.get_nonblocking_connection(&leader.tpu_quic.unwrap());
                            if let Ok(result) = timeout(MAX_TIMEOUT_SEND_DATA, conn.send_data(&wire_transaction)).await {
                                if result.is_ok() {
                                    warn!("transaction sent to leader {:?} successfully", leader);
                                    succeeded = true;
                                    statsd_time!(
                                        "transaction_received_by_leader",
                                        sent_at.elapsed(),
                                        "leader_num" => &leader_num_str,
                                        "api_key" => "not_applicable",
                                        "retry" => "false"
                                    );
                                    break;
                                } else if i == SEND_TXN_RETRIES-1 {
                                    error!(
                                        retry = "false",
                                        "Failed to send transaction to {:?}: {:?}",
                                        leader, result
                                    );
                                    statsd_count!("transaction_send_error", 1, "retry" => "false", "last_attempt" => "true");
                                }
                            } else {
                                statsd_count!("transaction_send_timeout", 1);
                            }
                        }
                        if !succeeded {
                            let count = failed_leaders.fetch_add(1, Ordering::SeqCst);
                            if count + 1 == total_leaders {
                                let _ = tx.send(1).await;
                            }
                        }
                    });
                    leader_num += 1;
                }

                let blockhash = transaction_data.versioned_transaction.message.recent_blockhash().to_string();
                let signature_typed = Signature::from_str(&signature).unwrap();
                let TransactionCheckResult { is_invalid_blockhash, ..} = Self::detect_invalid_txn(
                    &rpc_client,
                    signature_typed,
                    &blockhash
                ).await;
                
                if is_invalid_blockhash {
                    let _ = tx.send(2).await;
                }

                tokio::select!{
                    confirmation = solana_rpc.verify_transaction(signature.clone(), TxnCommitment::Confirmed) => {
                        if let Some(_) = confirmation {
                        } else {
                            warn!("transaction failed to confirm: {:?}", signature);
                            break;
                        }
                    }
                    Some(val) = rx.recv() => {
                        if val == 1 {
                            warn!("all leaders ({}) rejected transaction {}, failing fast", total_leaders, signature);
                        } else if val == 2 {
                            warn!("invalid blockhash {} detected for transaction {}, skipping confirmation", blockhash, signature);
                        }
                        break;
                    }
                }
            }
        });
    }
}

fn bin_counter_to_tag(counter: Option<i32>, bins: &Vec<i32>) -> String {
    if counter.is_none() {
        return "none".to_string();
    }
    let counter = counter.unwrap();

    // Iterate through the bins vector to find the appropriate bin
    let mut bin_start = "-inf".to_string();
    let mut bin_end = "inf".to_string();
    for bin in bins.iter().rev() {
        if counter >= *bin {
            bin_start = bin.to_string();
            break;
        }

        bin_end = bin.to_string();
    }
    format!("{}_{}", bin_start, bin_end)
}

#[test]
fn test_bin_counter() {
    let bins = vec![0, 1, 2, 5, 10, 25];
    assert_eq!(bin_counter_to_tag(None, &bins), "none");
    assert_eq!(bin_counter_to_tag(Some(-100), &bins), "-inf_0");
    assert_eq!(bin_counter_to_tag(Some(0), &bins), "0_1");
    assert_eq!(bin_counter_to_tag(Some(1), &bins), "1_2");
    assert_eq!(bin_counter_to_tag(Some(2), &bins), "2_5");
    assert_eq!(bin_counter_to_tag(Some(3), &bins), "2_5");
    assert_eq!(bin_counter_to_tag(Some(17), &bins), "10_25");
    assert_eq!(bin_counter_to_tag(Some(34), &bins), "25_inf");
}
