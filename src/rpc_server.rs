use std::{sync::Arc, time::Instant};

use cadence_macros::{statsd_count, statsd_time};
use dashmap::DashSet;
use jsonrpsee::{
    core::{async_trait, RpcResult},
    proc_macros::rpc,
    types::ErrorObjectOwned,
};
use serde::Deserialize;
use solana_rpc_client_api::config::RpcSendTransactionConfig;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::UiTransactionEncoding;

use crate::{
    errors::invalid_request,
    transaction_store::{TransactionData, TransactionStore},
    txn_sender::TxnSender,
    vendor::solana_rpc::decode_and_deserialize,
};

// jsonrpsee does not make it easy to access http data,
// so creating this optional param to pass in metadata
#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all(serialize = "camelCase", deserialize = "camelCase"))]
pub struct RequestMetadata {
    pub api_key: String,
}

struct InvalidBlockhashCache {
    invalid_blockhashes: DashSet<String>,
}

impl InvalidBlockhashCache {
    fn new() -> Self {
        Self {
            invalid_blockhashes: DashSet::new(),
        }
    }

    fn is_invalid(&self, blockhash: &str) -> bool {
        self.invalid_blockhashes.contains(blockhash)
    }

    fn find_invalid_blockhash(&self, blockhashes: Vec<String>) -> bool {
        blockhashes.iter().any(|bh| self.invalid_blockhashes.contains(bh))
    }

    // TODO: being able to unset for edge cases
    fn set_invalid(&mut self, blockhash: &str) {
        self.invalid_blockhashes.insert(blockhash.to_string());
    }
}

#[rpc(server)]
pub trait AtlasTxnSender {
    #[method(name = "health")]
    async fn health(&self) -> String;
    #[method(name = "sendTransaction")]
    async fn send_transaction(
        &self,
        txn: String,
        params: RpcSendTransactionConfig,
        request_metadata: Option<RequestMetadata>,
    ) -> RpcResult<String>;
    #[method(name = "sendTransactionBundle")]
    async fn send_transaction_bundle(
        &self,
        transactions: Vec<String>,
        params: RpcSendTransactionConfig,
        request_metadata: Option<RequestMetadata>,
    ) -> RpcResult<Vec<String>>;
}

pub struct AtlasTxnSenderImpl {
    txn_sender: Arc<dyn TxnSender>,
    transaction_store: Arc<dyn TransactionStore>,
    max_txn_send_retries: usize,
    invalid_blockhash_cache: Arc<InvalidBlockhashCache>,
}

impl AtlasTxnSenderImpl {
    pub fn new(
        txn_sender: Arc<dyn TxnSender>,
        transaction_store: Arc<dyn TransactionStore>,
        max_txn_send_retries: usize,
    ) -> Self {
        Self {
            txn_sender,
            max_txn_send_retries,
            transaction_store,
            invalid_blockhash_cache: Arc::new(InvalidBlockhashCache::new()),
        }
    }

    fn validate_and_decode_transaction(
        &self,
        txn: String,
        params: &RpcSendTransactionConfig,
        request_metadata: &Option<RequestMetadata>,
    ) -> RpcResult<(TransactionData, String)> {
        let sent_at = Instant::now();
        let api_key = request_metadata
            .clone()
            .map(|m| m.api_key)
            .unwrap_or("none".to_string());
        statsd_count!("send_transaction", 1, "api_key" => &api_key);
        
        validate_send_transaction_params(params)?;
        
        let encoding = params.encoding.unwrap_or(UiTransactionEncoding::Base58);
        let binary_encoding = encoding.into_binary_encoding().ok_or_else(|| {
            invalid_request(&format!(
                "unsupported encoding: {encoding}. Supported encodings: base58, base64"
            ))
        })?;
        
        let (wire_transaction, versioned_transaction) =
            match decode_and_deserialize::<VersionedTransaction>(txn, binary_encoding) {
                Ok((wire_transaction, versioned_transaction)) => {
                    (wire_transaction, versioned_transaction)
                }
                Err(e) => {
                    return Err(invalid_request(&e.to_string()));
                }
            };

        let blockhash = versioned_transaction.message.recent_blockhash();
        if self.invalid_blockhash_cache.is_invalid(&blockhash.to_string()) {
            return Err(invalid_request("blockhash is invalid"));
        }
            
        let signature = versioned_transaction.signatures[0].to_string();
        if self.transaction_store.has_signature(&signature) {
            statsd_count!("duplicate_transaction", 1, "api_key" => &api_key);
            return Ok((TransactionData {
                wire_transaction,
                versioned_transaction,
                sent_at,
                retry_count: 0,
                max_retries: std::cmp::min(
                    self.max_txn_send_retries,
                    params.max_retries.unwrap_or(self.max_txn_send_retries),
                ),
                request_metadata: request_metadata.clone(),
            }, signature));
        }

        Ok((TransactionData {
            wire_transaction,
            versioned_transaction,
            sent_at,
            retry_count: 0,
            max_retries: std::cmp::min(
                self.max_txn_send_retries,
                params.max_retries.unwrap_or(self.max_txn_send_retries),
            ),
            request_metadata: request_metadata.clone(),
        }, signature))
    }
}

#[async_trait]
impl AtlasTxnSenderServer for AtlasTxnSenderImpl {
    async fn health(&self) -> String {
        "ok".to_string()
    }
    async fn send_transaction(
        &self,
        txn: String,
        params: RpcSendTransactionConfig,
        request_metadata: Option<RequestMetadata>,
    ) -> RpcResult<String> {
        let start = Instant::now();
        let api_key = request_metadata
        .as_ref()
        .map(|m| m.api_key.as_str())
        .unwrap_or("none");

        let (transaction, signature) = self.validate_and_decode_transaction(txn, &params, &request_metadata)?;
        self.txn_sender.send_transaction(transaction);

        statsd_time!(
            "send_transaction_time",
            start.elapsed(),
            "api_key" => api_key
        );
        Ok(signature)
    }
    async fn send_transaction_bundle(
        &self,
        transactions: Vec<String>,
        params: RpcSendTransactionConfig,
        request_metadata: Option<RequestMetadata>,
    ) -> RpcResult<Vec<String>> {
        let start = Instant::now();
        let api_key = request_metadata
        .as_ref()
        .map(|m| m.api_key.as_str())
        .unwrap_or("none");
        let mut transaction_data_vec = Vec::new();
        
        for txn in transactions {
            let (transaction, _) = self.validate_and_decode_transaction(txn, &params, &request_metadata)?;
            transaction_data_vec.push(transaction);
        }

        let res = self.txn_sender.send_transaction_bundle(transaction_data_vec).await;

        statsd_time!(
            "send_transaction_time",
            start.elapsed(),
            "api_key" => api_key
        );

        Ok(res)
    }
}

fn validate_send_transaction_params(
    params: &RpcSendTransactionConfig,
) -> Result<(), ErrorObjectOwned> {
    if !params.skip_preflight {
        return Err(invalid_request("running preflight check is not supported"));
    }
    Ok(())
}
