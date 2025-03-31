use std::{sync::Arc, time::Instant};

use cadence_macros::{statsd_count, statsd_time};

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
    invalid_blockhash_cache::InvalidBlockhashCache,
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
        invalid_blockhash_cache: Arc<InvalidBlockhashCache>,
    ) -> Self {
        Self {
            txn_sender,
            max_txn_send_retries,
            transaction_store,
            invalid_blockhash_cache,
        }
    }

    fn validate_and_decode_transaction(
        &self,
        txn: String,
        params: &RpcSendTransactionConfig,
        request_metadata: &Option<RequestMetadata>,
        sent_at: Instant,
    ) -> RpcResult<(Option<TransactionData>, String)> {
        let api_key = request_metadata
            .as_ref()
            .map(|m| m.api_key.as_str())
            .unwrap_or("none");
        statsd_count!("send_transaction", 1, "api_key" => api_key);

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
        if self
            .invalid_blockhash_cache
            .is_invalid(&blockhash.to_string())
        {
            return Err(invalid_request("Blockhash is invalid"));
        }

        let signature = versioned_transaction.signatures[0].to_string();
        if self.transaction_store.has_signature(&signature) {
            statsd_count!("duplicate_transaction", 1, "api_key" => api_key);
            return Ok((None, signature));
        }

        Ok((
            Some(TransactionData {
                wire_transaction,
                versioned_transaction,
                sent_at,
                retry_count: 0,
                max_retries: std::cmp::min(
                    self.max_txn_send_retries,
                    params.max_retries.unwrap_or(self.max_txn_send_retries),
                ),
                request_metadata: request_metadata.clone(),
            }),
            signature,
        ))
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
        let sig;

        if let (Some(transaction), signature) =
            self.validate_and_decode_transaction(txn, &params, &request_metadata, start)?
        {
            self.txn_sender.send_transaction(transaction);
            sig = signature;
        } else {
            return Err(invalid_request("transaction is a duplicate or invalid"));
        }

        let api_key = request_metadata
            .as_ref()
            .map(|m| m.api_key.as_str())
            .unwrap_or("none");
        statsd_time!(
            "send_transaction_time",
            start.elapsed(),
            "api_key" => api_key
        );

        // TODO: add a status msg for redundant txn
        Ok(sig)
    }

    async fn send_transaction_bundle(
        &self,
        transactions: Vec<String>,
        params: RpcSendTransactionConfig,
        request_metadata: Option<RequestMetadata>,
    ) -> RpcResult<Vec<String>> {
        let start = Instant::now();
        let mut transaction_data_vec = Vec::new();

        // TODO: parallelize validations?
        for txn in transactions {
            let (transaction, _) =
                self.validate_and_decode_transaction(txn, &params, &request_metadata, start)?;
            if transaction.is_none() {
                return Err(invalid_request(
                    "Bundle contains duplicate or invalid transactions.",
                ));
            }
            transaction_data_vec.push(transaction.unwrap());
        }

        let signatures = self
            .txn_sender
            .send_transaction_bundle(transaction_data_vec)
            .await;
        let api_key = request_metadata
            .as_ref()
            .map(|m| m.api_key.as_str())
            .unwrap_or("none");
        statsd_time!(
            "send_transaction_time",
            start.elapsed(),
            "api_key" => api_key
        );

        Ok(signatures)
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
