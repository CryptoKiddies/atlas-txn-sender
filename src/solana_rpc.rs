use solana_sdk::clock::UnixTimestamp;
use tonic::async_trait;

#[derive(Clone, Copy)]
pub enum TxnCommitment {
    Processed,
    Confirmed,
}

impl From<TxnCommitment> for yellowstone_grpc_proto::geyser::CommitmentLevel {
    fn from(commitment: TxnCommitment) -> Self {
        match commitment {
            TxnCommitment::Processed => Self::Processed,
            TxnCommitment::Confirmed => Self::Confirmed,
        }
    }
}

impl From<TxnCommitment> for i32 {
    fn from(commitment: TxnCommitment) -> Self {
        let commitment_level: yellowstone_grpc_proto::geyser::CommitmentLevel = commitment.into();
        commitment_level.into()
    }
}

#[async_trait]
pub trait SolanaRpc: Send + Sync {
    fn get_next_slot(&self) -> Option<u64>;

    // return block_time if confirmed, None otherwise
    async fn verify_transaction(
        &self,
        signatures: String,
        commitment: TxnCommitment,
    ) -> Option<UnixTimestamp>;
}
