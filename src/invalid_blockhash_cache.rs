use dashmap::DashSet;
use tracing::warn;

#[derive(Clone)]
pub struct InvalidBlockhashCache {
    invalid_blockhashes: DashSet<String>,
}

impl InvalidBlockhashCache {
    pub fn new() -> Self {
        Self {
            invalid_blockhashes: DashSet::new(),
        }
    }

    pub fn is_invalid(&self, blockhash: &str) -> bool {
        let is_invalid = self.invalid_blockhashes.contains(blockhash);
        // TODO: remove this once we're done testing
        if is_invalid {
            warn!(
                "Rejecting transaction with invalid blockhash: {}",
                blockhash
            );
        }
        is_invalid
    }

    pub fn find_invalid_blockhash(&self, blockhashes: Vec<String>) -> bool {
        blockhashes
            .iter()
            .any(|bh| self.invalid_blockhashes.contains(bh))
    }

    // TODO: being able to unset for edge cases
    pub fn set_invalid(&mut self, blockhash: &str) {
        warn!("Adding blockhash to invalid cache: {}", blockhash);
        self.invalid_blockhashes.insert(blockhash.to_string());
    }
}
