use dashmap::DashSet;

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
        self.invalid_blockhashes.contains(blockhash)
    }

    pub fn find_invalid_blockhash(&self, blockhashes: Vec<String>) -> bool {
        blockhashes
            .iter()
            .any(|bh| self.invalid_blockhashes.contains(bh))
    }

    // TODO: being able to unset for edge cases
    pub fn set_invalid(&mut self, blockhash: &str) {
        self.invalid_blockhashes.insert(blockhash.to_string());
    }
}
