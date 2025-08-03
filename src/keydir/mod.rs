mod standard;

pub use standard::StdKeyDir;
use std::io;

#[derive(Debug, Clone)]
pub struct InMemoryEntry {
    pub file_id: usize,
    pub value_size: usize,
    pub value_offset: usize,
    pub timestamp: u64,
}

/// KeyDir is an in-memory structure to complement bitcast implementation.
/// We use a trait to allow the use of different hashmap implementations.
pub trait KeyDir {
    fn get(&self, key: &[u8]) -> Option<InMemoryEntry>;
    fn insert(&self, key: &[u8], entry: InMemoryEntry) -> Option<InMemoryEntry>;
    fn remove(&self, key: &[u8]) -> Option<InMemoryEntry>;
    fn keys(&self) -> Vec<Vec<u8>>;
}
