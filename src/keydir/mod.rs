mod standard;
pub use standard::StdKeyDir;

#[derive(Debug, Clone)]
pub struct InMemoryEntry {
    file_id: usize,
    value_size: usize,
    value_position: usize,
    timestamp: u64,
}

/// KeyDir is an in-memory structure to complement bitcast implementation.
/// We use a trait to allow the use of different hashmap implementations.
pub trait KeyDir {
    fn get(&self, key: &[u8]) -> Option<InMemoryEntry>;
    fn insert(&mut self, key: &[u8], value_size: usize, value_position: usize, timestamp: u64);
    fn remove(&mut self, key: &[u8]);
}
