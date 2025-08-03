use std::collections::HashMap;
use std::sync::RwLock;

use crate::keydir::{InMemoryEntry, KeyDir};

pub struct StdKeyDir {
    // No ARC because KeyDir is meant to be used in a struct which will be wrapped into an
    // ARC.
    inner: RwLock<HashMap<Vec<u8>, InMemoryEntry>>,
}

impl StdKeyDir {
    pub fn new() -> Self {
        StdKeyDir {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for StdKeyDir {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyDir for StdKeyDir {
    fn get(&self, key: &[u8]) -> Option<InMemoryEntry> {
        let guard = self.inner.read().expect("Mutex poisoned");
        guard.get(key).cloned()
    }

    fn insert(&self, key: &[u8], entry: InMemoryEntry) -> Option<InMemoryEntry> {
        let mut guard = self.inner.write().expect("Mutex poisoned");
        guard.insert(key.to_vec(), entry)
    }

    fn remove(&self, key: &[u8]) -> Option<InMemoryEntry> {
        let mut guard = self.inner.write().expect("Mutex poisoned");
        guard.remove(key)
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        let guard = self.inner.read().expect("Mutex poisoned");
        guard.keys().cloned().collect()
    }
}
