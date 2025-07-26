use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

use crate::keydir::InMemoryEntry;

pub struct StdKeyDir {
    // No ARC because KeyDir is meant to be used in a struct which will be wrapped into an
    // ARC.
    inner: RwLock<HashMap<Vec<u8>, InMemoryEntry>>
}

impl StdKeyDir {
    pub fn new() -> Self {
        StdKeyDir {
            inner: RwLock::new(HashMap::new())
        }
    }

    // TODO: we use io result for now. We might want to use more specific error types later
    pub fn get(&self, key: Vec<u8>) -> io::Result<Option<InMemoryEntry>> {
        let guard = self.inner.read().map_err(|e| io::Error::other(format!("keydir read error: {e}")))?;

        Ok(guard.get(&key).cloned())
    }

    fn insert(&self, key: Vec<u8>, entry: InMemoryEntry) -> io::Result<()> {
        let mut guard = self.inner.write().map_err(|e| io::Error::other(format!("keydir write error: {e}")))?;
        guard.insert(key, entry);
        Ok(())
    }

    fn remove(&self, key: Vec<u8>) -> io::Result<()> {
        let mut guard = self.inner.write().map_err(|e| io::Error::other(format!("keydir write error: {e}")))?;
        guard.remove(&key);
        Ok(())
    }
}