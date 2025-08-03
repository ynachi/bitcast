use crate::fs_reader::FileReader;
use crate::fs_writer::FileWriter;
use crate::keydir::{InMemoryEntry, KeyDir};
use crate::{EngineOptions, current_time_millis};
use std::collections::BTreeMap;
use std::sync::RwLock;
use tracing::{debug, error};

pub struct Engine<T: KeyDir, F: FileReader, W: FileWriter> {
    /// Various configuration options for the engine.
    options: EngineOptions,
    key_dir: T,
    /// FileWriter own the active file. It is used to write new data to the active file.
    /// It is also used to close the active file when it reaches the maximum size or when
    /// the engine is dropped.
    /// The active file is a wrapper around the opened active file. It allows directly writing
    /// to the file without needing to open it every time.
    /// The file is closed when the engine is dropped.
    write_handler: W,
    /// Datafiles is a cache of the opened data files. The key is the file number, and the value is
    /// a FileReader that allows reading from the file. FileReader can be thought as a file
    /// descriptor. No ARC because the engine itself would be wrapped in an ARC.
    data_files: RwLock<BTreeMap<usize, F>>,
}

impl<T, F, W> Engine<T, F, W>
where
    T: KeyDir,
    F: FileReader,
    W: FileWriter,
{
    pub fn new(options: EngineOptions, key_dir: T, file_reader: F, file_writer: W) -> Self {
        Self {
            options,
            key_dir,
            write_handler: file_writer,
            data_files: RwLock::new(BTreeMap::new()),
        }
    }

    /// If the engine existed before, this method will load necessary data
    pub fn start(&mut self) {
        unimplemented!()
    }

    pub fn stop(&self) {
        unimplemented!()
    }

    pub fn put(&mut self, key: &[u8], value: &[u8])  {
        match self.write_handler.write(key, value) {
            Ok(offset) => {
                let entry = InMemoryEntry {
                    file_id: self.write_handler.file_id(),
                    value_size: value.len(),
                    value_offset: offset,
                    timestamp: current_time_millis(),
                };
                self.key_dir.insert(key, entry);
            },
            Err(e) => {
                // TODO count write failure rate and act
                // because too many failures means storage issue
                error!("failed to write to file: {e:?}");
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.key_dir.get(key)?;
        let file_id = entry.file_id;
        debug!(
            "reading value for {} from file: {file_id}",
            String::from_utf8_lossy(key)
        );
        let guard = self.data_files.read().expect("Mutex poisoned");
        let reader = guard
            .get(&file_id)
            .expect("file not found in keydir but it should be");

        match reader.read_at(entry.value_offset as u64, entry.value_size) {
            Ok(value) => Some(value),
            Err(e) => {
                error!("Failed to read value from file: {e:?}");
                None
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        match self.write_handler.write(key, &[]) {
            Ok(_) => {
                self.key_dir.remove(key);
            },
            Err(e) => {
                // TODO count write failure rate and act
                error!("failed to write to file: {e:?}");
            }
        }
    }

    pub fn merge(&self) {
        unimplemented!()
    }

    pub fn list_keys(&self) -> Vec<Vec<u8>> {
        self.key_dir.keys()
    }

    pub fn sync(&self) {
        unimplemented!()
    }
}
