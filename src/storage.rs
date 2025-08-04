use crate::{EngineOptions, current_time_millis, InMemoryEntry};
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;
use tracing::{debug, error};
use crate::reader::FileReader;
use crate::writer::WriteHandler;

pub struct Engine {
    /// Various configuration options for the engine.
    options: EngineOptions,
    /// keydir, as defined in bitsect paper
    key_dir: RwLock<HashMap<Vec<u8>, InMemoryEntry>>,
    /// FileWriter own the active file. It is used to write new data to the active file.
    /// It is also used to close the active file when it reaches the maximum size or when
    /// the engine is dropped.
    /// The active file is a wrapper around the opened active file. It allows directly writing
    /// to the file without needing to open it every time.
    /// The file is closed when the engine is dropped.
    write_handler: WriteHandler,
    /// Datafiles is a cache of the opened data files. The key is the file number, and the value is
    /// a FileReader that allows reading from the file. FileReader can be thought as a file
    /// descriptor. No ARC because the engine itself would be wrapped in an ARC.
    data_files: RwLock<BTreeMap<usize, FileReader>>,
}

impl Engine
{
    /// Creates a new engine. Please note that the active file needs to also be cached in
    /// the data_files map, this is why this method takes the ID and a FD to the active file.
    pub fn new(options: EngineOptions, active_file_reader: FileReader, active_file_id: usize, file_writer: WriteHandler) -> Self {
        let mut data_files = BTreeMap::new();
        data_files.insert(active_file_id, active_file_reader);

        Self {
            options,
            key_dir: RwLock::new(HashMap::new()),
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
            Ok(w_result) => {
                let entry = InMemoryEntry {
                    file_id: w_result.written_file_id,
                    value_size: value.len(),
                    value_offset: w_result.write_offset,
                    timestamp: current_time_millis(),
                };
                self.key_dir.write().expect("Mutex Poisoned").insert(Vec::from(key), entry);

                // If there is a new active file, add it to the ro data file cache
                if let Some((file, file_id)) = w_result.new_active_file {
                    self.data_files.write().expect("mutex poisoned").insert(file_id, file.into());
                }
            },
            Err(e) => {
                // TODO count write failure rate and act
                // because too many failures means storage issue
                error!("failed to write to file: {e:?}");
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // entry is inexpensive to clone
        let entry = self.key_dir.read().expect("mutex poisoned").get(key).cloned()?;
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
                self.key_dir.write().expect("Mutex Poisoned").remove(key);
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
        self.key_dir.read().expect("mutex poisoned").keys().cloned().collect()
    }

    pub fn sync(&self) {
        unimplemented!()
    }

    fn file_id(&self) -> usize {
        self.write_handler.file_id()
    }
}

