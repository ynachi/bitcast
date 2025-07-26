use crate::fs_reader::{FileReader, SeekReader};
use crate::fs_writer::{FileWriter, MutexFileWriter};
use crate::keydir::{KeyDir, StdKeyDir};
use crate::EngineOptions;
use std::collections::BTreeMap;
use std::io;
use std::sync::RwLock;

pub struct Engine <T: KeyDir, F: FileReader, W: FileWriter> {
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
    data_files: RwLock<BTreeMap<u16, F>>,
    /// Read handler is used to read data on the file storage. They can be a different implementation
    /// of the reader.
    read_handler: F,
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
            read_handler: file_reader,
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

    pub fn put(&self, _key: &[u8], _value: &[u8]) {
        unimplemented!()
    }

    pub fn get(&self, _key: &[u8]) -> Option<Vec<u8>> {
        unimplemented!()
    }

    pub fn delete(&self, _key: &[u8]) {
        unimplemented!()
    }

    pub fn merge(&self) {
        unimplemented!()
    }

    pub fn list_keys(&self) -> Vec<Vec<u8>> {
        unimplemented!()
    }

    pub fn sync(&self) {
        unimplemented!()
    }
}
