use std::collections::BTreeMap;
use std::path::PathBuf;
use crate::keydir;
use crate::fs_reader::FileReader;
use crate::fs_writer::FileWriter;
use crate::keydir::KeyDir;

struct Engine <T: keydir::KeyDir, F: FileReader, W: FileWriter> {
    /// The path to the directory where the data and hint files are stored.
    ///
    /// bitcask_path/
    /// ├── 000001.data <-- Immutable data file (closed)
    /// ├── 000002.data <-- Immutable data file (closed)
    /// ├── 000003.data <-- Active data file (append-only)
    /// ├── 000001.hint <-- Hint file (optional)
    /// ├── ...
    data_path: PathBuf,
    /// The maximum size of the data files in bytes. An active file reaching this size
    /// will be closed and a new one will be created. The file size can be a little
    /// bit larger than this value due to the way files are written. During merge,
    /// the storage engine will also ensure that read-only files do not exceed this size that much.
    key_dir: T,
    // Think about how merge is done; periodically, by size or both?
    /// Compress old files while merging. Only the values are compressed, not the keys. This will
    /// have an impact on the performance of data retrieval but will save space. Also, the size
    /// of the value in the metadata will be the size of the compressed value.
    compress_old_files: bool,
    /// FileWriter own the active file. It is used to write new data to the active file.
    /// It is also used to close the active file when it reaches the maximum size or when
    /// the engine is dropped.
    /// The active file is a wrapper around the opened active file. It allows directly writing
    /// to the file without needing to open it every time.
    /// The file is closed when the engine is dropped.
    write_handler: W,
    /// Datafiles is a cache of the opened data files. The key is the file number, and the value is
    /// a FileReader that allows reading from the file. FileReader can be thought as a file
    /// descriptor.
    data_files: BTreeMap<u16, F>,
    /// The lock file
    lock_file_name: String,
}

impl<T, F, W> Engine<T, F, W>
where
    T: KeyDir,
    F: FileReader,
    W: FileWriter,
{

    pub fn start(&self) {
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