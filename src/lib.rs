use std::path::PathBuf;

mod fs_reader;
mod fs_writer;
pub mod keydir;
mod merge;
mod metrics;
mod storage;

#[derive(Debug, Clone)]
pub struct EngineOptions {
    pub data_path: PathBuf,
    /// Compress old files while merging. Only the values are compressed, not the keys. This will
    /// have an impact on the performance of data retrieval but will save space. Also, the size
    /// of the value in the metadata will be the size of the compressed value.
    pub compress_old_files: bool,
    /// The maximum size of the data files in bytes. An active file reaching this size
    /// will be closed and a new one will be created. The file size can be a little
    /// bit larger than this value due to the way files are written. During merge,
    /// the storage engine will also ensure that read-only files do not exceed this size that much.
    pub data_file_max_size: usize,
    pub key_max_size: usize,
    pub value_max_size: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            data_path: PathBuf::from("./bitcask"),
            compress_old_files: false,
            data_file_max_size: 1024 * 1024 * 128, // 128 MB
            key_max_size: 1024,                    // 1 KB
            value_max_size: 1024 * 1024,           // 1 MB
        }
    }
}

pub(crate) fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
