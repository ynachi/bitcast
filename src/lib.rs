use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

mod merge;
mod metrics;
mod reader;
mod storage;
mod writer;

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
    /// The writer handler needs to make sure data files are only opened by one process. The lock
    /// file makes sure to enforce that.
    pub writer_lock_file_name: String,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            data_path: PathBuf::from("./bitcask"),
            compress_old_files: false,
            data_file_max_size: 1024 * 1024 * 128, // 128 MB
            key_max_size: 1024,                    // 1 KB
            value_max_size: 1024 * 1024,           // 1 MB
            writer_lock_file_name: "writer.lock".to_string(),
        }
    }
}

pub(crate) fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct FileWithOffset {
    pub file: File,
    pub offset: usize,
}

impl FileWithOffset {
    pub fn new(file: File, offset: usize) -> Self {
        Self { file, offset }
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryEntry {
    pub file_id: usize,
    pub value_size: usize,
    pub value_offset: usize,
    pub timestamp: u64,
}

pub(crate) fn create_active_file(options: &EngineOptions, file_id: usize) -> io::Result<File> {
    let data_file_path = options.data_path.join(format!("{file_id:06}.data"));
    let data_file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&data_file_path)?;
    tracing::debug!("opened initial data file at {:?}", data_file_path);
    Ok(data_file)
}
