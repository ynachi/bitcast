use crate::reader::FileReader;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use crc32fast::Hasher;

mod hint;
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
    pub file_id: usize,
}

impl FileWithOffset {
    pub fn new(file: File, offset: usize, file_id: usize) -> Self {
        Self {
            file,
            offset,
            file_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryEntry {
    pub file_id: usize,
    // the following are written to some files; it is better to stick to deterministic types size
    // u32, u64 instead of usize.
    pub value_size: u32,
    pub value_offset: u64,
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

pub fn hint_path_from_id(id: usize, options: &EngineOptions) -> PathBuf {
    options.data_path.join(format!("{id:06}.hint"))
}

pub struct SharedContext {
    /// Various configuration options for the engine.
    options: EngineOptions,
    /// keydir, as defined in bitsect paper
    key_dir: RwLock<HashMap<Vec<u8>, InMemoryEntry>>,
    /// Datafiles is a cache of the opened data files. The key is the file number, and the value is
    /// a FileReader that allows reading from the file. FileReader can be thought as a file
    /// descriptor. No ARC because the engine itself would be wrapped in an ARC.
    data_files: RwLock<BTreeMap<usize, FileReader>>,
    file_id_allocator: FileIdAllocator,
}

impl SharedContext {
    pub fn new(options: EngineOptions, initial_file_id: usize) -> Self {
        Self {
            options,
            key_dir: RwLock::new(HashMap::new()),
            data_files: RwLock::new(BTreeMap::new()),
            file_id_allocator: FileIdAllocator::new(initial_file_id),
        }
    }
}

pub struct FileIdAllocator {
    counter: AtomicUsize,
}
impl FileIdAllocator {
    pub fn new(start: usize) -> Self {
        Self {
            counter: AtomicUsize::new(start),
        }
    }

    pub fn next(&self) -> usize {
        self.counter.fetch_add(1, Ordering::Release)
    }

    pub fn current(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }
}

#[inline]
pub(crate) fn get_u64(buf: &[u8], range: std::ops::Range<usize>) -> io::Result<u64> {
    Ok(u64::from_le_bytes(
        buf[range]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid key size"))?,
    ))
}

#[inline]
pub(crate) fn get_u32(buf: &[u8], range: std::ops::Range<usize>) -> io::Result<u32> {
    Ok(u32::from_le_bytes(
        buf[range]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid key size"))?,
    ))
}

pub(crate) fn crc_matches(prev_crc: u32, data: &[u8]) -> bool {
    let mut hasher = Hasher::new();
    hasher.update(data);
    let crc = hasher.finalize();
    crc == prev_crc
}