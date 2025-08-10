use crate::reader::FileReader;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::errors::Error::BufToArray;

mod hint;
mod merge;
mod metrics;
mod reader;
mod storage;
mod writer;
mod errors;

// ------------------- General consts ------------------------
const HINT_HEADER_SIZE: usize = 24;
const HINT_TIMESTAMP_RANGE: std::ops::Range<usize> = 0..8;
const HINT_KEY_SIZE_RANGE: std::ops::Range<usize> = 8..12;
const HINT_VALUE_SIZE_RANGE: std::ops::Range<usize> = 12..16;
const HINT_VALUE_POS_RANGE: std::ops::Range<usize> = 16..24;
const HINT_AVERAGE_KEY_SIZE: usize = 256;

const DATA_CRC_SIZE: usize = 4;
// -------------------- General Consts end -------------------

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

pub struct EntryRef<'a> {
    pub key: &'a [u8],
    pub value_size: u32,
    pub value_offset: u64,
    pub timestamp: u64,
    // data_size will be useful to the caller to skip part of the underlined I/O
    // It represents the part of the entry which is of variable lenght
    pub data_size: u64,
    pub file_id: u64,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub file_id: u64,
    // the following are written to some files; it is better to stick to deterministic types size
    // u32, u64 instead of usize.
    pub value_size: u32,
    pub value_offset: u64,
    pub timestamp: u64,
}

impl From<EntryRef<'_>> for Entry {
    fn from(e: EntryRef) -> Self {
        Entry {
            file_id: e.file_id,
            value_size: e.value_size,
            value_offset: e.value_offset,
            timestamp: e.timestamp,
        }
    }
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
    key_dir: RwLock<HashMap<Vec<u8>, Entry>>,
    /// Datafiles is a cache of the opened data files. The key is the file number, and the value is
    /// a FileReader that allows reading from the file. FileReader can be thought as a file
    /// descriptor. No ARC because the engine itself would be wrapped in an ARC.
    data_files: RwLock<BTreeMap<usize, FileReader<true>>>,
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

#[derive(Debug, Clone, Copy)]
// std::opts::Range does not implement copy, so let's use our own
pub(crate) struct ByteRange {
    pub start: usize,
    pub end: usize,
}

impl ByteRange {
    #[inline]
    pub(crate) fn slice<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        &data[self.start..self.end]
    }

    #[inline]
    /// warning, parse error is not checked
    pub(crate) fn get_u64(&self, buf: &[u8]) -> u64 {
        u64::from_le_bytes(self.slice(buf).try_into().unwrap())
    }

    #[inline]
    /// warning, parse error is not checked
    pub(crate) fn get_u32(&self, buf: &[u8]) -> u32 {
        u32::from_le_bytes(self.slice(buf).try_into().unwrap())
    }
}
