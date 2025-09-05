pub use crate::reader::FileReader;
use crc_fast::CrcAlgorithm::Crc32IsoHdlc;
use crc_fast::checksum;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

mod context;
mod errors;
mod hint;
mod merge;
mod metrics;
mod reader;
mod storage;
mod writer;

pub use crate::context::FileIdAllocator;
pub use crate::context::SharedContext;
pub use crate::reader::ReaderOptions;

// ------------------- General consts ------------------------
//data [CRC:4][key_size:4][value_size:4][timestamp:8][key][value]
//hint [key_size:4][value_size:4][timestamp:8][value_offset][key]
pub const HINT_HEADER_SIZE: usize = 24;
pub const HINT_KEY_SIZE_RANGE: std::ops::Range<usize> = 0..4;
const HINT_VALUE_SIZE_RANGE: std::ops::Range<usize> = 4..8;
const HINT_TIMESTAMP_RANGE: std::ops::Range<usize> = 8..16;
const HINT_VALUE_POS_RANGE: std::ops::Range<usize> = 16..24;
const HINT_AVERAGE_KEY_SIZE: usize = 256;

pub const DATA_CRC_SIZE: usize = 4;

const DATA_HEADER_SIZE: usize = 20;
const DATA_CRC_RANGE: std::ops::Range<usize> = 0..4;
const DATA_KEY_SIZE_RANGE: std::ops::Range<usize> = 4..8;
const DATA_VALUE_SIZE_RANGE: std::ops::Range<usize> = 8..12;
const DATA_TIMESTAMP_RANGE: std::ops::Range<usize> = 12..20;
// -------------------- General Consts end -------------------

#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// The permission mode for creating directories.
    pub dir_mode: u32,
    /// The file mode to set for the data and hint files.
    pub file_mode: u32,
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
            dir_mode: 0o750,
            file_mode: 0o644,
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
    pub file_id: u64,
}

impl FileWithOffset {
    pub fn new(file: File, offset: usize, file_id: u64) -> Self {
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

pub(crate) fn create_active_file(options: &EngineOptions, file_id: u64) -> io::Result<File> {
    let data_file_path = options.data_path.join(format!("{file_id:06}.data"));
    let data_file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&data_file_path)?;
    tracing::debug!("opened initial data file at {:?}", data_file_path);
    Ok(data_file)
}

pub fn hint_path_from_id(id: u64, options: &EngineOptions) -> PathBuf {
    options.data_path.join(format!("{id:06}.hint"))
}

#[derive(Debug, Clone, Copy)]
// std::opts::Range does not implement copy, so let's use our own
pub struct ByteRange {
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

fn create_data_entry_with_crc(key: &[u8], value: &[u8], timestamp: u64, buf: &mut Vec<u8>) {
    create_data_entry(key, value, timestamp, buf);

    let payload = &buf[4..];
    let crc = calculate_crc(payload);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
}

fn create_data_entry(key: &[u8], value: &[u8], timestamp: u64, buf: &mut Vec<u8>) {
    buf.clear();
    // Reserve space for CRC (will be filled later)
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(&timestamp.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
}

fn calculate_crc(data: &[u8]) -> u32 {
    checksum(Crc32IsoHdlc, data) as u32
}
