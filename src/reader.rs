use crate::errors::Error;
use crate::errors::Error::{InvalidCRC, KeyTooBig, ValueTooBig};
use crate::{errors::Result, ByteRange, EngineOptions, Entry, EntryRef, DATA_CRC_SIZE, HINT_HEADER_SIZE, HINT_KEY_SIZE_RANGE, HINT_TIMESTAMP_RANGE, HINT_VALUE_SIZE_RANGE};
use crc32fast::Hasher;
use memmap2::Mmap;
use std::fs::OpenOptions;
use std::path::PathBuf;

/// mmap-based file reader with opt-in CRC verification when needed
pub struct FileReader<const VERIFY_CRC: bool> {
    mmap: Mmap,
    file_id: u64,
    file_size: usize,
    engine_options: EngineOptions,
    reader_options: ReaderOptions,
}

/// The reader will be used for both hint and data files.
/// Because those have different offsets for their components,
/// This struct helps specify those to the reader
pub struct ReaderOptions {
    pub header_size: usize,
    pub key_size_range: ByteRange,
    pub value_size_range: ByteRange,
    pub timestamp_range: ByteRange,
    pub crc_range: ByteRange,
}

impl Default for ReaderOptions {
    // the default is for Hint file parsing
    fn default() -> Self {
        Self {
            header_size: HINT_HEADER_SIZE,
            key_size_range: ByteRange { start: HINT_KEY_SIZE_RANGE.start, end: HINT_KEY_SIZE_RANGE.end },
            value_size_range: ByteRange { start: HINT_VALUE_SIZE_RANGE.start, end: HINT_VALUE_SIZE_RANGE.end },
            timestamp_range: ByteRange { start: HINT_TIMESTAMP_RANGE.start, end: HINT_TIMESTAMP_RANGE.end },
            crc_range: ByteRange { start: 0, end: 0 },
        }

    }
}

impl<const VERIFY_CRC: bool> FileReader<VERIFY_CRC> {
    //data [CRC:4][key_size:4][value_size:4][timestamp:8][key][value]
    //hint [key_size:4][value_size:4][timestamp:8][value_offset][key]
    fn crc_ok(&self, offset: usize, data_size: usize) -> Result<bool> {
        let crc_size = self.reader_options.crc_range.end - self.reader_options.crc_range.start;
        // this method should only be called with valid crc, which is 4 bytes
        assert_eq!(crc_size, DATA_CRC_SIZE);

        let old_crc = self.reader_options.crc_range.get_u32(self.read_at(offset, 4)?);

        let payload_start_idx = offset + crc_size;
        let payload_len = self.reader_options.header_size + data_size - crc_size;
        let mut hasher = Hasher::new();
        hasher.update(self.read_at(payload_start_idx, payload_len)?);
        let crc = hasher.finalize();
        Ok(crc == old_crc)
    }

    pub fn open(path: &PathBuf, file_id: u64, engine_options: EngineOptions, reader_options: ReaderOptions) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(false).open(path)?;
        let mmap = unsafe {Mmap::map(&file)?};
        let file_size = mmap.len();

        Ok(
            FileReader {
                mmap,
                file_id,
                file_size,
                engine_options,
                reader_options,
            }
        )
    }

    pub fn read_at(&self, offset: usize, len: usize) -> Result<&[u8]> {
        if offset + len > self.file_size {
            return Err(Error::MmapReadOverflow(offset, len, self.file_size));
        }
        Ok(&self.mmap[offset..offset + len])
    }

    pub fn parse_entry_ref_at(&self, offset: usize) -> Result<EntryRef> {
        // It is ok to unwrap in all this function because not being able to parse
        // is a programming error

        let header = self.read_at(offset, self.reader_options.header_size)?;
        let key_size = self.reader_options.key_size_range.get_u32(header) as usize;
        let value_size = self.reader_options.value_size_range.get_u32(header);
        let data_size = key_size + value_size as usize;

        // comptime
        if VERIFY_CRC {
            if !self.crc_ok(offset, data_size)? {
                return Err(InvalidCRC(data_size));
            }
        }

        if key_size > self.engine_options.key_max_size {
            return Err(KeyTooBig(key_size, self.engine_options.key_max_size));
        }
        if value_size as usize > self.engine_options.value_max_size {
            return Err(ValueTooBig(value_size as usize, self.engine_options.value_max_size));
        }

        let key = self.read_at(offset + self.reader_options.header_size, key_size)?;
        let value_offset = (offset + self.reader_options.header_size + key_size) as u64;

        Ok(EntryRef {
            key,
            value_size,
            value_offset,
            timestamp: self.reader_options.timestamp_range.get_u64(header),
            data_size: data_size as u64,
            file_id: self.file_id,
        })
    }

    pub fn parse_entry_at(&self, offset: usize) -> Result<Entry> {
        let entry_ref = self.parse_entry_ref_at(offset)?;
        Ok(entry_ref.into())
    }
}

