use crate::errors::Error;
use crate::errors::Error::{InvalidCRC, KeyTooBig, ValueTooBig};
use crate::{
    ByteRange, DATA_CRC_SIZE, EngineOptions, Entry, EntryRef, HINT_HEADER_SIZE,
    HINT_KEY_SIZE_RANGE, HINT_TIMESTAMP_RANGE, HINT_VALUE_SIZE_RANGE, errors::Result,
};
use memmap2::Mmap;
use std::fs::OpenOptions;
use std::path::PathBuf;
use crc_fast::checksum;
use crc_fast::CrcAlgorithm::Crc32IsoHdlc;

/// mmap-based file reader with opt-in CRC verification when needed
//data [CRC:4][key_size:4][value_size:4][timestamp:8][key][value]
//hint [key_size:4][value_size:4][timestamp:8][value_offset][key]
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
            key_size_range: ByteRange {
                start: HINT_KEY_SIZE_RANGE.start,
                end: HINT_KEY_SIZE_RANGE.end,
            },
            value_size_range: ByteRange {
                start: HINT_VALUE_SIZE_RANGE.start,
                end: HINT_VALUE_SIZE_RANGE.end,
            },
            timestamp_range: ByteRange {
                start: HINT_TIMESTAMP_RANGE.start,
                end: HINT_TIMESTAMP_RANGE.end,
            },
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

        let old_crc = self
            .reader_options
            .crc_range
            .get_u32(self.read_at(offset, 4)?);

        let payload_start_idx = offset + crc_size;
        let payload_len = self.reader_options.header_size + data_size - crc_size;
        let crc = checksum(Crc32IsoHdlc, self.read_at(payload_start_idx, payload_len)?) as u32;
        Ok(crc == old_crc)
    }

    pub fn open(
        path: &PathBuf,
        file_id: u64,
        engine_options: EngineOptions,
        reader_options: ReaderOptions,
    ) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(false).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let file_size = mmap.len();

        Ok(FileReader {
            mmap,
            file_id,
            file_size,
            engine_options,
            reader_options,
        })
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
        if VERIFY_CRC && !self.crc_ok(offset, data_size)? {
                return Err(InvalidCRC(data_size));
        }

        if key_size > self.engine_options.key_max_size {
            return Err(KeyTooBig(key_size, self.engine_options.key_max_size));
        }
        if value_size as usize > self.engine_options.value_max_size {
            return Err(ValueTooBig(
                value_size as usize,
                self.engine_options.value_max_size,
            ));
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



#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use tempfile::TempDir;
    use crate::{DATA_CRC_RANGE, DATA_HEADER_SIZE, DATA_KEY_SIZE_RANGE, DATA_TIMESTAMP_RANGE, DATA_VALUE_SIZE_RANGE};

    // Test constants
    const TEST_KEY_MAX_SIZE: usize = 1024;
    const TEST_VALUE_MAX_SIZE: usize = 4096;
    const TEST_FILE_ID: u64 = 123;
    const TEST_TIMESTAMP: u64 = 1692547200000; // milliseconds

    fn create_engine_options() -> EngineOptions {
        EngineOptions {
            key_max_size: TEST_KEY_MAX_SIZE,
            value_max_size: TEST_VALUE_MAX_SIZE,
            ..Default::default()
        }
    }

    fn create_data_reader_options() -> ReaderOptions {
        ReaderOptions {
            header_size: DATA_HEADER_SIZE,
            key_size_range: ByteRange { start: DATA_KEY_SIZE_RANGE.start, end: DATA_KEY_SIZE_RANGE.end},
            value_size_range: ByteRange { start: DATA_VALUE_SIZE_RANGE.start, end: DATA_VALUE_SIZE_RANGE.end },
            timestamp_range: ByteRange { start: DATA_TIMESTAMP_RANGE.start, end: DATA_TIMESTAMP_RANGE.end },
            crc_range: ByteRange { start: DATA_CRC_RANGE.start, end: DATA_CRC_RANGE.end },           // First 4 bytes
        }
    }

    fn create_hint_reader_options() -> ReaderOptions {
        ReaderOptions::default()
    }

    fn calculate_crc(data: &[u8]) -> u32 {
        checksum(Crc32IsoHdlc, data) as u32
    }

    fn create_test_file(data: &[u8]) -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_file");
        let mut file = File::create(&file_path).expect("Failed to create test file");
        file.write_all(data).expect("Failed to write test data");
        (temp_dir, file_path)
    }

    fn create_valid_data_entry(key: &[u8], value: &[u8], timestamp: u64) -> Vec<u8> {
        let mut entry = Vec::new();

        // Reserve space for CRC (will be filled later)
        entry.extend_from_slice(&[0u8; 4]);
        entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
        entry.extend_from_slice(&(value.len() as u32).to_le_bytes());
        entry.extend_from_slice(&timestamp.to_le_bytes());
        entry.extend_from_slice(key);
        entry.extend_from_slice(value);

        let payload = &entry[4..];
        let crc = calculate_crc(payload);
        entry[0..4].copy_from_slice(&crc.to_le_bytes());

        entry
    }

    fn create_invalid_crc_data_entry(key: &[u8], value: &[u8], timestamp: u64) -> Vec<u8> {
        let mut entry = create_valid_data_entry(key, value, timestamp);
        // Corrupt the CRC
        entry[0] = entry[0].wrapping_add(1);
        entry
    }

    fn create_valid_hint_entry(key: &[u8], value_size: u32, timestamp: u64, value_offset: u64) -> Vec<u8> {
        let mut entry = Vec::new();
        entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
        entry.extend_from_slice(&value_size.to_le_bytes());
        entry.extend_from_slice(&timestamp.to_le_bytes());
        entry.extend_from_slice(&value_offset.to_le_bytes());
        entry.extend_from_slice(key);

        entry
    }

    mod reader_data_file_tests {
        use super::*;

        mod happy_path_tests {
            use super::*;
            #[test]
            fn test_parse_single_valid_entry() {
                let key = b"test_key";
                let value = b"test_value";
                let entry_data = create_valid_data_entry(key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let entry_ref = reader.parse_entry_ref_at(0).unwrap();

                assert_eq!(entry_ref.key, key);
                assert_eq!(entry_ref.value_size, value.len() as u32);
                assert_eq!(entry_ref.timestamp, TEST_TIMESTAMP);
                assert_eq!(entry_ref.file_id, TEST_FILE_ID);
                assert_eq!(entry_ref.data_size, (key.len() + value.len()) as u64);
            }

            #[test]
            fn test_parse_entry_converts_to_owned() {
                let key = b"convert_test";
                let value = b"owned_data";
                let entry_data = create_valid_data_entry(key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let entry = reader.parse_entry_at(0).unwrap();
                // Entry should contain owned data converted from EntryRef
                assert_eq!(entry.timestamp, TEST_TIMESTAMP);
                assert_eq!(entry.file_id, TEST_FILE_ID);
            }

            #[test]
            fn test_multiple_entries() {
                let entries = vec![
                    (b"key1".as_slice(), b"value1".as_slice()),
                    (b"key2".as_slice(), b"value2".as_slice()),
                    (b"key3".as_slice(), b"value3".as_slice()),
                ];

                let mut file_data = Vec::new();
                let mut offsets = Vec::new();

                for (key, value) in &entries {
                    offsets.push(file_data.len());
                    let entry_data = create_valid_data_entry(key, value, TEST_TIMESTAMP);
                    file_data.extend_from_slice(&entry_data);
                }

                let (_temp_dir, file_path) = create_test_file(&file_data);
                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                for (i, (expected_key, _)) in entries.iter().enumerate() {
                    let entry_ref = reader.parse_entry_ref_at(offsets[i]).unwrap();
                    assert_eq!(entry_ref.key, *expected_key);
                }
            }
        }

        mod crc_verification_tests {
            use super::*;

            #[test]
            fn test_valid_crc_passes() {
                let key = b"crc_test";
                let value = b"valid_crc_data";
                let entry_data = create_valid_data_entry(key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                // Should succeed with valid CRC
                let result = reader.parse_entry_ref_at(0);
                assert!(result.is_ok());
            }

            #[test]
            fn test_invalid_crc_fails() {
                let key = b"bad_crc";
                let value = b"corrupted_data";
                let entry_data = create_invalid_crc_data_entry(key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let result = reader.parse_entry_ref_at(0);
                assert!(matches!(result, Err(InvalidCRC(_))));
            }

            #[test]
            fn test_crc_verification_disabled() {
                let key = b"no_verify";
                let value = b"skip_crc_check";
                let entry_data = create_invalid_crc_data_entry(key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<false>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                // Should succeed even with invalid CRC when verification is disabled
                let result = reader.parse_entry_ref_at(0);
                assert!(result.is_ok());
            }
        }

        mod size_validation_tests {
            use super::*;
            #[test]
            fn test_key_too_big() {
                let big_key = vec![b'x'; TEST_KEY_MAX_SIZE + 1];
                let value = b"normal_value";
                let entry_data = create_valid_data_entry(&big_key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let result = reader.parse_entry_ref_at(0);
                assert!(matches!(result, Err(KeyTooBig(actual, max)) if actual == TEST_KEY_MAX_SIZE + 1 && max == TEST_KEY_MAX_SIZE));
            }

            #[test]
            fn test_value_too_big() {
                let key = b"normal_key";
                let big_value = vec![b'y'; TEST_VALUE_MAX_SIZE + 1];
                let entry_data = create_valid_data_entry(key, &big_value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let result = reader.parse_entry_ref_at(0);
                assert!(matches!(result, Err(ValueTooBig(actual, max)) if actual == TEST_VALUE_MAX_SIZE + 1 && max == TEST_VALUE_MAX_SIZE));
            }

            #[test]
            fn test_max_size_boundaries() {
                // Test exactly at the limit - should succeed
                let max_key = vec![b'k'; TEST_KEY_MAX_SIZE];
                let max_value = vec![b'v'; TEST_VALUE_MAX_SIZE];
                let entry_data = create_valid_data_entry(&max_key, &max_value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let result = reader.parse_entry_ref_at(0);
                assert!(result.is_ok());
            }
        }

        mod read_overflow_tests {
            use super::*;
            #[test]
            fn test_read_at_overflow() {
                let key = b"small";
                let value = b"data";
                let entry_data = create_valid_data_entry(key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                // Try to read beyond the file size
                let result = reader.read_at(0, entry_data.len() + 1);
                assert!(matches!(result, Err(Error::MmapReadOverflow(_, _, _))));
            }

            #[test]
            fn test_parse_entry_overflow_header() {
                let small_data = vec![0u8; 10]; // Smaller than header size
                let (_temp_dir, file_path) = create_test_file(&small_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let result = reader.parse_entry_ref_at(0);
                assert!(matches!(result, Err(Error::MmapReadOverflow(_, _, _))));
            }

            #[test]
            fn test_parse_entry_overflow_key() {
                // Create entry with key size that would overflow
                let mut partial_entry = Vec::new();
                partial_entry.extend_from_slice(&[0u8; 4]); // CRC
                partial_entry.extend_from_slice(&1000u32.to_le_bytes()); // Large key size
                partial_entry.extend_from_slice(&10u32.to_le_bytes()); // Value size
                partial_entry.extend_from_slice(&TEST_TIMESTAMP.to_le_bytes()); // Timestamp
                partial_entry.extend_from_slice(&[0u8; 50]); // Insufficient data for key

                let (_temp_dir, file_path) = create_test_file(&partial_entry);
                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                let result = reader.parse_entry_ref_at(0);
                assert!(matches!(result, Err(Error::MmapReadOverflow(_, _, _))));
            }

            #[test]
            fn test_read_at_exact_boundary() {
                let key = b"boundary";
                let value = b"test";
                let entry_data = create_valid_data_entry(key, value, TEST_TIMESTAMP);
                let (_temp_dir, file_path) = create_test_file(&entry_data);

                let reader = FileReader::<true>::open(
                    &file_path,
                    TEST_FILE_ID,
                    create_engine_options(),
                    create_data_reader_options(),
                ).unwrap();

                // Reading exactly the file size should work
                let result = reader.read_at(0, entry_data.len());
                assert!(result.is_ok());
                assert_eq!(result.unwrap().len(), entry_data.len());
            }
        }
    }

    mod hint_file_tests {
        use super::*;
        #[test]
        fn test_parse_hint_entry() {
            let key = b"hint_key";
            let value_size = 256u32;
            let value_offset = 1024u64;
            let entry_data = create_valid_hint_entry(key, value_size, TEST_TIMESTAMP, value_offset);
            let (_temp_dir, file_path) = create_test_file(&entry_data);

            let reader = FileReader::<false>::open(
                &file_path,
                TEST_FILE_ID,
                create_engine_options(),
                create_hint_reader_options(),
            ).unwrap();

            let entry_ref = reader.parse_entry_ref_at(0).unwrap();

            assert_eq!(entry_ref.key, key);
            assert_eq!(entry_ref.value_size, value_size);
            assert_eq!(entry_ref.timestamp, TEST_TIMESTAMP);
            assert_eq!(entry_ref.file_id, TEST_FILE_ID);
        }

        #[test]
        fn test_multiple_hint_entries() {
            let entries = vec![
                (b"hint1".as_slice(), 100u32, 2000u64),
                (b"hint2".as_slice(), 200u32, 4000u64),
                (b"hint3".as_slice(), 300u32, 8000u64),
            ];

            let mut file_data = Vec::new();
            let mut offsets = Vec::new();

            for (key, value_size, value_offset) in &entries {
                offsets.push(file_data.len());
                let entry_data = create_valid_hint_entry(key, *value_size, TEST_TIMESTAMP, *value_offset);
                file_data.extend_from_slice(&entry_data);
            }

            let (_temp_dir, file_path) = create_test_file(&file_data);
            let reader = FileReader::<false>::open(
                &file_path,
                TEST_FILE_ID,
                create_engine_options(),
                create_hint_reader_options(),
            ).unwrap();

            for (i, (expected_key, expected_value_size, _)) in entries.iter().enumerate() {
                let entry_ref = reader.parse_entry_ref_at(offsets[i]).unwrap();
                assert_eq!(entry_ref.key, *expected_key);
                assert_eq!(entry_ref.value_size, *expected_value_size);
            }
        }

        #[test]
        fn test_hint_key_too_big() {
            let big_key = vec![b'h'; TEST_KEY_MAX_SIZE + 1];
            let entry_data = create_valid_hint_entry(&big_key, 100, TEST_TIMESTAMP, 1000);
            let (_temp_dir, file_path) = create_test_file(&entry_data);

            let reader = FileReader::<false>::open(
                &file_path,
                TEST_FILE_ID,
                create_engine_options(),
                create_hint_reader_options(),
            ).unwrap();

            let result = reader.parse_entry_ref_at(0);
            assert!(matches!(result, Err(KeyTooBig(_, _))));
        }

        #[test]
        fn test_hint_value_too_big() {
            let key = b"hint_key";
            let big_value_size = (TEST_VALUE_MAX_SIZE + 1) as u32;
            let entry_data = create_valid_hint_entry(key, big_value_size, TEST_TIMESTAMP, 1000);
            let (_temp_dir, file_path) = create_test_file(&entry_data);

            let reader = FileReader::<false>::open(
                &file_path,
                TEST_FILE_ID,
                create_engine_options(),
                create_hint_reader_options(),
            ).unwrap();

            let result = reader.parse_entry_ref_at(0);
            assert!(matches!(result, Err(ValueTooBig(_, _))));
        }
    }

    mod edge_cases {
        use super::*;

        #[test]
        fn test_empty_key_and_value() {
            let key = b"";
            let value = b"";
            let entry_data = create_valid_data_entry(key, value, TEST_TIMESTAMP);
            let (_temp_dir, file_path) = create_test_file(&entry_data);

            let reader = FileReader::<true>::open(
                &file_path,
                TEST_FILE_ID,
                create_engine_options(),
                create_data_reader_options(),
            ).unwrap();

            let result = reader.parse_entry_ref_at(0);
            assert!(result.is_ok());

            let entry_ref = result.unwrap();
            assert_eq!(entry_ref.key.len(), 0);
            assert_eq!(entry_ref.value_size, 0);
        }

        #[test]
        fn test_zero_timestamp() {
            let key = b"zero_time";
            let value = b"data";
            let entry_data = create_valid_data_entry(key, value, 0);
            let (_temp_dir, file_path) = create_test_file(&entry_data);

            let reader = FileReader::<true>::open(
                &file_path,
                TEST_FILE_ID,
                create_engine_options(),
                create_data_reader_options(),
            ).unwrap();

            let entry_ref = reader.parse_entry_ref_at(0).unwrap();
            assert_eq!(entry_ref.timestamp, 0);
        }

        #[test]
        fn test_max_timestamp() {
            let key = b"max_time";
            let value = b"data";
            let max_timestamp = u64::MAX;
            let entry_data = create_valid_data_entry(key, value, max_timestamp);
            let (_temp_dir, file_path) = create_test_file(&entry_data);

            let reader = FileReader::<true>::open(
                &file_path,
                TEST_FILE_ID,
                create_engine_options(),
                create_data_reader_options(),
            ).unwrap();

            let entry_ref = reader.parse_entry_ref_at(0).unwrap();
            assert_eq!(entry_ref.timestamp, max_timestamp);
        }
    }
}
