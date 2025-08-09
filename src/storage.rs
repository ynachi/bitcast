use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::{io, vec};
use std::io::{BufReader, Read};
use crate::writer::WriteHandler;
use crate::{Entry, current_time_millis, SharedContext, EngineOptions, hint_path_from_id, get_u32, get_u64, crc_matches};
use std::sync::{Arc, RwLock};
use tracing::{debug, error, warn};
use crate::hint::FileHintService;
use crate::reader::FileReader;

// Bitcask hint file format constants
const HINT_HEADER_SIZE: usize = 24;
const HINT_TIMESTAMP_RANGE: std::ops::Range<usize> = 0..8;
const HINT_KEY_SIZE_RANGE: std::ops::Range<usize> = 8..12;
const HINT_VALUE_SIZE_RANGE: std::ops::Range<usize> = 12..16;
const HINT_VALUE_POS_RANGE: std::ops::Range<usize> = 16..24;
const HINT_AVERAGE_KEY_SIZE: usize = 256;

// Bitcask data file format constants
const DATA_HEADER_SIZE: usize = 20;
const DATA_CRC_RANGE: std::ops::Range<usize> = 0..4;
const DATA_TIMESTAMP_RANGE: std::ops::Range<usize> = 4..12;
const DATA_KEY_SIZE_RANGE: std::ops::Range<usize> = 12..16;
const DATA_VALUE_SIZE_RANGE: std::ops::Range<usize> = 16..20;
const DATA_AVERAGE_KEY_SIZE: usize = 256;
const DATA_AVERAGE_VALUE_SIZE: usize = 1024;

pub struct Engine {
    ctx: Arc<SharedContext>,
    /// FileWriter own the active file. It is used to write new data to the active file.
    /// It is also used to close the active file when it reaches the maximum size or when
    /// the engine is dropped.
    /// The active file is a wrapper around the opened active file. It allows directly writing
    /// to the file without needing to open it every time.
    /// The file is closed when the engine is dropped.
    write_handler: WriteHandler,
}

impl Engine {
    /// Creates a new engine. Please note that the active file needs to also be cached in
    /// the data_files map, this is why this method takes the ID and a FD to the active file.
    pub fn new(
        option: EngineOptions,
    ) -> io::Result<Self> {

        let key_dir: HashMap<Vec<u8>, Entry> = HashMap::new();
        let data_files = Self::build_data_files(&option)?;

        // load data_files

        // build key_dir from hint files or data files

        let hint_service = FileHintService::new(ctx.clone());

        let write_handler = WriteHandler::new(ctx.clone(), init_active_file_id, hint_service)?;

        Ok(Self {
            ctx,
            write_handler,
        })
    }


    // Read the bitcast base folder, build file reader cache and return hint files paths.
    // Hint files will be inferred from the data file map so there is no need to build
    // them too
    fn build_data_files(option: &EngineOptions) -> io::Result<(BTreeMap<usize, FileReader>)> {
        if !option.data_path.is_dir() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "data path not found"));
        }

        let mut data_files: BTreeMap<usize, FileReader> = BTreeMap::new();

        for file in option.data_path.read_dir()? {
            let file = file?;
            if let Some(file_name) = file.file_name().to_str() {
                if let Some(file_id) = file_name.strip_suffix(".data") {
                    let file_id = match file_id.parse::<usize>() {
                        Ok(id) => id,
                        Err(err) => {
                            // do not exit as this is not a fatal error
                            error!("failed to extract file ID from filename: {err}");
                            continue;
                        },
                    };
                    data_files.insert(file_id, FileReader::new(&file.path())?);

                } else {
                    debug!("building data files: ignoring file: {file_name}");
                }

            } else {
                error!("failed to read file name, file name is not a valid unicode string");
                continue;
            }

        }

        Ok(data_files)
    }

    // Use data files ids to infer a hint file, open hint files to build the keydir
    // If a hint file does not exist for a given data file, use the data file instead
    // in the build process.
    // hint format | timestamp | key_sz | value_sz | value_pos | key |
    //             ---------------------------------------------------
    //              he1 (hint entry part1, fixed size, 8+8+8+8 | he2 (hint entry part2)
    fn build_key_dir(data_files: &BTreeMap<usize, FileReader>, options: &EngineOptions) -> io::Result<HashMap<Vec<u8>, Entry>> {
        let mut key_dir = HashMap::new();

        for (id, rd)  in data_files.iter().rev() {
            let hint_file_path = hint_path_from_id(*id, options);

            let hint_file = OpenOptions::new()
                .read(true)
                .create(false)
                .open(&hint_file_path)?;

            // loop to decode the whole hint file
            let mut h1_buffer = [0u8; 32];
            let mut buf_reader = BufReader::new(hint_file);
            loop {
                match buf_reader.read_exact(&mut h1_buffer) {
                    Ok(_) => {

                    }
                    // do not return, log and ignore
                    Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {}
                    Err(err) => return Err(err),
                }

            }
        }
        key_dir
    }

    // parse hint file to build key_dir
    // We modified the original bitcast format a bit for more performant parsing
    // at the cost of more storage usage.
    // hint format | timestamp | key_sz | value_sz | value_pos | key |
    //             ---------------------------------------------------
    //              he1 (hint entry part1, fixed size, 8+4+4+8 | he2 (hint entry part2)
    // TODO: use pread instead of bufreader!!!!
    fn parse_hint_file(
        file: File,
        file_id: usize,
        key_dir: &mut HashMap<Vec<u8>, Entry>
    ) -> io::Result<()> {
        let mut reader = BufReader::new(file);
        let mut header = [0u8; HINT_HEADER_SIZE];
        let mut key_buf = Vec::with_capacity(HINT_AVERAGE_KEY_SIZE);

            // the loop will absolutely exit at the end of the file
            loop {
                reader.read_exact(&mut header)?;

                let key_size = get_u32(&header, HINT_KEY_SIZE_RANGE)? as usize;
                let value_size = get_u32(&header, HINT_VALUE_SIZE_RANGE)?;
                let value_offset = get_u64(&header, HINT_VALUE_POS_RANGE)?;
                let timestamp = get_u64(&header, HINT_TIMESTAMP_RANGE)?;
                // this operation looks expensive but can be highly optimized on modern CPUs.
                key_buf.resize(key_size, 0);
                // now read key data
                reader.read_exact(&mut key_buf)?;

                let entry = Entry {
                    file_id,
                    value_size,
                    value_offset,
                    timestamp,
                };

                key_dir.insert(
                    std::mem::take(&mut key_buf),
                    entry
                );
            }
    }

    // parse data file to build key_dir
    fn parse_data_file(
        data_files: &BTreeMap<usize, FileReader>,
        file_id: usize,
        key_dir: &mut HashMap<Vec<u8>, Entry>,
        engine_options: &EngineOptions,
    ) -> io::Result<()> {
        let reader = data_files.get(&file_id).ok_or_else(||
            io::Error::new(io::ErrorKind::NotFound, format!("data file ID {} not found in data files cache", file_id))
        )?;

        let mut header = [0u8; DATA_HEADER_SIZE];
        // we will read the full data to check the CRC. Data == full entry minus the header
        let mut data_buff = Vec::with_capacity(DATA_AVERAGE_VALUE_SIZE + DATA_AVERAGE_KEY_SIZE);
        let mut crc_data = Vec::with_capacity(DATA_AVERAGE_VALUE_SIZE + DATA_AVERAGE_KEY_SIZE + DATA_HEADER_SIZE);

        // the loop will absolutely exit at the end of the file
        let mut offset = 0;
        loop {
            reader.read_exact_at(&mut header, offset)?;

            let key_size = get_u32(&header, DATA_KEY_SIZE_RANGE)?;
            let value_size = get_u32(&header, DATA_VALUE_SIZE_RANGE)?;
            let crc = get_u32(&header, DATA_CRC_RANGE)?;
            let timestamp = get_u64(&header, DATA_TIMESTAMP_RANGE)?;
            let entry_size = DATA_HEADER_SIZE as u64 + (key_size + value_size) as u64;

            if key_size as usize > engine_options.key_max_size ||  value_size as usize > engine_options.value_max_size{
                warn!("skipping entry due to large key or value size, key: {} value: {}", key_size, value_size);
                // skip the whole entry
                offset += entry_size;
                continue;
            }

            // now read the rest of the entry (key data and value data)
            data_buff.resize((value_size + key_size) as usize, 0);
            let data_offset = offset + DATA_HEADER_SIZE as u64;
            reader.read_exact_at(&mut data_buff, data_offset)?;

            // check crc now
            crc_data.clear();
            crc_data.extend_from_slice(&header[4..]);
            crc_data.extend_from_slice(&data_buff);

            let key = data_buff[..key_size as usize].to_vec();

            if crc_matches(crc, &*crc_data) {
                // skip deleted entries, which are represented by 0 value-sized entries
                if value_size != 0 {
                    let entry = Entry {
                        file_id,
                        value_size,
                        value_offset: offset + DATA_HEADER_SIZE as u64 + key_size as u64,
                        timestamp,
                    };

                    key_dir.insert(key, entry);
                } else {
                    key_dir.remove(&key);
                }
            } else {
                // log error on corrupted entry but continue processing
                error!("entry data corrupted")
            }

            offset += entry_size;
        }
    }

    /// If the engine existed before, this method will load necessary data
    pub fn start(&mut self) {
        unimplemented!()
    }

    pub fn stop(&self) {
        unimplemented!()
    }

    // pub fn put(&mut self, key: &[u8], value: &[u8]) {
    //     match self.write_handler.write(key, value) {
    //         Ok(w_result) => {
    //             let entry = InMemoryEntry {
    //                 file_id: w_result.written_file_id,
    //                 value_size: value.len(),
    //                 value_offset: w_result.write_offset,
    //                 timestamp: current_time_millis(),
    //             };
    //             self.key_dir
    //                 .write()
    //                 .expect("Mutex Poisoned")
    //                 .insert(Vec::from(key), entry);
    //
    //             // If there is a new active file, add it to the ro data file cache
    //             if let Some((file, file_id)) = w_result.new_active_file {
    //                 self.data_files
    //                     .write()
    //                     .expect("mutex poisoned")
    //                     .insert(file_id, file.into());
    //             }
    //         }
    //         Err(e) => {
    //             // TODO count write failure rate and act
    //             // because too many failures means storage issue
    //             error!("failed to write to file: {e:?}");
    //         }
    //     }
    // }

    // pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
    //     // entry is inexpensive to clone
    //     let entry = self
    //         .key_dir
    //         .read()
    //         .expect("mutex poisoned")
    //         .get(key)
    //         .cloned()?;
    //     let file_id = entry.file_id;
    //     debug!(
    //         "reading value for {} from file: {file_id}",
    //         String::from_utf8_lossy(key)
    //     );
    //     let guard = self.data_files.read().expect("Mutex poisoned");
    //     let reader = guard
    //         .get(&file_id)
    //         .expect("file not found in keydir but it should be");
    //
    //     match reader.read_at(entry.value_offset as u64, entry.value_size) {
    //         Ok(value) => Some(value),
    //         Err(e) => {
    //             error!("Failed to read value from file: {e:?}");
    //             None
    //         }
    //     }
    // }

    // pub fn delete(&mut self, key: &[u8]) {
    //     match self.write_handler.write(key, &[]) {
    //         Ok(_) => {
    //             self.key_dir.write().expect("Mutex Poisoned").remove(key);
    //         }
    //         Err(e) => {
    //             // TODO count write failure rate and act
    //             error!("failed to write to file: {e:?}");
    //         }
    //     }
    // }

    pub fn merge(&self) {
        unimplemented!()
    }

    // pub fn list_keys(&self) -> Vec<Vec<u8>> {
    //     self.key_dir
    //         .read()
    //         .expect("mutex poisoned")
    //         .keys()
    //         .cloned()
    //         .collect()
    // }

    pub fn sync(&self) {
        unimplemented!()
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_engine_put() {
        unimplemented!()
    }
}
