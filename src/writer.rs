use std::fs::{File, OpenOptions};
use crate::hint::{FileHintService, HintMessage};
use crate::{FileReader, FileWithOffset, SharedContext, create_active_file, create_data_entry, create_data_entry_with_crc, current_time_millis, DATA_HEADER_SIZE};
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::{debug, error};

/// FileWriter is the service used to append the data to the active file.
/// Users have to decide upfront whether to use CRC or not. The performance
/// penalty of CRC is huge, our benchmarks revealed around 30% throughput drop.
pub struct FileWriter<const USE_CRC: bool> {
    ctx: Arc<SharedContext<USE_CRC>>,
    // we have to maintain the offset manually because
    // entries need that information
    pub active_file: Arc<Mutex<FileWithOffset>>,
    lock_file: File,
    hint_service: FileHintService,
}

pub(crate) struct FileWriteResult {
    /// offset of the written data
    pub(crate) write_offset: usize,
    /// File to which the data was written to
    pub(crate) written_file_id: u64,
}

impl<const USE_CRC: bool> FileWriter<USE_CRC> {
    // returns (entry offset, file_id), Maybe (New file FD, new file ID)
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<FileWriteResult> {
        let key_size = key.len();
        if key_size > self.ctx.engine_options.key_max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Key size exceeds maximum allowed size",
            ));
        }
        let data_size = value.len();
        if data_size > self.ctx.engine_options.value_max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Value size exceeds maximum allowed size",
            ));
        }

        let timestamp = current_time_millis();

        //data [CRC:4][key_size:4][value_size:4][timestamp:8][key][value] == header + key + value
        let entry_size = DATA_HEADER_SIZE + key_size + data_size;
        let mut entry_buffer = Vec::with_capacity(entry_size);

        if USE_CRC {
            create_data_entry_with_crc(key, value, timestamp, &mut entry_buffer);
        } else {
            create_data_entry(key, value, timestamp, &mut entry_buffer);
        }

        let mut guard = self.active_file.lock().expect("mutex lock poisoned");
        // the current offset will be the offset of the entry we are inserting, save before
        // mutation
        let current_offset = guard.offset;
        let written_file_id = guard.file_id;

        guard.file.write_all(&entry_buffer)?;
        guard.file.flush()?;
        guard.offset += entry_size;

        // should we rotate?
        self.maybe_rotate(guard)?;

        Ok(FileWriteResult {
            write_offset: current_offset,
            written_file_id,
        })
    }

    fn maybe_rotate(&self, mut guard: MutexGuard<FileWithOffset>) -> io::Result<()> {
        if guard.offset >= self.ctx.engine_options.data_file_max_size {
            let old_file_id = guard.file_id;

            let reader = FileReader::<USE_CRC>::from_opened_file(
                guard.file.try_clone()?,
                guard.file_id,
                &self.ctx.engine_options,
                &self.ctx.rd_options,
            ).expect("Failed to create reader during file rotation - data store may be corrupted");

            self.ctx
                .data_files
                .write()
                .expect("failed to get mutex on data files")
                .insert(guard.file_id, reader);

            let next_file_id = self.ctx.file_id_allocator.next();
            guard.file = create_active_file(&self.ctx.engine_options, next_file_id)?;
            guard.file_id = next_file_id;
            guard.offset = 0;

            self
                .hint_service
                .sender
                .send(HintMessage::Hint(old_file_id))
                .expect("Failed to send file hint creation notification - data store may be corrupted");
        }
        Ok(())
    }

    /// Creates a new mutex file writer, returning the writer along with the FD
    /// of the underlined file. We need to return that FD because the caller would
    /// typically use it to form a file reader.
    ///
    pub fn new(
        ctx: Arc<SharedContext<USE_CRC>>,
        initial_active_file_id: u64,
        file_hint_service: FileHintService,
    ) -> io::Result<Self> {

                let lock_file_path = &ctx.engine_options
                    .data_path
                    .join(&ctx.engine_options.writer_lock_file_name);


                let lock_file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    // We force to create new to actually simulate a lock file.
                    // TODO: Check how to prevent stale lock.
                    .create_new(true)
                    .open(lock_file_path)?;
                debug!(
                    "lock successfully acquired, created lock file at {:?}",
                    lock_file_path
                );

        let data_file = create_active_file(&ctx.engine_options, initial_active_file_id)?;

        Ok(FileWriter {
            ctx,
            active_file: Arc::new(Mutex::new(FileWithOffset::new(
                data_file,
                0,
                initial_active_file_id,
            ))),
            lock_file,
            hint_service: file_hint_service,
        })
    }

    /// write in batch to implement, for performance
    pub fn write_batch(
        &mut self,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> io::Result<Vec<FileWriteResult>> {
        unimplemented!()
    }
}

impl<const USE_CRC: bool> Drop for FileWriter<USE_CRC> {
    fn drop(&mut self) {
        // Ask the hint service thread to stop
        if let Err(e) = self.hint_service.sender.send(HintMessage::Stop) {
            error!(
                error = %e,
                "Failed to send stop message to hint service"
            );
        }

        match self.active_file.lock() {
            Ok(guard) => {
                guard
                    .file
                    .sync_all()
                    .unwrap_or_else(|e| error!("failed to sync active file: {}", e));
            }
            Err(e) => error!("failed to lock active file mutex: {}", e),
        };

        debug!("MutexFileWriter dropped, lock file and active file closed.");
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use super::*;
    use crate::{current_time_millis, EngineOptions, ReaderOptions, DATA_KEY_SIZE_RANGE, DATA_TIMESTAMP_RANGE, DATA_VALUE_SIZE_RANGE};
    use std::io::Read;
    use tempfile::TempDir;

    //data [CRC:4][key_size:4][value_size:4][timestamp:8][key][value]
    //hint [key_size:4][value_size:4][timestamp:8][value_offset][key]

    #[test]
    fn test_writer() {
        let dir = TempDir::new().unwrap();
        let engine_options = EngineOptions {
            dir_mode: 0o750,
            file_mode: 0o644,
            data_path: dir.path().to_path_buf(),
            compress_old_files: false,
            data_file_max_size: 80,
            key_max_size: 12,
            value_max_size: 24,
            writer_lock_file_name: "writer.lock".to_string(),
        };

        let rd_options = ReaderOptions::default();

        let ctx:Arc<SharedContext<true>>  = Arc::new(SharedContext::new(engine_options.clone(), rd_options).unwrap());

        let file_hint_service = FileHintService::new(ctx.clone());

        let mut writer = FileWriter::new(ctx.clone(), 0, file_hint_service).unwrap();

        // check if the written data matches what is expected
        let result = writer.write(b"key", b"value").unwrap();
        let mut file = File::open(engine_options.data_path.join("000000.data")).unwrap();
        let mut data = Vec::with_capacity(28);
        let line = file.read_to_end(&mut data).unwrap();
        assert_eq!(result.write_offset, 0);
        assert_eq!(line, 28);
        assert_eq!(result.written_file_id, 0);
        println!("{:?}", &data);

        // check key size is well written
        assert_eq!(u32::from_le_bytes(data[DATA_KEY_SIZE_RANGE].try_into().unwrap()), 3);
        // check data size is well written
        assert_eq!(u32::from_le_bytes(data[DATA_VALUE_SIZE_RANGE].try_into().unwrap()), 5);
        // check that a key is well written
        assert_eq!(String::from_utf8_lossy(&data[20..23]), "key");
        // check that data is well written
        assert_eq!(String::from_utf8_lossy(&data[23..28]), "value");
        // check that the timestamp in the file is in the past
        let file_timestamp = u64::from_le_bytes(data[DATA_TIMESTAMP_RANGE].try_into().unwrap());
        // put a sleep to make sure some time has passed since the put operation
        std::thread::sleep(std::time::Duration::from_millis(100));
        let now_timestamp = current_time_millis();
        assert!(file_timestamp < now_timestamp);

        // Assert we cannot have two writers
        let file_hint_service = FileHintService::new(ctx.clone());
        let writer = FileWriter::new(ctx.clone(), 0, file_hint_service);
        assert!(writer.is_err());

        // // let's write again and check that cursor move and no rotation
        // let result = writer.write(b"key", b"value").unwrap();
        // assert_eq!(result.write_offset, 36);
        // assert!(result.new_active_file.is_none());
        // assert_eq!(result.written_file_id, 0);
        //
        // // check that rotation happens when we write more than data_file_max_size == 80
        // // actually written == 36 * 3. Rotation happens here, but the entry was written to the
        // // previous file so the offset and file id of the written data should be from the previous
        // // file.
        // let result = writer.write(b"key", b"value").unwrap();
        // assert_eq!(result.write_offset, 72);
        // assert!(result.new_active_file.is_some());
        // assert_eq!(result.written_file_id, 0);
        // // check that the active file has changed
        // assert_eq!(writer.file_id(), 1);
        //
        // // check that we can write to the new active file
        // // check if the written data matches what is expected
        // let result = writer.write(b"key", b"value").unwrap();
        // let mut file = File::open(engine_options.data_path.join("000001.data")).unwrap();
        // let mut data = Vec::with_capacity(36);
        // let line = file.read_to_end(&mut data).unwrap();
        // // as this is the first read in the new file, the offset of the writen data should come
        // // back to 0.
        // assert_eq!(result.write_offset, 0);
        // assert_eq!(line, 36);
        // assert_eq!(result.written_file_id, 1);
    }

    // #[test]
    // fn test_mutex_writer_existing_data_file() {
    //     let dir = TempDir::new().unwrap();
    //     let engine_options = EngineOptions {
    //         data_path: dir.path().to_path_buf(),
    //         compress_old_files: false,
    //         data_file_max_size: 10,
    //         key_max_size: 12,
    //         value_max_size: 24,
    //         writer_lock_file_name: "writer.lock".to_string(),
    //     };
    //
    //     // manually create file 0 in a dir
    //     let mut file = File::create(engine_options.data_path.join("000000.data")).unwrap();
    //     file.write_all(b"test").unwrap();
    //     file.sync_all().unwrap();
    //
    //     // Now let's try to create a writer. It should not fail
    //     let response = WriteHandler::new(engine_options.clone(), 0);
    //     assert!(response.is_ok());
    // }
}
