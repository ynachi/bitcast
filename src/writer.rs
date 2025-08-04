use crate::{create_active_file, current_time_millis, EngineOptions, FileWithOffset};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions, remove_file};
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, error};

pub struct WriteHandler {
    // we have to maintain the offset manually because
    // entries need that information
    pub active_file: Arc<Mutex<FileWithOffset>>,
    lock_file: File,
    engine_options: EngineOptions,
    pub active_file_id: AtomicUsize,
}

fn build_entry_buffer(key: &[u8], value: &[u8], timestamp: u64, buffer: &mut Vec<u8>) {
    let key_size = key.len();
    let data_size = value.len();

    // Placeholder for CRC
    buffer.extend_from_slice(&[0u8; 4]);
    buffer.extend_from_slice(&timestamp.to_le_bytes());
    buffer.extend_from_slice(&(key_size as u64).to_le_bytes());
    buffer.extend_from_slice(&(data_size as u64).to_le_bytes());
    buffer.extend_from_slice(key);
    buffer.extend_from_slice(value);

    // Compute CRC over everything except the first 4 bytes
    let mut hasher = Hasher::new();
    hasher.update(&buffer[4..]);
    let crc = hasher.finalize();

    // Write CRC into the first 4 bytes
    buffer[0..4].copy_from_slice(&crc.to_le_bytes());
}

pub(crate) struct FileWriteResult {
    /// offset of the written data
    pub(crate) write_offset: usize,
    /// File to which the data was written to
    pub(crate) written_file_id: usize,
    /// If rotation happened, FD and ID of the new file
    pub(crate) new_active_file: Option<(File, usize)>,
}

impl WriteHandler  {
    // returns (entry offset, file_id), Maybe (New file FD, new file ID)
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<FileWriteResult> {
        let key_size = key.len();
        if key_size > self.engine_options.key_max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Key size exceeds maximum allowed size",
            ));
        }
        let data_size = value.len();
        if data_size > self.engine_options.value_max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Value size exceeds maximum allowed size",
            ));
        }

        let timestamp = current_time_millis();

        // 4 for CRC + 8 for key_size, + sizeof key, + 8 + sizeof data, + 8 for timestamp + 32 bits for checksum
        let entry_size = 4 + 8 + key_size + 8 + data_size + 8;
        let mut entry_buffer = Vec::with_capacity(entry_size);

        build_entry_buffer(key, value, timestamp, &mut entry_buffer);

        let mut guard = self.active_file.lock().expect("mutex lock poisoned");

        guard.file.write_all(&entry_buffer)?;
        guard.file.flush()?;
        // the current offset will be the offset of the entry we are inserting, save before
        // mutation
        let current_offset = guard.offset;
        guard.offset += entry_size;

        // should we rotate?
        let curr_file_id = self.file_id();
        let maybe_new_file = self.maybe_rotate_active_file(&mut guard, curr_file_id)?;

        Ok(FileWriteResult{
            write_offset: current_offset,
            written_file_id: curr_file_id,
            new_active_file: maybe_new_file,
        })
    }

    pub(crate) fn file_id(&self) -> usize {
        self.active_file_id.load(Ordering::Acquire)
    }

    /// Creates a new mutex file writer, returning the writer along with the FD
    /// of the underlined file. We need to return that FD because the caller would
    /// typically use it to form a file reader.
    ///
    pub fn create(engine_options: EngineOptions, data_file_id: usize) -> io::Result<(Self, File)> {
        let lock_file_path = engine_options.data_path.join(&engine_options.writer_lock_file_name);

        let lock_file = OpenOptions::new()
            .read(true)
            .append(true)
            // We force to create new to actually simulate a lock file.
            // TODO: Check how to prevent stale lock.
            .create_new(true)
            .open(&lock_file_path)?;
        debug!(
            "lock successfully acquired, created lock file at {:?}",
            lock_file_path
        );

        let data_file = create_active_file(&engine_options, data_file_id)?;
        let data_file_clone = data_file.try_clone()?;

        Ok((WriteHandler {
            active_file: Arc::new(Mutex::new(FileWithOffset::new(data_file, 0))),
            lock_file,
            engine_options,
            active_file_id: AtomicUsize::new(data_file_id),
        }, data_file_clone))
    }

    /// Opens a new file, mark it as active and returns the FD of the new file
    //We pass the file id to this method unnecessary atomic operations
    fn maybe_rotate_active_file(&self, guard: &mut MutexGuard<FileWithOffset>, current_active_file_id: usize) -> io::Result<Option<(File, usize)>> {
        if guard.offset >= self.engine_options.data_file_max_size {
            let new_data_file = create_active_file(&self.engine_options, current_active_file_id + 1)?;
            guard.file.sync_all()?;
            guard.file = new_data_file.try_clone()?;
            guard.offset = 0;
            self.active_file_id.fetch_add(1, Ordering::Release);
            return Ok(Some((new_data_file, current_active_file_id + 1)));
        }
        Ok(None)
    }

    /// write in batch to implement, for performance
    pub fn write_batch(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> io::Result<Vec<FileWriteResult>> {
        unimplemented!()
    }
}

impl Drop for WriteHandler {
    fn drop(&mut self) {
        let lock_file_path = self.engine_options.data_path.join(&self.engine_options.writer_lock_file_name);
        remove_file(lock_file_path)
            .unwrap_or_else(|e| error!("Failed to remove lock file: {}", e));

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
    use super::*;
    use crate::current_time_millis;
    use std::io::Read;
    use tempfile::TempDir;

    #[test]
    fn test_mutex_writer() {
        let dir = TempDir::new().unwrap();
        let engine_options = EngineOptions {
            data_path: dir.path().to_path_buf(),
            compress_old_files: false,
            data_file_max_size: 80,
            key_max_size: 12,
            value_max_size: 24,
            writer_lock_file_name: "writer.lock".to_string(),
        };

        let (mut writer, _) = WriteHandler::create(engine_options.clone(), 0).unwrap();

        // check if the written data matches what is expected
        let result = writer.write(b"key", b"value").unwrap();
        let mut file = File::open(engine_options.data_path.join("000000.data")).unwrap();
        let mut data = Vec::with_capacity(36);
        let line = file.read_to_end(&mut data).unwrap();
        assert_eq!(result.write_offset, 0);
        assert_eq!(line, 36);
        assert_eq!(result.written_file_id, 0);

        // check key size is well written
        assert_eq!(u64::from_le_bytes(data[12..20].try_into().unwrap()), 3);
        // check data size is well written
        assert_eq!(u64::from_le_bytes(data[20..28].try_into().unwrap()), 5);
        // check that a key is well written
        assert_eq!(String::from_utf8_lossy(&data[28..31]), "key");
        // check that data is well written
        assert_eq!(String::from_utf8_lossy(&data[31..36]), "value");
        // check that the timestamp in the file is in the past
        let file_timestamp = u64::from_le_bytes(data[4..12].try_into().unwrap());
        // put a sleep to make sure some time has passed since the put operation
        std::thread::sleep(std::time::Duration::from_millis(100));
        let now_timestamp = current_time_millis();
        assert!(file_timestamp < now_timestamp);

        // Assert we cannot have two writers
        assert!(WriteHandler::create(engine_options.clone(), 0).is_err());

        // let's write again and check that cursor move and no rotation
        let result = writer.write(b"key", b"value").unwrap();
        assert_eq!(result.write_offset, 36);
        assert!(result.new_active_file.is_none());
        assert_eq!(result.written_file_id, 0);

        // check that rotation happens when we write more than data_file_max_size == 80
        // actually written == 36 * 3. Rotation happens here, but the entry was written to the
        // previous file so the offset and file id of the written data should be from the previous
        // file.
        let result = writer.write(b"key", b"value").unwrap();
        assert_eq!(result.write_offset, 72);
        assert!(result.new_active_file.is_some());
        assert_eq!(result.written_file_id, 0);
        // check that the active file has changed
        assert_eq!(writer.file_id(), 1);

        // check that we can write to the new active file
        // check if the written data matches what is expected
        let result = writer.write(b"key", b"value").unwrap();
        let mut file = File::open(engine_options.data_path.join("000001.data")).unwrap();
        let mut data = Vec::with_capacity(36);
        let line = file.read_to_end(&mut data).unwrap();
        // as this is the first read in the new file, the offset of the writen data should come
        // back to 0.
        assert_eq!(result.write_offset, 0);
        assert_eq!(line, 36);
        assert_eq!(result.written_file_id, 1);

    }

    #[test]
    fn test_mutex_writer_existing_data_file() {
        let dir = TempDir::new().unwrap();
        let engine_options = EngineOptions {
            data_path: dir.path().to_path_buf(),
            compress_old_files: false,
            data_file_max_size: 10,
            key_max_size: 12,
            value_max_size: 24,
            writer_lock_file_name: "writer.lock".to_string(),
        };

        // manually create file 0 in a dir
        let mut file = File::create(engine_options.data_path.join("000000.data")).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();

        // Now let's try to create a writer. It should not fail
        let response = WriteHandler::create(engine_options.clone(), 0);
        assert!(response.is_ok());
    }
}
