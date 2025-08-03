use crate::EngineOptions;
use crate::fs_writer::FileWriter;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions, remove_file};
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};


pub struct MutexFileWriter {
    // we have to maintain the offset manually because
    // entries need that information
    active_file: Arc<Mutex<(File, usize)>>,
    // TODO use me for multiprocess lock on data files
    lock_file: File,
    engine_options: EngineOptions,
    pub file_id: usize,
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

impl FileWriter for MutexFileWriter {
    fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<usize> {
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
        let now = SystemTime::now();

        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_millis() as u64;

        // 4 for CRC + 8 for key_size, + sizeof key, + 8 + sizeof data, + 8 for timestamp + 8 bits for checksum
        let entry_size = 4 + 8 + key_size + 8 + data_size + 8;
        let mut entry_buffer = Vec::with_capacity(entry_size);

        build_entry_buffer(key, value, timestamp, &mut entry_buffer);

        let mut guard = self.active_file.lock().expect("mutex lock poisoned");

        guard.0.write_all(&entry_buffer)?;
        guard.0.flush()?;
        // the current offset will be the offset of the entry we are inserting
        let current_offset = guard.1;
        guard.1 += entry_size;
        Ok(current_offset)
    }

    fn file_id(&self) -> usize {
        self.file_id
    }
}

impl MutexFileWriter {
    pub fn new(engine_options: EngineOptions, data_file_id: usize) -> io::Result<Self> {
        let lock_file_path = engine_options.data_path.join("write.lock");

        let lock_file = OpenOptions::new()
            .read(true)
            .append(true)
            // We force to create new to actually simulate a lock file.
            // TODO: Check how to prevent stale lock.
            .create_new(true)
            .open(&lock_file_path)?;
        tracing::debug!(
            "lock successfully acquired, created lock file at {:?}",
            lock_file_path
        );

        let data_file_path = engine_options
            .data_path
            .join(format!("{:06}.data", data_file_id));
        let data_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&data_file_path)?;
        tracing::debug!("opened initial data file at {:?}", data_file_path);

        Ok(MutexFileWriter {
            active_file: Arc::new(Mutex::new((data_file, 0))),
            lock_file,
            engine_options,
            file_id: data_file_id,
        })
    }

    pub fn rotate(&mut self) -> io::Result<()> {
        let mut guard = self.active_file.lock().expect("mutex lock poisoned");
        guard.0.sync_all()?;
        guard.0.flush()?;
        guard.0.set_len(0)?;
        guard.1 = 0;
        Ok(())
    }
}

impl Drop for MutexFileWriter {
    fn drop(&mut self) {
        let lock_file_path = self.engine_options.data_path.join("write.lock");
        remove_file(lock_file_path)
            .unwrap_or_else(|e| tracing::error!("Failed to remove lock file: {}", e));

        match self.active_file.lock() {
            Ok(guard) => {
                guard
                    .0
                    .sync_all()
                    .unwrap_or_else(|e| tracing::error!("failed to sync active file: {}", e));
            }
            Err(e) => tracing::error!("failed to lock active file mutex: {}", e),
        };

        if let Err(e) = self.active_file.lock().unwrap().0.sync_all() {
            tracing::error!("Failed to sync active file: {}", e);
        }
        tracing::debug!("MutexFileWriter dropped, lock file and active file closed.");
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
            data_file_max_size: 10,
            key_max_size: 12,
            value_max_size: 24,
        };

        let mut writer = MutexFileWriter::new(engine_options.clone(), 0).unwrap();

        // check if the written data matches what is expected
        let cursor = writer.write(b"key", b"value").unwrap();
        let mut file = File::open(engine_options.data_path.join("000000.data")).unwrap();
        let mut data = Vec::with_capacity(36);
        let line = file.read_to_end(&mut data).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(line, 36);
        // check key size is well written
        assert_eq!(u64::from_le_bytes(data[12..20].try_into().unwrap()), 3);
        // check data size is well written
        assert_eq!(u64::from_le_bytes(data[20..28].try_into().unwrap()), 5);
        // check that key is well written
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
        assert!(MutexFileWriter::new(engine_options.clone(), 0).is_err());
        // lets write again and check that cursor move
        let cursor = writer.write(b"key", b"value").unwrap();
        assert_eq!(cursor, 36);
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
        };

        // manually create file 0 in a dir
        let mut file = File::create(engine_options.data_path.join("000000.data")).unwrap();
        file.write_all(b"test").unwrap();
        file.sync_all().unwrap();

        // Now let's try to create a writer. It should not fail
        let response = MutexFileWriter::new(engine_options.clone(), 0);
        assert!(response.is_ok());
    }
}
