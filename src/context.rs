use crate::errors::Result;
use crate::{EngineOptions, Entry, FileReader, ReaderOptions};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, error};

/// Components shared between different parts of the engine.
pub struct SharedContext<const VERIFY_CRC: bool> {
    /// Various configuration options for the engine.
    pub engine_options: EngineOptions,
    pub rd_options: ReaderOptions,
    /// keydir, as defined in bitsect paper
    pub key_dir: RwLock<HashMap<Vec<u8>, Entry>>,
    /// Datafiles is a cache of the opened data files. The key is the file number, and the value is
    /// a FileReader that allows reading from the file. FileReader can be thought as a file
    /// descriptor. No ARC because the engine itself would be wrapped in an ARC.
    pub data_files: RwLock<BTreeMap<u64, FileReader<VERIFY_CRC>>>,
    pub file_id_allocator: FileIdAllocator,
}

impl<const VERIFY_CRC: bool> SharedContext<VERIFY_CRC> {
    pub fn new(options: EngineOptions, rd_options: ReaderOptions) -> Result<Self> {
        prepare_directories(&options)?;
        let file_ids = get_data_files_id(&options.data_path)?;
        let current_id = file_ids.last().unwrap_or(&0);
        let data_files: RwLock<BTreeMap<u64, FileReader<VERIFY_CRC>>> =
            RwLock::new(BTreeMap::new());
        Ok(Self {
            engine_options: options,
            rd_options,
            key_dir: RwLock::new(HashMap::new()),
            data_files,
            file_id_allocator: FileIdAllocator::new(*current_id),
        })
    }
}

fn prepare_directories(options: &EngineOptions) -> Result<()> {
    fs::create_dir_all(&options.data_path)?;

    if let Ok(metadata) = fs::metadata(&options.data_path) {
        let mut permissions = metadata.permissions();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(options.dir_mode);
        }

        #[cfg(windows)]
        {
            permissions.set_readonly(false);
        }

        fs::set_permissions(&options.data_path, permissions)?;
    }

    Ok(())
}

pub struct FileIdAllocator {
    counter: AtomicU64,
}
impl FileIdAllocator {
    pub fn new(current: u64) -> Self {
        Self {
            counter: AtomicU64::new(current),
        }
    }

    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Release)
    }

    pub fn current(&self) -> u64 {
        self.counter.load(Ordering::Acquire)
    }
}

/// Gets all the data files in the directory by their id in sorted order.
pub(crate) fn get_data_files_id(data_path: &Path) -> Result<Vec<u64>> {
    let mut data_file_ids = Vec::new();
    for file in data_path.read_dir()? {
        let file = file?;
        // skip non-files
        if !file.file_type()?.is_file() {
            continue;
        }

        let filename = file.file_name();
        if let Some(file_id) = filename.to_string_lossy().strip_suffix(".data") {
            let file_id = match file_id.parse::<u64>() {
                Ok(id) => id,
                Err(err) => {
                    // do not exit as this is not a fatal error
                    error!("failed to extract file ID from filename: {err}");
                    continue;
                }
            };
            data_file_ids.push(file_id);
        } else {
            debug!(
                "building data files ids: ignoring non-compatible file: {}",
                filename.to_string_lossy()
            );
        }
    }

    Ok(data_file_ids)
}
