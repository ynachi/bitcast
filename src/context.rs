use crate::errors::Result;
use crate::{EngineOptions, Entry, FileReader};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path};
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, error};

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
    pub fn new(options: EngineOptions) -> Result<Self> {
        prepare_directories(&options)?;
        let file_ids = get_data_files_id(&options.data_path)?;
        let current_id = file_ids.last().unwrap_or(&0);
        Ok(Self {
            options,
            key_dir: RwLock::new(HashMap::new()),
            data_files: RwLock::new(BTreeMap::new()),
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
    counter: AtomicUsize,
}
impl FileIdAllocator {
    pub fn new(current: usize) -> Self {
        Self {
            counter: AtomicUsize::new(current),
        }
    }

    pub fn next(&self) -> usize {
        self.counter.fetch_add(1, Ordering::Release)
    }

    pub fn current(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }
}

/// Gets all the data files in the directory by their id in sorted order.
pub(crate) fn get_data_files_id(data_path: &Path) -> Result<Vec<usize>> {
    let mut data_file_ids = Vec::new();
    for file in data_path.read_dir()? {
        let file = file?;
        // skip non-files
        if !file.file_type()?.is_file() {
            continue;
        }

        let filename = file.file_name();
        if let Some(file_id) = filename.to_string_lossy().strip_suffix(".data") {
            let file_id = match file_id.parse::<usize>() {
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
