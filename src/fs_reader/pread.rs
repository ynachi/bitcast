use crate::fs_reader::FileReader;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

/// A file reader based on std File and Seek trait. It leverages the standard library's
/// file handlers cloning capabilities to allow multiple readers to read from the same file
pub struct PreadReader {
    file: File,
}

impl FileReader for PreadReader {
    fn read_at(&self, offset: u64, size: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0; size];
        self.file.read_at(&mut buf[0..size], offset)?;
        Ok(buf)
    }
}

impl PreadReader {
    pub fn new(path_buf: &PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(false).open(path_buf)?;
        Ok(PreadReader { file })
    }
}
