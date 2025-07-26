use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::sync::Arc;
use crate::fs_reader::FileReader;

/// A file reader based on std File and Seek trait. It leverages the standard library's
/// file handlers cloning capabilities to allow multiple readers to read from the same file
struct SeakReader {
    file: Arc<File>,
}

impl FileReader for SeakReader {
    fn read_at(&mut self, offset: u64, size: usize, buf: &mut [u8]) -> io::Result<()> {
        let mut handle = self.file.clone();
        handle.seek(io::SeekFrom::Start(offset))?;
        Ok(handle.read_exact(&mut buf[0..size])?)
    }
}

impl SeakReader {
    fn new(path_buf: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(path_buf)?;
        Ok(SeakReader { file: Arc::new(file) })
    }
}