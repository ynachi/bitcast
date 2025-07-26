use crate::fs_reader::FileReader;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek};
use std::path::PathBuf;

/// A file reader based on std File and Seek trait. It leverages the standard library's
/// file handlers cloning capabilities to allow multiple readers to read from the same file
pub struct SeekReader {
    file: File,
}

impl FileReader for SeekReader {
    fn read_at(&mut self, offset: u64, size: usize, buf: &mut [u8]) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf[0..size])
    }
}

impl SeekReader {
    pub fn new(path_buf: &PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(path_buf)?;
        Ok(SeekReader { file })
    }
}