use crate::fs_reader::FileReader;
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

pub struct MmapReader {
    _file: File,
    mmap: Mmap,
}

impl FileReader for MmapReader {
    fn read_at(&self, offset: u64, size: usize) -> io::Result<Vec<u8>> {
        let offset = offset as usize;
        Ok(self.mmap[offset..offset + size].to_vec())
    }
}

impl MmapReader {
    pub fn new(path_buf: &PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path_buf)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { _file: file, mmap })
    }
}
