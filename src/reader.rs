use std::fs::{File, OpenOptions};
use std::io;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use tracing::debug;

/// pread-based reader
pub struct FileReader {
    // TODO add more context like file ID or name
    file: File,
}

impl FileReader {
    pub fn new(path_buf: &PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(false).open(path_buf)?;
        Ok(FileReader { file })
    }

    /// Reads the exact number of bytes required to fill buf from the given offset.
    /// The offset is relative to the start of the file and thus independent of the current cursor.
    /// The current file cursor is not affected by this function.
    /// Similar to io::Read::read_exact but uses read_at instead of read.
    ///
    /// Errors
    /// If this function encounters an error of the kind io::ErrorKind::Interrupted then the error is ignored and the operation will continue.
    /// If this function encounters an "end of file" before completely filling the buffer, it returns an error of the kind io::ErrorKind::UnexpectedEof. The contents of buf are unspecified in this case.
    /// If any other read error is encountered then this function immediately returns. The contents of buf are unspecified in this case.
    /// If this function returns an error, it is unspecified how many bytes it has read, but it will never read more than would be necessary to completely fill the buffer.
    pub(crate) fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, offset)
    }
}

impl From<File> for FileReader {
    fn from(file: File) -> Self {
        FileReader { file }
    }
}
