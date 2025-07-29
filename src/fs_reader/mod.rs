mod mmap;
mod pread;

use std::io;

pub trait FileReader: Send + Sync {
    /// Reads a specific number of bytes from the file at the given offset. It is advised to the
    /// user to pass a buffer of the exact size to avoid unnecessary allocations. Also,
    /// implementations should make sure readers do not step into each other's way.
    /// So implementations should make sure one user do not move the file cursor in a way that
    /// would affect another user.
    fn read_at(&self, offset: u64, size: usize) -> io::Result<Vec<u8>>;
}
