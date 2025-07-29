mod mutex;

pub trait FileWriter {
    // The write method should return the offset of the file
    fn write(&mut self, key: &[u8], data: &[u8]) -> std::io::Result<usize>;

    fn file_id(&self) -> usize;
}
