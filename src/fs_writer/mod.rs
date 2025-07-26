mod mutex;

pub trait FileWriter {
    // Implementation details for writing to a file
    fn write(&mut self, key: &[u8], data: &[u8]) -> std::io::Result<()>;
}

