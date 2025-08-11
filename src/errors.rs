use std::fmt::{Display, Formatter};
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    MmapReadOverflow(usize, usize, usize),
    // Invalid CRC with payload size in case one wants to skip the corrupted data
    // payload == full entry without the header
    InvalidCRC(usize),
    KeyTooBig(usize, usize),
    ValueTooBig(usize, usize),
    BufToArray,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "IO error: {}", err),
            Error::MmapReadOverflow(read_sz, file_sz, offset) => write!(
                f,
                "File reader: read size {} exceed file size {} offset {}",
                read_sz, file_sz, offset
            ),
            Error::InvalidCRC(payload_sz) => {
                write!(f, "File reader: invalid CRC, payload size {}", payload_sz)
            }
            Error::KeyTooBig(key_sz, max_sz) => write!(
                f,
                "key size {} exceed the maximum allowed {}",
                key_sz, max_sz
            ),
            Error::ValueTooBig(value_sz, max_sz) => write!(
                f,
                "value size {} exceed the maximum allowed {}",
                value_sz, max_sz
            ),
            Error::BufToArray => write!(f, "failed to convert a buffer to an array"),
        }
    }
}

// Implementation of Error trait for Error
impl std::error::Error for Error {}

// Implementation to convert io::Error into Error
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}
