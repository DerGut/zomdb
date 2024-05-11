use std::{
    error, fmt,
    io::{self},
    str,
};

mod heap;

pub use heap::Heap;

/// The maximum byte size of keys.
const MAX_KEY_SIZE: usize = 256;

/// The maximum byte size of values.
const MAX_VALUE_SIZE: usize = 1024;

pub trait Index {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
}

#[derive(Debug)]
pub enum Error {
    Input(InputError),
    IO(io::Error),

    /// Indicates that the data on disk was corrupted.
    Data(DeserializationError),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Input(e) => write!(f, "Input error: {}", e),
            Error::IO(e) => write!(f, "IO error: {}", e),
            Error::Data(e) => write!(f, "Data error: {}", e),
        }
    }
}

#[derive(Debug)]
pub enum InputError {
    Utf8(str::Utf8Error),
    KeySize(usize),
    ValueSize(usize),
}

impl error::Error for InputError {}

impl fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputError::Utf8(e) => write!(f, "UTF-8 error: {}", e),
            InputError::KeySize(size) => {
                write!(f, "Key size not in [1,{}]: {}", MAX_KEY_SIZE, size)
            }
            InputError::ValueSize(size) => {
                write!(f, "Value size not in [1,{}]: {}", MAX_VALUE_SIZE, size)
            }
        }
    }
}

#[derive(Debug)]
pub enum DeserializationError {
    KeySizeTooBig,
    ValueSizeTooBig,
    DataTooShort,
}

impl error::Error for DeserializationError {}

impl fmt::Display for DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeserializationError::KeySizeTooBig => write!(f, "Key size too big"),
            DeserializationError::ValueSizeTooBig => {
                write!(f, "Value size too big")
            }
            DeserializationError::DataTooShort => {
                write!(f, "data buffer too short")
            }
        }
    }
}
