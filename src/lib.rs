use std::{
    cmp, error, fmt, fs,
    io::{self, Read, Seek, Write},
    path, str,
};

mod sys;

/// The maximum byte size of keys.
const MAX_KEY_SIZE: usize = 256;

/// The maximum byte size of values.
const MAX_VALUE_SIZE: usize = 1024;

trait Index {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
}

#[derive(Debug)]
enum Error {
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
enum InputError {
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

pub struct Heap {
    file: fs::File,
}

impl Heap {
    fn new(file: fs::File) -> Self {
        Self { file }
    }

    fn from(path: path::PathBuf) -> Result<Self, Error> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(path)
            .map_err(Error::IO)?;
        Ok(Self::new(file))
    }

    fn serialize(key: &[u8], value: &[u8]) -> Vec<u8> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        // 8bit for key size
        // 16bit for value size
        let mut data = Vec::with_capacity(key.len() + value.len() + 1 + 2);
        data.extend_from_slice(value);
        data.extend_from_slice(key);
        data.push((value.len() >> 8) as u8);
        data.push(value.len() as u8);
        data.push(key.len() as u8);
        data
    }

    fn deserialize(data: &[u8]) -> Result<HeapTuple, DeserializationError> {
        assert!(data.len() > 3);

        let key_size = data[data.len() - 1] as usize;
        if key_size > MAX_KEY_SIZE {
            return Err(DeserializationError::KeySizeTooBig);
        }

        let value_size = ((data[data.len() - 3] as usize) << 8) | data[data.len() - 2] as usize;
        if value_size > MAX_VALUE_SIZE {
            return Err(DeserializationError::ValueSizeTooBig);
        }

        if data.len() < key_size + value_size + 3 {
            return Err(DeserializationError::DataTooShort);
        }

        let key = &data[data.len() - 3 - key_size..data.len() - 3];
        let value = &data[data.len() - 3 - key_size - value_size..data.len() - 3 - key_size];

        Ok(HeapTuple::from(key, value))
    }
}

#[derive(Debug, PartialEq)]
struct HeapTuple {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl HeapTuple {
    fn from(key: &[u8], value: &[u8]) -> Self {
        HeapTuple {
            // TODO: check again, what's more idiomatic?
            key: key.to_owned(),
            value: value.to_vec(),
        }
    }

    fn disk_len(&self) -> usize {
        self.key.len() + self.value.len() + 3
    }
}

impl Index for Heap {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if key.len() > MAX_KEY_SIZE || key.is_empty() {
            return Err(Error::Input(InputError::KeySize(key.len())));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::Input(InputError::ValueSize(value.len())));
        }

        let bytes = Self::serialize(key, value);

        self.file.write_all(bytes.as_slice()).map_err(Error::IO)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        search_reverse(key, &self.file)
    }
}

fn search_reverse(key: &[u8], mut file: &fs::File) -> Result<Option<Vec<u8>>, Error> {
    const MAX_TUPLE_SIZE: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE + 3;

    file.seek(io::SeekFrom::End(0)).map_err(Error::IO)?;

    let file_size = file.metadata().map_err(Error::IO)?.len() as usize;

    let mut bytes_remaining = file_size;
    while bytes_remaining > 0 {
        let current_chunk_size = cmp::min(MAX_TUPLE_SIZE, bytes_remaining);

        file.seek(io::SeekFrom::Current(-(current_chunk_size as i64)))
            .map_err(Error::IO)?;

        let mut chunk_buffer = vec![0u8; current_chunk_size];
        file.read_exact(&mut chunk_buffer).map_err(Error::IO)?;

        const MIN_TUPLE_SIZE: usize = 4;
        let mut unread_chunk_bytes = current_chunk_size;
        while unread_chunk_bytes > MIN_TUPLE_SIZE {
            // Check, whether we already read more tuples into the current buffer.
            let tuple = match Heap::deserialize(&chunk_buffer[..unread_chunk_bytes]) {
                Ok(tuple) => tuple,
                Err(DeserializationError::DataTooShort) => {
                    // We've exhausted the buffer and need to read a new chunk.
                    // TODO: move these bytes into an overflow buffer and
                    // re-enter the main chunk read loop.
                    panic!("TODO");
                }
                Err(e) => return Err(Error::Data(e)),
            };

            // TODO: How does this compare? Should we use mem::cmp instead?
            if tuple.key == key {
                return Ok(Some(tuple.value));
            }

            unread_chunk_bytes -= tuple.disk_len();
            bytes_remaining -= tuple.disk_len();
        }
    }

    Ok(None)
}

#[derive(Debug)]
enum DeserializationError {
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

#[cfg(test)]
mod test {
    use std::{
        io::{Read, Seek},
        vec,
    };
    use tempfile::tempfile;

    use super::*;

    #[test]
    fn test_heap_serialize() {
        let serialized = Heap::serialize(b"key", b"value");
        assert_eq!(
            serialized,
            vec![b'v', b'a', b'l', b'u', b'e', b'k', b'e', b'y', 0, 5, 3]
        );
    }

    #[test]
    fn test_heap_deserialize() {
        let serialized = vec![b'v', b'a', b'l', b'u', b'e', b'k', b'e', b'y', 0, 5, 3];
        let deserialized = Heap::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, HeapTuple::from(b"key", b"value"));
    }

    #[test]
    fn test_heap_serde() {
        let key = b"key";
        let value = b"value";

        let serialized = Heap::serialize(key, value);
        let deserialized = Heap::deserialize(&serialized).unwrap();

        assert_eq!(deserialized, HeapTuple::from(key, value),);
    }

    #[test]
    fn test_heap_get() {
        let mut heap_file = tempfile().unwrap();

        heap_file
            .write_all(&Heap::serialize(b"key", b"value"))
            .unwrap();
        heap_file.rewind().unwrap();

        let mut heap = Heap::new(heap_file);
        let value = heap.get(b"key").unwrap();

        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_heap_put() {
        let heap_file = tempfile().unwrap();

        let mut heap = Heap::new(heap_file);
        heap.put(b"key", b"value").unwrap();

        heap.file.rewind().unwrap();

        let mut buf = Vec::new();
        heap.file.read_to_end(&mut buf).unwrap();

        assert_eq!(
            buf,
            vec![b'v', b'a', b'l', b'u', b'e', b'k', b'e', b'y', 0, 5, 3]
        );
    }

    #[test]
    fn test_heap_put_get() {
        let heap_file = tempfile().unwrap();
        let mut heap = Heap::new(heap_file);

        heap.put(b"key", b"value").unwrap();
        let value = heap.get(b"key").unwrap();

        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_heap_put_get_multiple() {
        let heap_file = tempfile().unwrap();
        let mut heap = Heap::new(heap_file);

        heap.put(b"key1", b"value1").unwrap();
        heap.put(b"key2", b"value2").unwrap();
        heap.put(b"key3", b"value3").unwrap();

        let value1 = heap.get(b"key1").unwrap();
        let value2 = heap.get(b"key2").unwrap();
        let value3 = heap.get(b"key3").unwrap();

        assert_eq!(value1, Some(b"value1".to_vec()));
        assert_eq!(value2, Some(b"value2".to_vec()));
        assert_eq!(value3, Some(b"value3".to_vec()));
    }

    #[test]
    fn test_heap_put_get_non_utf8_bytes() {
        let heap_file = tempfile().unwrap();
        let heap = Heap::new(heap_file);

        _ = heap;
        panic!("todo: find an example byte string")
    }
}
