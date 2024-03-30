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
    fn put(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn get(&mut self, key: &str) -> Result<Option<String>, Error>;
}

#[derive(Debug)]
enum Error {
    InputError(InputError),
    IOError(io::Error),

    /// Indicates that the data on disk was corrupted.
    DataError(DeserializationError),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InputError(e) => write!(f, "Input error: {}", e),
            Error::IOError(e) => write!(f, "IO error: {}", e),
            Error::DataError(e) => write!(f, "Data error: {}", e),
        }
    }
}

#[derive(Debug)]
enum InputError {
    Utf8Error(str::Utf8Error),
    KeySizeError(usize),
    ValueSizeError(usize),
}

impl error::Error for InputError {}

impl fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputError::Utf8Error(e) => write!(f, "UTF-8 error: {}", e),
            InputError::KeySizeError(size) => {
                write!(f, "Key size not in [1,{}]: {}", MAX_KEY_SIZE, size)
            }
            InputError::ValueSizeError(size) => {
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
            .map_err(Error::IOError)?;
        Ok(Self::new(file))
    }

    fn serialize(key: &str, value: &str) -> Vec<u8> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        // 8bit for key size
        // 16bit for value size
        let mut data = Vec::with_capacity(key.len() + value.len() + 1 + 2);
        data.extend_from_slice(value.as_bytes());
        data.extend_from_slice(key.as_bytes());
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

        let key = std::str::from_utf8(&data[data.len() - 3 - key_size..data.len() - 3]).unwrap();
        let value = std::str::from_utf8(
            &data[data.len() - 3 - key_size - value_size..data.len() - 3 - key_size],
        )
        .unwrap();

        Ok(HeapTuple::from(key, value))
    }
}

#[derive(Debug, PartialEq)]
struct HeapTuple {
    key: String,
    value: String,
}

impl HeapTuple {
    fn from(key: &str, value: &str) -> Self {
        return HeapTuple {
            key: key.to_string(),
            value: value.to_string(),
        };
    }

    fn disk_len(&self) -> usize {
        return self.key.len() + self.value.len() + 3;
    }
}

impl Index for Heap {
    fn put(&mut self, key: &str, value: &str) -> Result<(), Error> {
        if key.len() > MAX_KEY_SIZE || key.len() == 0 {
            return Err(Error::InputError(InputError::KeySizeError(key.len())));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::InputError(InputError::ValueSizeError(value.len())));
        }

        let bytes = Self::serialize(key, value);

        self.file
            .write_all(bytes.as_slice())
            .map_err(Error::IOError)
    }

    fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        search_key_reverse(key, &self.file)
        // self.file.rewind().map_err(Error::IOError)?;
        // let reader = io::BufReader::new(&self.file);

        // // TODO: Read the file in reverse line by line instead of reading
        // // everything into memory first.
        // let lines: Vec<_> = reader.lines().map_while(|l| l.ok()).collect();

        // for line in lines.iter().rev() {
        //     let heap_tuple = match Self::deserialize(line.as_bytes()) {
        //         Ok(heap_tuple) => heap_tuple,
        //         Err(e) => {
        //             print!("Skipping line: {}", e);
        //             // This means that the line is not a valid heap tuple and
        //             // our file is corrupted. We skip this line for now but
        //             // should at least log this.
        //             continue;
        //         }
        //     };
        //     if heap_tuple.0 == key {
        //         return Ok(Some(heap_tuple.1));
        //     }
        // }

        // Ok(None)
    }
}

#[derive(Debug)]
enum DeserializationError {
    KeySizeTooBig,
    ValueSizeTooBig,
    DataTooShort,
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
        let serialized = Heap::serialize("key", "value");
        assert_eq!(
            serialized,
            vec![3, 0, 5, b'k', b'e', b'y', b'v', b'a', b'l', b'u', b'e']
        );
    }

    #[test]
    fn test_heap_deserialize() {
        let serialized = vec![b'v', b'a', b'l', b'u', b'e', b'k', b'e', b'y', 0, 5, 3];
        let deserialized = Heap::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, HeapTuple::from("key", "value"),);
    }

    #[test]
    fn test_heap_serde() {
        let key = "key";
        let value = "value";

        let serialized = Heap::serialize(&key, &value);
        let deserialized = Heap::deserialize(&serialized).unwrap();

        assert_eq!(deserialized, HeapTuple::from(key, value),);
    }

    #[test]
impl error::Error for DeserializationError {}

impl fmt::Display for DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeserializationError::KeySizeTooBig => write!(f, "Key size too big"),
            DeserializationError::ValueSizeTooBig => write!(f, "Value size too big"),
            DeserializationError::DataTooShort => write!(f, "Data too short"),
        }
    }
}

fn search_key_reverse(key: &str, file: &fs::File) -> Result<Option<String>, Error> {
    return search_reverse_chunked(key, file);
}

fn search_reverse_chunked(key: &str, mut file: &fs::File) -> Result<Option<String>, Error> {
    const MAX_TUPLE_SIZE: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE + 3;

    file.seek(io::SeekFrom::End(0)).map_err(Error::IOError)?;

    let file_size = file.metadata().map_err(Error::IOError)?.len() as usize;

    let mut bytes_remaining = file_size;
    while bytes_remaining > 0 {
        let current_chunk_size = cmp::min(MAX_TUPLE_SIZE, bytes_remaining);

        // TODO: We don't seek to the beginning of the tuple, but to the
        // beginning of the max tuple size. This means, that the next seek
        // will seek relative from the beginning of the buffer, not relative
        // from where the tuple ends.
        // TODO: Either consuming the remaining buffer until we get
        // DataTooShort, or reread some data into a new buffer.
        // Seek to beginning of buffer read
        file.seek(io::SeekFrom::Current(-(current_chunk_size as i64)))
            .map_err(Error::IOError)?;

        let mut chunk_buffer = vec![0u8; current_chunk_size];
        file.read_exact(&mut chunk_buffer).map_err(Error::IOError)?;

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
                Err(e) => return Err(Error::DataError(e)),
            };

            if tuple.key == key {
                return Ok(Some(tuple.value));
            }

            unread_chunk_bytes -= tuple.disk_len();
            bytes_remaining -= tuple.disk_len();
        }
    }

    Ok(None)
}

    fn test_heap_get() {
        let mut heap_file = tempfile().unwrap();

        heap_file
            .write_all(&Heap::serialize("key", "value"))
            .unwrap();
        heap_file.rewind().unwrap();

        let mut heap = Heap::new(heap_file);
        let value = heap.get("key").unwrap();

        assert_eq!(value, Some("value".to_string()));
    }

    #[test]
    fn test_heap_put() {
        let heap_file = tempfile().unwrap();

        let mut heap = Heap::new(heap_file);
        heap.put("key", "value").unwrap();

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

        heap.put("key", "value").unwrap();
        let value = heap.get("key").unwrap();

        assert_eq!(value, Some("value".to_string()));
    }
}

    #[test]
    fn test_heap_put_get_multiple() {
        let heap_file = tempfile().unwrap();
        let mut heap = Heap::new(heap_file);

        heap.put("key1", "value1").unwrap();
        heap.put("key2", "value2").unwrap();
        heap.put("key3", "value3").unwrap();

        let value1 = heap.get("key1").unwrap();
        let value2 = heap.get("key2").unwrap();
        let value3 = heap.get("key3").unwrap();

        assert_eq!(value1, Some("value1".to_string()));
        assert_eq!(value2, Some("value2".to_string()));
        assert_eq!(value3, Some("value3".to_string()));
    }
