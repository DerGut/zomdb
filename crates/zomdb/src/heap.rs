use std::{cmp, fs, io, path};
use std::collections::HashSet;
use std::io::{Read, Seek, Write};
use crate::{DeserializationError, Error, Index, InputError, MAX_KEY_SIZE, MAX_VALUE_SIZE};

pub struct Heap {
    file: fs::File,
}

impl Heap {
    /// The maximum byte size of a tuple on disk.
    const MAX_TUPLE_SIZE: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE + 3;

    /// The minimum byte size of a tuple on disk.
    const MIN_TUPLE_SIZE: usize = 1+3; // 1 byte key + 0 byte value

    fn new(file: fs::File) -> Self {
        Self { file }
    }

    pub fn from(path: path::PathBuf) -> Result<Self, Error> {
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

        // We use a single byte to encode the key size which allows to store
        // the value 255 as a maximum. We also require keys to be of at least
        // one byte in size. This means, that we don't need the 0 value and
        // can shift the encoded number by 1 to allow for key sizes of 256 bytes.
        let key_len = key.len() - 1;
        data.push(key_len as u8);

        data
    }

    fn deserialize(data: &[u8]) -> Result<HeapTuple, DeserializationError> {
        if data.len() < Self::MIN_TUPLE_SIZE {
            return Err(DeserializationError::DataTooShort);
        }

        let key_size = (data[data.len() - 1] as usize)+1;
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

    /// Returns an Iter that starts iterating from the last inserted tuple.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            file: &self.file,
            initialized: false,

            file_size: 0,
            file_offset: 0,

            chunk_buffer: Vec::new(),
            buffer_offset: 0,
            overflow: Vec::new(),

            seen_keys: HashSet::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct HeapTuple {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl HeapTuple {
    fn from(key: &[u8], value: &[u8]) -> Self {
        assert!(key.len()<=MAX_KEY_SIZE);
        assert!(!key.is_empty());
        assert!(value.len()<=MAX_VALUE_SIZE);
        HeapTuple {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn disk_len(&self) -> usize {
        self.key.len() + self.value.len() + 3
    }
}

impl<'a> IntoIterator for &'a Heap {
    type Item = Result<HeapTuple, Error>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

pub struct Iter<'a> {
    file: &'a fs::File,
    initialized: bool,

    file_size: u64,
    file_offset: u64, // offset measured from the beginning of the file

    chunk_buffer: Vec<u8>,
    buffer_offset: usize,
    overflow: Vec<u8>,

    seen_keys: HashSet<Vec<u8>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<HeapTuple, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_iter().transpose()
    }
}

impl<'a> Iter<'a> {

    const DEFAULT_CHUNK_SIZE: usize = Heap::MAX_TUPLE_SIZE;

    fn next_iter(&mut self) -> Result<Option<HeapTuple>, Error> {
        if !self.initialized {
            self.file_size = self.file.metadata().map_err(Error::IO)?.len();
            self.file_offset = self.file_size;
            self.initialized = true;
        }


        while self.file_bytes_remaining() > 0 {
            self.seek()?; // This call could be avoided if this run is reentrant
            if self.buffer_bytes_remaining() == 0 {
                self.fill_chunk_buffer()?;
                self.buffer_offset = 0;
            }

            while self.buffer_bytes_remaining() > 0 {
                // Read next tuple from the chunk buffer.

                let bytes = &self.chunk_buffer[..self.buffer_bytes_remaining()];
                let tuple =
                    match Heap::deserialize(bytes) {
                        Ok(tuple) => tuple,
                        Err(DeserializationError::DataTooShort) => {
                            // We've exhausted the buffer and need to read a new chunk from the file
                            // before completely deserializing this tuple. We move the remaining
                            // bytes to an overflow buffer to append them on the next chunk read.
                            self.overflow = Vec::from(bytes);
                            self.buffer_offset += self.overflow.len(); // Skip to the next chunk

                            continue
                        }
                        Err(e) => return Err(Error::Data(e)),
                    };

                self.buffer_offset += tuple.disk_len();

                if self.seen_keys.contains(&tuple.key) {
                    // We've already seen a more recent tuple with this key.
                    continue;
                }
                self.seen_keys.insert(tuple.key.clone());

                return Ok(Some(tuple));
            }

            self.file_offset -= self.buffer_offset as u64;
        }

        Ok(None)
    }

    fn seek(&mut self) -> Result<(), Error> {
        // In between calls to iter, new tuples may be appended to the file
        // which changes its size. Because the file is append-only, seeking
        // to offsets starting at the beginning should be safe.
        self.file.seek(io::SeekFrom::Start(self.file_offset)).map_err(Error::IO).map(|_| ())
    }

    fn file_bytes_remaining(&self) -> usize {
        self.file_offset as usize
    }

    fn buffer_bytes_remaining(&self) -> usize {
        self.chunk_buffer.len() - self.buffer_offset
    }

    fn fill_chunk_buffer(&mut self) -> Result<usize, Error> {
        let new_chunk_size = cmp::min(Self::DEFAULT_CHUNK_SIZE, self.file_bytes_remaining());

        self.file
            .seek(io::SeekFrom::Current(-(new_chunk_size as i64)))
            .map_err(Error::IO)?;

        self.chunk_buffer = vec![0u8; new_chunk_size];
        self.file
            .read_exact(&mut self.chunk_buffer)
            .map_err(Error::IO)?;

        if !self.overflow.is_empty() {
            // Empties self.overflow into chunk_buffer
            self.chunk_buffer.append(&mut self.overflow);
            assert!(self.overflow.is_empty());
        }

        Ok(new_chunk_size)
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
        for tuple in self.iter() {
            match tuple {
                Ok(tuple) => {
                    if tuple.key == key {
                        return Ok(Some(tuple.value));
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(None)
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
            vec![b'v', b'a', b'l', b'u', b'e', b'k', b'e', b'y', 0, 5, 2]
        );
    }

    #[test]
    fn test_heap_deserialize() {
        let serialized = vec![b'v', b'a', b'l', b'u', b'e', b'k', b'e', b'y', 0, 5, 2];
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
            vec![b'v', b'a', b'l', b'u', b'e', b'k', b'e', b'y', 0, 5, 2]
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
        let mut heap = Heap::new(heap_file);

        heap.put(b"key", b"ke\xf2").unwrap();
        let value = heap.get(b"key").unwrap();

        assert_eq!(value, Some(b"ke\xf2".to_vec()));
    }

    #[test]
    fn test_heap_iter() {
        let heap_file = tempfile().unwrap();
        let mut heap = Heap::new(heap_file);

        heap.put(b"key1", b"value1").unwrap();
        heap.put(b"key2", b"value2").unwrap();
        heap.put(b"key3", b"value3").unwrap();

        let mut iter = heap.iter();
        let tuple1 = iter.next().unwrap().unwrap();
        let tuple2 = iter.next().unwrap().unwrap();
        let tuple3 = iter.next().unwrap().unwrap();

        assert_eq!(tuple1, HeapTuple::from(b"key3", b"value3"));
        assert_eq!(tuple2, HeapTuple::from(b"key2", b"value2"));
        assert_eq!(tuple3, HeapTuple::from(b"key1", b"value1"));
    }

    #[test]
    fn test_heap_iter_skips_duplicate_keys() {
        let heap_file = tempfile().unwrap();
        let mut heap = Heap::new(heap_file);

        heap.put(b"key1", b"red").unwrap();
        heap.put(b"key2", b"green").unwrap();
        heap.put(b"key1", b"blue").unwrap();

        let mut iter = heap.iter();
        let tuple1 = iter.next().unwrap().unwrap();
        let tuple2 = iter.next().unwrap().unwrap();
        let tuple3 = iter.next();

        assert_eq!(tuple1, HeapTuple::from(b"key1", b"blue"));
        assert_eq!(tuple2, HeapTuple::from(b"key2", b"green"));
        assert!(tuple3.is_none());
    }

    #[test]
    fn test_heap_iter_handles_chunk_spanning_tuples() {
        let heap_file = tempfile().unwrap();
        let mut heap = Heap::new(heap_file);

        // Compute key and value size such that the second tuple will overshoot the chunk size.
        let test_tuple_size = (Iter::DEFAULT_CHUNK_SIZE / 2) + 5;
        let key_size = MAX_KEY_SIZE;
        let value_size = test_tuple_size - key_size;

        assert!(test_tuple_size <= Heap::MAX_TUPLE_SIZE, "test_tuple_size too large");
        assert!(value_size <= MAX_VALUE_SIZE, "value_size too large");

        let key1 = vec![1u8; key_size];
        let key2 = vec![2u8; key_size];
        let value1 = vec![3u8; value_size];
        let value2 = vec![4u8; value_size];

        heap.put(&key1, &value1).unwrap();
        heap.put(&key2, &value2).unwrap();

        let mut iter = heap.iter();
        let tuple2 = iter.next().unwrap().unwrap();
        let tuple1 = iter.next().unwrap().unwrap();

        assert_eq!(tuple2, HeapTuple::from(&key2, &value2), "latest tuple has unexpected value");
        assert_eq!(tuple1, HeapTuple::from(&key1, &value1));
    }
}

