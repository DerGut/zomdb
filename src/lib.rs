use std::{
    ffi::CStr,
    fs,
    io::{self, BufRead, Error, Seek, Write},
    mem::transmute,
};

const MAX_KEY_SIZE: usize = 256;
const MAX_VALUE_SIZE: usize = 1024;

trait Index {
    fn put(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn get(&mut self, key: &str) -> Result<Option<String>, Error>;
}

#[no_mangle]
pub unsafe extern "C" fn create_heap(file_name: *const libc::c_char) -> *mut Heap {
    let file_name_cstr = unsafe { CStr::from_ptr(file_name) };
    let file_name_str = file_name_cstr.to_str().unwrap();

    println!("opening heap file: {}", file_name_str);

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_name_str)
        .unwrap();
    let heap = Box::new(Heap::new(file));

    unsafe { transmute(heap) }
}

#[no_mangle]
pub unsafe extern "C" fn heap_get(ptr: *mut Heap, key: *const libc::c_char) -> *const libc::c_char {
    let heap = unsafe { &mut *ptr };
    let key_cstr = unsafe { CStr::from_ptr(key) };
    let key_str = key_cstr.to_str().unwrap();

    let value = heap.get(key_str).unwrap();
    match value {
        Some(value) => {
            let value_cstr = std::ffi::CString::new(value).unwrap();
            value_cstr.into_raw()
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn heap_set(
    ptr: *mut Heap,
    key: *const libc::c_char,
    value: *const libc::c_char,
) {
    let heap = unsafe { &mut *ptr };
    let key_cstr = unsafe { CStr::from_ptr(key) };
    let key_str = key_cstr.to_str().unwrap();
    let value_cstr = unsafe { CStr::from_ptr(value) };
    let value_str = value_cstr.to_str().unwrap();

    heap.put(key_str, value_str).unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn destroy_heap(ptr: *mut Heap) {
    let heap = unsafe { Box::from_raw(ptr) };
    drop(heap);
}

pub struct Heap {
    file: fs::File,
}

impl Heap {
    fn new(file: fs::File) -> Self {
        Self { file }
    }

    fn serialize(key: &str, value: &str) -> Vec<u8> {
        assert!(key.len() <= MAX_KEY_SIZE);
        assert!(value.len() <= MAX_VALUE_SIZE);
        // 8bit for key size
        // 16bit for value size
        let mut data = Vec::with_capacity(key.len() + value.len() + 1 + 2);
        data.push(key.len() as u8);
        data.push((value.len() >> 8) as u8);
        data.push(value.len() as u8);
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(value.as_bytes());
        data
    }

    fn deserialize(data: &[u8]) -> Result<HeapTuple, DeserializationError> {
        assert!(data.len() >= 3);

        let key_size = data[0] as usize;
        if key_size > MAX_KEY_SIZE {
            return Err(DeserializationError::KeySizeTooBig);
        }

        let value_size = ((data[1] as usize) << 8) | data[2] as usize;
        if value_size > MAX_VALUE_SIZE {
            return Err(DeserializationError::ValueSizeTooBig);
        }

        if key_size + value_size + 3 < data.len() {
            return Err(DeserializationError::DataTooShort);
        }

        let key = std::str::from_utf8(&data[3..3 + key_size]).unwrap();
        let value = std::str::from_utf8(&data[3 + key_size..3 + key_size + value_size]).unwrap();

        Ok((key.to_string(), value.to_string()))
    }
}

type HeapTuple = (String, String);

impl Index for Heap {
    fn put(&mut self, key: &str, value: &str) -> Result<(), Error> {
        let bytes = Self::serialize(key, value);
        self.file.write_all(bytes.as_slice())
    }

    fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        self.file.rewind()?;
        let reader = io::BufReader::new(&self.file);

        // TODO: Read the file in reverse line by line instead of reading
        // everything into memory first.
        let lines: Vec<_> = reader.lines().map_while(|l| l.ok()).collect();

        for line in lines.iter().rev() {
            let heap_tuple = Self::deserialize(line.as_bytes()).unwrap();
            if heap_tuple.0 == key {
                return Ok(Some(heap_tuple.1));
            }
        }

        Ok(None)
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
        let serialized = vec![3, 0, 5, b'k', b'e', b'y', b'v', b'a', b'l', b'u', b'e'];
        let deserialized = Heap::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, ("key".to_string(), "value".to_string()));
    }

    #[test]
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
            vec![3, 0, 5, b'k', b'e', b'y', b'v', b'a', b'l', b'u', b'e']
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
