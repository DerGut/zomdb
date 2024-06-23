//! FFI wrapper for functions exposed from the zomdb crate.
use std::{ffi, mem::transmute};
use zomdb::Index;

/// Heap is a primitive on-disk key-value structure.
///
/// A Heap can be used to set and get key-value pairs, and to iterate over them.
pub struct Heap {
    // Heap only delegates to the inner Heap.
    // This is because it isn't straightforward to generate FFI bindings
    // for external packages, so we redefine a Heap struct here instead.
    inner: zomdb::Heap
}

#[no_mangle]
pub unsafe extern "C" fn create_heap(file_name_cstr: *const ffi::c_char) -> *mut Heap {
    let file_name = match string_from_cstr(file_name_cstr) {
        Ok(s) => s,
        Err(e) => {
            println!("zomdb: file_name: {:?}", e);
            errno::set_errno(to_errno(zomdb::Error::Input(e)));
            return std::ptr::null_mut();
        }
    };

    println!("zomdb: opening heap file: {}", file_name);

    let heap = match zomdb::Heap::from(file_name.into()) {
        Ok(heap) => Heap{ inner: heap },
        Err(e) => {
            println!("zomdb: Heap::from: {:?}", e);
            errno::set_errno(to_errno(e));
            return std::ptr::null_mut();
        }
    };

    unsafe { transmute(Box::new(heap)) }
}

/// Get a value from the heap.
///
/// Returns a pointer to the value if found, or null if not found. If not
/// found, the global errno will be set to ERR_NOT_FOUND.
///
/// If an error occurs, the global errno will be set to the appropriate error.
///
/// The accepted key is a null-terminated string. Any calling code must
/// therefore guarantee that no null bytes are present in the key.
#[no_mangle]
pub unsafe extern "C" fn heap_get(
    ptr: *mut Heap,
    key_cstr: *const ffi::c_char,
) -> *const ffi::c_char {
    let heap = unsafe { &mut *ptr };

    let key = bytes_from_cstr(key_cstr);

    match heap.inner.get(&key) {
        Ok(Some(value)) => to_cstr(&value),
        Ok(None) => {
            errno::set_errno(errno::Errno(ERR_NOT_FOUND));
            std::ptr::null()
        }
        Err(e) => {
            println!("zomdb: heap.get: {:?}", e);
            errno::set_errno(to_errno(e));
            std::ptr::null()
        }
    }
}

/// Set a key and value in the heap.
///
/// If an error occurs, the global errno will be set to the appropriate error.
///
/// The accepted key and value are null-terminated strings. Any calling code
/// must therefore guarantee that no null bytes are present in the key or
/// value.
#[no_mangle]
pub unsafe extern "C" fn heap_set(
    ptr: *mut Heap,
    key_cstr: *const ffi::c_char,
    value_cstr: *const ffi::c_char,
) {
    let heap = unsafe { &mut *ptr };

    let key = bytes_from_cstr(key_cstr);
    let value = bytes_from_cstr(value_cstr);

    match heap.inner.put(&key, &value) {
        Ok(_) => {}
        Err(e) => {
            println!("zomdb: heap.put: {:?}", e);
            errno::set_errno(to_errno(e));
        }
    };
}

#[no_mangle]
pub unsafe extern "C" fn destroy_heap(ptr: *mut Heap) {
    let heap = unsafe { Box::from_raw(ptr) };
    drop(heap);
}

#[no_mangle]
pub extern "C" fn heap_iter(ptr: *mut Heap) -> *mut HeapIter<'static> {
    let heap = unsafe { &mut *ptr };
    let iter = heap.inner.iter();

    unsafe { transmute(Box::new(HeapIter { inner: iter })) }
}

/// Can be used to iterate a Heap structure.
///
/// Use heap_iter to create an instance of this struct from a Heap.
pub struct HeapIter<'a> {
    inner: zomdb::Iter<'a>,
}

#[no_mangle]
pub unsafe extern "C" fn heap_iter_next(ptr: *mut HeapIter) -> *const HeapTuple {
    let iter = unsafe { &mut *ptr };

    match iter.inner.next() {
        Some(Ok(tuple)) => {
            let tuple = HeapTuple {
                key: to_cstr(&tuple.key),
                value: to_cstr(&tuple.value),
            };
            unsafe { transmute(Box::new(tuple)) }
        }
        Some(Err(e)) => {
            println!("zomdb: heap_iter.next: {:?}", e);
            errno::set_errno(to_errno(e));
            std::ptr::null()
        }
        None => std::ptr::null(),
    }
}

/// HeapTuple is a key-value pair from a Heap.
#[repr(C)]
pub struct HeapTuple {
    key: *const ffi::c_char,
    value: *const ffi::c_char,
}

#[no_mangle]
pub extern "C" fn heap_iter_destroy(ptr: *mut HeapIter) {
    let iter = unsafe { Box::from_raw(ptr) };
    drop(iter);
}

unsafe fn string_from_cstr(s: *const ffi::c_char) -> Result<String, zomdb::InputError> {
    let cstr = unsafe { ffi::CStr::from_ptr(s) };
    let s = cstr.to_str().map_err(zomdb::InputError::Utf8)?;
    Ok(s.to_string())
}

unsafe fn bytes_from_cstr(s: *const ffi::c_char) -> Vec<u8> {
    let cstr = unsafe { ffi::CStr::from_ptr(s) };
    cstr.to_bytes().to_vec()
}

unsafe fn to_cstr(s: &[u8]) -> *const ffi::c_char {
    let cstr = ffi::CString::new(s).unwrap();
    cstr.into_raw()
}

/// Error code for keys that could not be found.
pub const ERR_NOT_FOUND: i32 = 1;

/// Error code for I/O errors.
pub const ERR_IO: i32 = 10;

/// Error code for invalid UTF-8.
/// Type of an input error.
pub const ERR_UTF8: i32 = 30;

/// Error code for invalid key size.
/// Type of an input error.
pub const ERR_KEY_SIZE: i32 = 31;

/// Error code for invalid value size.
/// Type of an input error.
pub const ERR_VALUE_SIZE: i32 = 32;

/// Error code for data errors.
/// Indicates that data on disk is corrupted.
pub const ERR_DATA: i32 = 50;

fn to_errno(e: zomdb::Error) -> errno::Errno {
    let no = match e {
        zomdb::Error::IO(_) => ERR_IO,
        zomdb::Error::Input(zomdb::InputError::Utf8(_)) => ERR_UTF8,
        zomdb::Error::Input(zomdb::InputError::KeySize(_)) => ERR_KEY_SIZE,
        zomdb::Error::Input(zomdb::InputError::ValueSize(_)) => ERR_VALUE_SIZE,
        zomdb::Error::Data(_) => ERR_DATA,
    };

    errno::Errno(no)
}
