//! FFI wrapper for functions exposed from zomdb.
//!
//! This module will eventually move to its own zomdb-sys crate.
use crate::{Error, Heap, Index, InputError};
use std::{ffi, fs, mem::transmute};

#[no_mangle]
pub unsafe extern "C" fn create_heap(file_name_cstr: *const ffi::c_char) -> *mut Heap {
    let file_name = match from_cstr(file_name_cstr) {
        Ok(s) => s,
        Err(e) => {
            println!("zomdb: file_name: {:?}", e);
            errno::set_errno(to_errno(e));
            return std::ptr::null_mut();
        }
    };

    println!("zomdb: opening heap file: {}", file_name);

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_name)
        .unwrap();
    let heap = Box::new(Heap::new(file));

    unsafe { transmute(heap) }
}

#[no_mangle]
pub unsafe extern "C" fn heap_get(
    ptr: *mut Heap,
    key_cstr: *const ffi::c_char,
) -> *const ffi::c_char {
    let heap = unsafe { &mut *ptr };

    let key = match from_cstr(key_cstr) {
        Ok(s) => s,
        Err(e) => {
            println!("zomdb: key: {:?}", e);
            errno::set_errno(to_errno(e));
            return std::ptr::null();
        }
    };

    match heap.get(&key) {
        Ok(Some(value)) => to_cstr(&value),
        // TODO: Do we want to set errno here?
        Ok(None) => std::ptr::null(),
        Err(e) => {
            println!("zomdb: heap.get: {:?}", e);
            errno::set_errno(to_errno(e));
            std::ptr::null()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn heap_set(
    ptr: *mut Heap,
    key_cstr: *const ffi::c_char,
    value_cstr: *const ffi::c_char,
) {
    let heap = unsafe { &mut *ptr };

    let key = match from_cstr(key_cstr) {
        Ok(s) => s,
        Err(e) => {
            println!("zomdb: key: {:?}", e);
            errno::set_errno(to_errno(e));
            return;
        }
    };

    let value = match from_cstr(value_cstr) {
        Ok(s) => s,
        Err(e) => {
            println!("zomdb: value: {:?}", e);
            errno::set_errno(to_errno(e));
            return;
        }
    };

    match heap.put(&key, &value) {
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

unsafe fn from_cstr(s: *const ffi::c_char) -> Result<String, Error> {
    let cstr = unsafe { ffi::CStr::from_ptr(s) };
    match cstr.to_str() {
        Ok(s) => Ok(s.to_owned()),
        Err(e) => Err(Error::InputError(InputError::Utf8Error(e))),
    }
}

unsafe fn to_cstr(s: &str) -> *const ffi::c_char {
    let cstr = ffi::CString::new(s).unwrap();
    cstr.into_raw()
}

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

fn to_errno(e: crate::Error) -> errno::Errno {
    let no = match e {
        Error::IOError(_) => ERR_IO,
        Error::InputError(InputError::Utf8Error(_)) => ERR_UTF8,
        Error::InputError(InputError::KeySizeError(_)) => ERR_KEY_SIZE,
        Error::InputError(InputError::ValueSizeError(_)) => ERR_VALUE_SIZE,
    };

    errno::Errno(no)
}
