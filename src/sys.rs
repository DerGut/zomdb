//! FFI wrapper for functions exposed from zomdb.
//!
//! This module will eventually move to its own zomdb-sys crate.
use crate::{Error, Heap, Index, InputError};
use std::{ffi, mem::transmute};

#[no_mangle]
pub unsafe extern "C" fn create_heap(file_name_cstr: *const ffi::c_char) -> *mut Heap {
    let file_name = match string_from_cstr(file_name_cstr) {
        Ok(s) => s,
        Err(e) => {
            println!("zomdb: file_name: {:?}", e);
            errno::set_errno(to_errno(Error::Input(e)));
            return std::ptr::null_mut();
        }
    };

    println!("zomdb: opening heap file: {}", file_name);

    let heap = match Heap::from(file_name.into()) {
        Ok(heap) => heap,
        Err(e) => {
            println!("zomdb: Heap::from: {:?}", e);
            errno::set_errno(to_errno(e));
            return std::ptr::null_mut();
        }
    };

    unsafe { transmute(Box::new(heap)) }
}

#[no_mangle]
pub unsafe extern "C" fn heap_get(
    ptr: *mut Heap,
    key_cstr: *const ffi::c_char,
) -> *const ffi::c_char {
    let heap = unsafe { &mut *ptr };

    let key = bytes_from_cstr(key_cstr);

    match heap.get(&key) {
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

#[no_mangle]
pub unsafe extern "C" fn heap_set(
    ptr: *mut Heap,
    key_cstr: *const ffi::c_char,
    value_cstr: *const ffi::c_char,
) {
    let heap = unsafe { &mut *ptr };

    let key = bytes_from_cstr(key_cstr);
    let value = bytes_from_cstr(value_cstr);

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

unsafe fn string_from_cstr(s: *const ffi::c_char) -> Result<String, InputError> {
    let cstr = unsafe { ffi::CStr::from_ptr(s) };
    let s = cstr.to_str().map_err(InputError::Utf8)?;
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

fn to_errno(e: crate::Error) -> errno::Errno {
    let no = match e {
        Error::IO(_) => ERR_IO,
        Error::Input(InputError::Utf8(_)) => ERR_UTF8,
        Error::Input(InputError::KeySize(_)) => ERR_KEY_SIZE,
        Error::Input(InputError::ValueSize(_)) => ERR_VALUE_SIZE,
        Error::Data(_) => ERR_DATA,
    };

    errno::Errno(no)
}
