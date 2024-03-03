//! FFI wrapper for functions exposed from zomdb.
//!
//! This module will eventually move to its own zomdb-sys crate.
use crate::{Error, Heap, Index};
use std::{ffi, fs, mem::transmute};

#[no_mangle]
pub unsafe extern "C" fn create_heap(file_name_cstr: *const ffi::c_char) -> *mut Heap {
    let file_name = from_cstr(file_name_cstr);

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
    let key = from_cstr(key_cstr);

    let value = heap.get(&key).unwrap();
    match value {
        Some(value) => to_cstr(&value),
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn heap_set(
    ptr: *mut Heap,
    key_cstr: *const ffi::c_char,
    value_cstr: *const ffi::c_char,
) {
    let heap = unsafe { &mut *ptr };
    let key = from_cstr(key_cstr);
    let value = from_cstr(value_cstr);

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

unsafe fn from_cstr(s: *const ffi::c_char) -> String {
    let cstr = unsafe { ffi::CStr::from_ptr(s) };
    cstr.to_str()
        // The crate is built to be used with Go, which should guarantee
        // strings to be utf8 encoded.
        .expect("value is not utf8")
        .to_owned()
}

unsafe fn to_cstr(s: &str) -> *const ffi::c_char {
    let cstr = ffi::CString::new(s).unwrap();
    cstr.into_raw()
}

pub const ERR_IO: i32 = 10;

fn to_errno(e: crate::Error) -> errno::Errno {
    let no = match e {
        Error::IOError(_) => ERR_IO,
    };

    errno::Errno(no)
}
