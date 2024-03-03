use crate::{Heap, Index};
use std::{ffi, fs, mem::transmute};

#[no_mangle]
pub unsafe extern "C" fn create_heap(file_name: *const ffi::c_char) -> *mut Heap {
    let file_name_cstr = unsafe { ffi::CStr::from_ptr(file_name) };
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
pub unsafe extern "C" fn heap_get(ptr: *mut Heap, key: *const ffi::c_char) -> *const ffi::c_char {
    let heap = unsafe { &mut *ptr };
    let key_cstr = unsafe { ffi::CStr::from_ptr(key) };
    let key_str = key_cstr.to_str().unwrap();

    let value = heap.get(key_str).unwrap();
    match value {
        Some(value) => {
            let value_cstr = ffi::CString::new(value).unwrap();
            value_cstr.into_raw()
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn heap_set(
    ptr: *mut Heap,
    key: *const ffi::c_char,
    value: *const ffi::c_char,
) {
    let heap = unsafe { &mut *ptr };
    let key_cstr = unsafe { ffi::CStr::from_ptr(key) };
    let key_str = key_cstr.to_str().unwrap();
    let value_cstr = unsafe { ffi::CStr::from_ptr(value) };
    let value_str = value_cstr.to_str().unwrap();

    heap.put(key_str, value_str).unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn destroy_heap(ptr: *mut Heap) {
    let heap = unsafe { Box::from_raw(ptr) };
    drop(heap);
}
