#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Error code for keys that could not be found.
 */
#define ERR_NOT_FOUND 1

/**
 * Error code for I/O errors.
 */
#define ERR_IO 10

/**
 * Error code for invalid UTF-8.
 * Type of an input error.
 */
#define ERR_UTF8 30

/**
 * Error code for invalid key size.
 * Type of an input error.
 */
#define ERR_KEY_SIZE 31

/**
 * Error code for invalid value size.
 * Type of an input error.
 */
#define ERR_VALUE_SIZE 32

/**
 * Error code for data errors.
 * Indicates that data on disk is corrupted.
 */
#define ERR_DATA 50

/**
 * Heap is a primitive on-disk key-value structure.
 *
 * A Heap can be used to set and get key-value pairs, and to iterate over them.
 */
typedef struct Heap Heap;

/**
 * Can be used to iterate a Heap structure.
 *
 * Use heap_iter to create an instance of this struct from a Heap.
 */
typedef struct HeapIter HeapIter;

struct Heap *create_heap(const char *file_name_cstr);

/**
 * Get a value from the heap.
 *
 * Returns a pointer to the value if found, or null if not found. If not
 * found, the global errno will be set to ERR_NOT_FOUND.
 *
 * If an error occurs, the global errno will be set to the appropriate error.
 *
 * The accepted key is a null-terminated string. Any calling code must
 * therefore guarantee that no null bytes are present in the key.
 */
const char *heap_get(struct Heap *ptr, const char *key_cstr);

/**
 * Set a key and value in the heap.
 *
 * If an error occurs, the global errno will be set to the appropriate error.
 *
 * The accepted key and value are null-terminated strings. Any calling code
 * must therefore guarantee that no null bytes are present in the key or
 * value.
 */
void heap_set(struct Heap *ptr, const char *key_cstr, const char *value_cstr);

void destroy_heap(struct Heap *ptr);

struct HeapIter *heap_iter(struct Heap *ptr);

const char *heap_iter_next(struct HeapIter *ptr);

void heap_iter_destroy(struct HeapIter *ptr);
