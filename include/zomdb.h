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

typedef struct Heap Heap;

struct Heap *create_heap(const char *file_name_cstr);

const char *heap_get(struct Heap *ptr, const char *key_cstr);

void heap_set(struct Heap *ptr, const char *key_cstr, const char *value_cstr);

void destroy_heap(struct Heap *ptr);
