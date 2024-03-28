#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Error code for I/O errors.
 */
#define ERR_IO 10

#define ERR_UTF8 30

typedef struct Heap Heap;

struct Heap *create_heap(const char *file_name_cstr);

const char *heap_get(struct Heap *ptr, const char *key_cstr);

void heap_set(struct Heap *ptr, const char *key_cstr, const char *value_cstr);

void destroy_heap(struct Heap *ptr);
