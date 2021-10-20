/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers

// Internal headers
#include <tm.h>

// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
**/
#undef likely
#ifdef __GNUC__
    #define likely(prop) \
        __builtin_expect((prop) ? 1 : 0, 1)
#else
    #define likely(prop) \
        (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
**/
#undef unlikely
#ifdef __GNUC__
    #define unlikely(prop) \
        __builtin_expect((prop) ? 1 : 0, 0)
#else
    #define unlikely(prop) \
        (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
**/
#undef as
#ifdef __GNUC__
    #define as(type...) \
        __attribute__((type))
#else
    #define as(type...)
    #warning This compiler has no support for GCC attributes
#endif

// ------------------------------BATCHER------------------------------------- //
batcher_t init_batcher() {
    batcher_t batcher;
    batcher->counter = 0;
    batcher->remaining = 0;
    batcher->blocked = calloc(N, sizeof(thread_t*));
    batcher->size = 0;
    return batcher;
};

int get_epoch(batcher* batcher) {
    return batcher->counter;
}

int enter(batcher* batcher, thread_t* thread) {
    if (batcher->remaining == 0) {
        batcher->remaining = 1;
    } else {
        if(batcher->size == N) {
            batcher->blocked = realloc(batcher->blocked, N * sizeof(threzad_t*)) != null
        }
        if(batcher->blocked != null) {
            (batcher->blocked)[batcher->size] = thread;
        }
        batcher->size += 1;
        while(thread->sleeping)
    }
    return 0;
}

int leave(batcher* batcher, thread_t* thread) {
    batcher->remaining -= 1;
    if (batcher->remaining == 0) {
        batcher->counter += 1;
        batcher->remaining = batcher->size;
        // Wake up everyone
        for(i = 0; i < batcher->size; i++) {
            batcher->blocked[s]->sleeping = 0;
        }
        free(batcher->blocked) // empty the list
        batcher->blocked = calloc(N, sizeof(thread_t*)); // reallocate a block of memory
        batcher->size = 0;
    }
}

// ------------------------------HELPER------------------------------------- //
segment_t init_segment(size_t size) {
    segment_t segment;
    segment.next = NULL;
    segment.prev = NULL;
    segment.size = size;
    segment.words = calloc(1, size); // size is a multiple of align, there are (size / align) allocated words
    segment.words_copy = calloc(1, size);

    segment_control_t control;
    control.valid = VALID;
    control.writable = WRITABLE;
    control.modified = NOT_MODIFIED;
    segment.control = control;

    return segment;
}

void free_segment(segment_t* segment) {
    free(segment->words);
    free(segment->words_copy);
}

// ------------------------------TRANSACTIONS------------------------------- //
/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment
 *  of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple
 *   of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/

shared_t tm_create(size_t size, size_t align) {
    if(size % align != 0) {
        return invalid_shared;
    }
    region_t* region = calloc(1, sizeof(region_t));
    region->init_segment = init_segment(size);
    region->size = size;
    region->align = align;
    region->batcher = init_batcher();

    shared_t shared = region;
    return shared;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    region_t* region = ((region_t*)shared)
    segment_t* current = &(region->init_segment);
    segment_t* next = current;
    while (current->next != NULL) {
        next = current->next;
        free_segment(current);
        current = next;
    }
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    return &((region_t*)shared)->init_segment;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    return ((region_t*)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    return ((region_t*)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared as(unused), bool is_ro as(unused)) {
    tx_t transaction;
    region_t smr = *(region_t)shared;
    // batcher.enter()
    enter(&smr.batcher, )
    return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx as(unused)) {
    // TODO: tm_end(shared_t, tx_t)

    //Should call batcher.leave()
    return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) {
    // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) {
    // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared as(unused), tx_t tx as(unused), void* target as(unused)) {
    // TODO: tm_free(shared_t, tx_t, void*)
    return false;
}
