/**
 * @file   tm.c
 * @author Capucine Berger-Sigrist
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
    batcher.counter = 0;
    batcher.remaining = 0;
    batcher.blocked = calloc(N, sizeof(transaction_t*));
    batcher.size = 0;
    return batcher;
}

int get_epoch(batcher_t* batcher) {
    return batcher->counter;
}

int enter(transaction_t* transaction, batcher_t* batcher) {
    if (batcher->remaining == 0) {
        batcher->remaining = 1;
    } else {
        if(batcher->size == N) {
            batcher->blocked = realloc(batcher->blocked, N * sizeof(transaction_t*));
        }
        if(batcher->blocked != NULL) {
            (batcher->blocked)[batcher->size] = transaction;
            transaction->is_blocked = BLOCKED;
        }
        batcher->size += 1;
        while(transaction->is_blocked);
    }
    return 0;
}

int leave(batcher_t* batcher) {
    batcher->remaining -= 1;
    if (batcher->remaining == 0) {
        batcher->counter += 1;
        batcher->remaining = batcher->size;
        // Wake up everyone
        for(size_t i = 0; i < batcher->size; i++) {
            batcher->blocked[i]->is_blocked = NOT_BLOCKED;
        }
        free(batcher->blocked); // empty the list
        batcher->blocked = calloc(N, sizeof(transaction_t*)); // reallocate a block of memory
        batcher->size = 0;
    }
    return 0;
}
// ------------------------------SEGMENT_HELPER----------------------------- //
segment_t* create_segment(size_t size, size_t align) {
    segment_t* segment = calloc(1, sizeof(segment_t));
    if(segment == NULL) {
        return NULL;
    }
    segment->size = size; // in bytes
    int num_words = size/align;
    segment->readable_copy = calloc(num_words, sizeof(void*)); // words of 8 bytes
    segment->writable_copy = calloc(num_words, sizeof(void*));


    segment->control = calloc(num_words, sizeof(word_control_t));
    for(int i = 0; i < num_words; i++) {
        segment->control[i].valid = READABLE_COPY;
        segment->control[i].access_set = NOT_MODIFIED;
        segment->control[i].written = NOT_WRITTEN;
    }

    return segment;
}

void free_segment(segment_t* segment) {
    free(segment->readable_copy);
    free(segment->writable_copy);
    free(segment->control);
    free(segment);
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
    region->segments = calloc(N, sizeof(segment_t)); // allocate list of N segments
    region->segments = create_segment(size, align); // first segment
    region->size = 1;
    region->align = align;
    region->batcher = init_batcher();

    shared_t shared = region;
    return shared;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    region_t* region = ((region_t*)shared);
    segment_t* current = region->segments;
    while(current != NULL) {
        free_segment(current);
        current++;
    }
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    return &((region_t*)shared)->segments;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    region_t* region = (region_t*)shared;
    return region->segments[0].size;
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
tx_t tm_begin(shared_t shared, bool is_ro) {
    region_t* region = (region_t*)shared;
    transaction_t* transaction = calloc(1, sizeof(transaction_t));
    if(transaction != NULL) {
        transaction->is_ro = is_ro;
        transaction->is_blocked = NOT_BLOCKED;
        //enter transaction
        if (enter(transaction, &region->batcher) == 0) { // SUCCESS
            tx_t tx = (uintptr_t)transaction;
            return tx;
        }
        // TODO find out when it should return invalid_tx
        // Not sure how this could happen
    }
    return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) {
    transaction_t* transaction = (transaction_t*)tx;
    region_t* region = (region_t*)shared;
    leave(&region->batcher);
    //TODO
    return false;
}

bool read_word(size_t index, transaction_t* transaction, segment_t* source_seg, segment_t target_seg) {
    word_control_t control = source_seg->control[index];
    if(transaction->is_ro) {
        memcpy(&target_seg.readable_copy[index], &source_seg->readable_copy[index], sizeof(void*));
        return true;
    } else {
        if(control.written == WRITTEN) {
            // TODO: Might not work an other thread who hasn't worked on it would still see MODIFIED_BY_CURRENT_TX...
            if(control.access_set == MODIFIED_BY_CURRENT_TX) {
                memcpy(&target_seg.readable_copy[index], &source_seg->writable_copy[index], sizeof(void*));
                return true;
            } else {
                return false;
            }
        } else {
            memcpy(&target_seg.readable_copy[index], &source_seg->readable_copy[index], sizeof(void*));
            source_seg->control[index].access_set = MODIFIED_BY_CURRENT_TX; // Might not work
            return true;
        }
    }
    return false;
}

// Returns the segment in which the address of the first word is source
segment_t* find_segment(region_t* region, void const* source) {
    size_t size = region->size;
    int i = 0;
    while(i < size) {
        void* word = &(region->segments[i].readable_copy[0]);
        if(word == source) {
            return &region->segments[i];
        }
        i++;
    }
    return NULL;
}

size_t find_seg_size(segment_t segment) {
    size_t size = 0;
    void* current = segment.readable_copy;
    while(current != NULL) {
        size += sizeof(void*);
        current++;
    }
    return size;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    // TODO target and source point to the first word of a segment
    transaction_t* transaction = (transaction_t*)tx;
    region_t* region = (region_t*)shared;
    batcher_t batcher = region->batcher;
    size_t align = region->align;

    if(size % align != 0) {
        return false;
    }
    print("Source = %p", &source[0]);
    print("Target = %p", &target[0]);

    /*if(&source[0] % align != 0 || &target[0] % align != 0) {
        return false;
    }*/
    size_t num_words = size / align;
    segment_t* source_seg = find_segment(region, source);

    if(source_seg != NULL) {
        segment_t target_seg;
        target_seg.readable_copy = target;
        target_seg.writable_copy = NULL;
        target_seg.control = NULL;
        target_seg.size = find_seg_size(target_seg);

        if (source_seg->size < size || target_seg.size < size) {
            return false;
        }
        for (int i = 0; i < num_words; i++) {
            if (read_word(i, transaction, source_seg, target_seg) == false) {
                return false;
            }
        }
        return true;
    }
    return false;
}

// TODO Write the content of source into target
bool write_word(size_t index, segment_t* target_seg, segment_t source_seg) {
    word_control_t control = target_seg->control[index];
    if(control.written == WRITTEN) {
        if(control.access_set == MODIFIED_BY_CURRENT_TX) {
            memcpy(&target_seg->writable_copy[index], &source_seg.readable_copy[index], sizeof(void*));// write the content at source into writable copy
            return true;
        } else {
            return false;
        }
    } else {
        if(control.access_set == MODIFIED) {
            return false;
        } else {
            memcpy(&target_seg->writable_copy[index], &source_seg.readable_copy[index], sizeof(void*));
            target_seg->control[index].access_set = MODIFIED_BY_CURRENT_TX;
            target_seg->control[index].written = WRITTEN;
            return true;
        }
    }

}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    transaction_t* transaction = (transaction_t*)tx;
    region_t* region = (region_t*)shared;
    batcher_t batcher = region->batcher;
    size_t align = region->align;

    if(size % align != 0) {
        return false;
    }
    /*if(source % align != 0 || target % align != 0) {
        return false;
    }*/
    size_t num_words = size / align;
    segment_t* target_seg = find_segment(region, target);
    if(target_seg != NULL) {
        segment_t source_seg;
        source_seg.readable_copy = source;
        source_seg.writable_copy = NULL;
        source_seg.control = NULL;
        source_seg.size = find_seg_size(source_seg);

        if (source_seg.size < size || target_seg->size < size) {
            return false;
        }
        for(int i = 0; i < num_words; i++) {
            if(write_word(i, target_seg, source_seg) == false) {
                return false;
            }
        }
        return true;
    }
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
    transaction_t* transaction = (transaction_t*) tx;
    region_t* region = (region_t*)shared;
    size_t region_size = region->size;
    size_t align = region->align;
    if(size % align != 0) {
        return abort_alloc;
    }
    // the size of the region is a multiple of the allocated number of segments then need to allocate N segments
    if(region_size % N == 0) {
        if(realloc(region->segments, sizeof(segment_t) * (region_size + N)) == NULL) {
            return nomem_alloc;
        }
    }
    segment_t* new_segment = create_segment(size, align);
    if(new_segment == NULL) {
        return nomem_alloc;
    }
    region->segments[region_size] = *new_segment;
    region->size += 1;
    // TODO register the segment in the set of allocated segments ???? What is this set ?
    *target = new_segment->readable_copy;

    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    transaction_t* transaction = (transaction_t*) tx;
    region_t* region = (region_t*)shared;

    segment_t* segment = find_segment(region, target);
    // TODO
    if(segment == &region->segments[0]) {
        free_segment(segment);
        return true;
    }

    return false;
}

int commit() {
    // TODO
    return 0;
}
