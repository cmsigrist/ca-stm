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

//#include <tm.h>
#include "../include/tm.h"
#include "lock.h"

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

// -------------------------------------------------------------------------- //
// -------------------------------LOCKS-------------------------------------- //
// -------------------------------------------------------------------------- //

static struct lock_t lock;

// -------------------------------------------------------------------------- //
// ------------------------------BATCHER------------------------------------- //
// -------------------------------------------------------------------------- //

batcher_t init_batcher() {
    batcher_t batcher;
    batcher.epoch =  0;
    batcher.to_commit_counter = 0;
    batcher.time_to_commit = 0;
    batcher.remaining = 0;
    batcher.blocked = calloc(N, sizeof(tx_t*));
    batcher.size = 0;
    return batcher;
}

/**
 *
 * @param batcher
 * @return Returns an integer that is unique to each batch of threads
 */
int get_epoch(batcher_t batcher) {
    //return batcher.epoch;
    return atomic_load(&batcher.epoch);
}

int get_commit_counter(batcher_t batcher) {
    return atomic_load(&batcher.to_commit_counter);
}

int enter(transaction_t * transaction, batcher_t* batcher) {
    printf("Entering batcher tx : %lu\n", (uintptr_t) transaction);
    int zero = 0;
    if (atomic_compare_exchange_strong(&batcher->remaining, &zero, 1) != true) {
        size_t size = atomic_load(&batcher->size);
        /*if(size % N == 0) {
            batcher->blocked = realloc(batcher->blocked, batcher->size + N * sizeof(transaction_t*));
        }*/
        if(batcher->blocked != NULL) {
            (batcher->blocked)[size] = (tx_t*)transaction;
            transaction->is_blocked = BLOCKED;
            atomic_fetch_add(&batcher->size, 1);
            while(transaction->is_blocked); // sleep with cond var instead
        }
    }
    // TODO need to commit ++
    atomic_fetch_add(&batcher->to_commit_counter, 1);
    //printf("%p entered\n", transaction);
    return 0;
}

int time_to_commit(batcher_t* batcher) {
    while (atomic_load(&batcher->to_commit_counter) != 0) {} // really ugly !
    printf("Time to commit ended\n");
    atomic_fetch_sub(&batcher->time_to_commit, 1);
    return 0;
}

int wake_up(batcher_t* batcher) {
    printf("Deblocking threads\n");
    atomic_fetch_add(&batcher->epoch, 1);
    size_t size = batcher->size;
    // Wake up everyone
    for(size_t i = 0; i < size; i++) {
        //printf("Deblocking blocked tx\n");
        ((transaction_t*)batcher->blocked[i])->is_blocked = NOT_BLOCKED;
    }
    memset(batcher->blocked, 0, size);
    atomic_store(&batcher->size, 0);
    memset(batcher->blocked, 0, size);
    atomic_store(&batcher->size, 0);
    return 0;
}

int leave(batcher_t* batcher) {
    int zero = 0;
    //printf("Leaving batcher\n");
    atomic_fetch_sub(&batcher->remaining, 1);
    printf("remaining thread in batcher = %d\n", batcher->remaining);
    size_t size = atomic_load(&batcher->size);
    if (atomic_compare_exchange_strong(&batcher->remaining, &zero, size) == true) { // last thread left -> new epoch
        // TODO time for everyone to commit; wake up all sleeping tx waiting to commit
        // enough cond var to wake up all the tx waiting to commit; then block
        //printf("TIME TO COMMIT\n");
        atomic_fetch_add(&batcher->time_to_commit, 1); // only one tx can increment this;
        return START_NEW_EPOCH;
    }
    return WAIT;
}


// -------------------------------------------------------------------------- //
// ------------------------------SEGMENT_HELPER------------------------------ //
// -------------------------------------------------------------------------- //
int create_segment(segment_t** segments, size_t index, size_t size, size_t align) {
    //printf("create segment in %p\n", region->segments[size]);
    segments[index] = calloc(1, sizeof(segment_t));
    if(segments[index] == NULL) {
        return 1;
    }
    segments[index]->size = size; // in bytes
    size_t num_words = size / align;
    //printf("Allocating %d words\n", num_words);
    segments[index]->readable_copy = calloc(num_words, sizeof(word_t));
    segments[index]->writable_copy = calloc(num_words, sizeof(word_t));

    segments[index]->control = calloc(num_words, sizeof(word_control_t));
    for(size_t i = 0; i < num_words; i++) {
        segments[index]->control[i].valid = READABLE_COPY;
        segments[index]->control[i].access_set[0] = NOT_MODIFIED;
        segments[index]->control[i].access_set[1] = NOT_MODIFIED;
        segments[index]->control[i].written = NOT_WRITTEN;
    }
    segments[index]->deregister = 0;
    //printf("created segment in %p\n", region->segments[size]);
    return 0;
}

void free_segment(segment_t* segment) {
    free(segment->readable_copy);
    free(segment->writable_copy);
    free(segment->control);
    free(segment);
}

// -------------------------------------------------------------------------- //
// ---------------------------TRANSACTIONS HELPER---------------------------- //
// -------------------------------------------------------------------------- //

bool in_copy(void* target, void* copy, size_t size) {
    if (target >= copy && target < copy + size) { // TODO check if this works
        return true;
    }
    return false;
}

segment_t* find_target_segment(segment_t** segments,
                        void* target,
                        size_t size)
{
    size_t i = 0;
    while(i < size) {
        word_t* readable = segments[i]->readable_copy;
        word_t* writable = segments[i]->writable_copy;
        size_t segment_size = segments[i]->size;

        if(in_copy(target, readable, segment_size)){
            return segments[i];
        }
        if(in_copy(target, writable, segment_size)) {
            return segments[i];
        }
        i++;
    }
    /*size_t offset = (uintptr_t)target >= (uintptr_t)target_seg->writable_copy ?
                    (int)((uintptr_t)target - (uintptr_t)target_seg->writable_copy) / 4 :
                    (int)((uintptr_t)target - (uintptr_t)target_seg->readable_copy) / 4;
    word_t* */

    return NULL;
}

/*size_t find_segment_size(segment_t segment) {
    printf("Computing seg size\n");
    size_t size = 0;
    void* current = segment.readable_copy;
    while(current != NULL) {
        size += sizeof(void*);
        current++;
    }
    return size;
}*/


// TODO put a lock on read
bool read_word(size_t index,
               size_t offset,
               transaction_t* tx,
               segment_t* source_seg,
               void* target_seg,
               int current_epoch)
{
    /*printf("-----------------\n");
    printf("index + offset = %zu\n", index + offset); */
    word_control_t control = source_seg->control[index + offset];
    /*printf("current epoch = %d, ", current_epoch);
    printf("control written = %d, ", control.written);
    printf("control access 0 = %lu, ", control.access_set[0]);
    printf("control access 1 = %lu\n", control.access_set[1]);*/

    word_t* readable_copy = control.valid == READABLE_COPY ? source_seg->readable_copy : source_seg->writable_copy;
    word_t* writable_copy = control.valid == READABLE_COPY ? source_seg->writable_copy : source_seg->readable_copy;
    if(tx->is_ro) {
        /*printf("Reading readable copy = %p\n", &readable_copy[index + offset]);
        printf("Content : %d in %p\n", readable_copy[index + offset], (word_t*)target_seg + index);*/
        memcpy((word_t*)target_seg + index, &readable_copy[index + offset], sizeof(word_t));
        return true;
    } else {
        if(control.written == current_epoch) {
            if(control.access_set[WRITER] == (uintptr_t)&(*tx)) {
                /*printf("Current epoch Reading writable copy = %p\n", &writable_copy[index + offset]);
                printf("Content : %d in %p\n", writable_copy[index + offset], (word_t*)target_seg + index);*/
                memcpy((word_t*)target_seg + index, &writable_copy[index + offset], sizeof(word_t));
                return true;
            } else {
                return false;
            }
        } else {
            /*printf("Reading readable copy = %p\n", &readable_copy[index + offset]);
            printf("Content : %d in %p\n", readable_copy[index + offset], (word_t*)target_seg + index);*/
            memcpy((word_t*)target_seg + index, &readable_copy[index + offset], sizeof(word_t));
            // first to read
            if(control.access_set[READERS] == NOT_MODIFIED) {
                source_seg->control[index + offset].access_set[READERS] = (uintptr_t)&(*tx);
            } // different from first reader
            else if(control.access_set[READERS] != (uintptr_t)&(*tx)) {
                source_seg->control[index + offset].access_set[READERS] = MULTIPLE_READERS;
            }
            return true;
        }
    }

}

// TODO put a lock on word
bool write_word(size_t index, size_t offset, transaction_t* tx, segment_t* target_seg, const void* source, int current_epoch) {
    /*printf("-----------------\n");
    printf("index + offset = %zu\n", index + offset);*/
    word_control_t control = target_seg->control[index + offset];
    word_t* writable_copy = control.valid == READABLE_COPY ? target_seg->writable_copy : target_seg->readable_copy;
    /*printf("current epoch = %d, ", current_epoch);
    printf("control written = %d, ", control.written);
    printf("control access 0 = %lu, ", control.access_set[0]);
    printf("control access 1 = %lu\n", control.access_set[1]);*/
    if(control.written == current_epoch) {
        if(control.access_set[WRITER] == (uintptr_t)&(*tx)) {
            /*printf("Writing word to %p\n", &writable_copy[index + offset]);
            printf("from : %p\t", (word_t*)(source) + index);
            printf("content = %d\n", *((word_t*)(source) + index));*/
            memcpy(&writable_copy[index + offset], (word_t*)(source) + index, sizeof(word_t));
            return true;
        } else {
            return false;
        }
    } else {
        if(control.access_set[READERS] == MULTIPLE_READERS /*&& control.access_set[READERS] != (uintptr_t)&(*tx)*/) {
            return false;
        } else {
           /* printf("Writing word to %p\n", &writable_copy[index+ offset]);
            printf("from : %p\t", (word_t*)(source) + index);
            printf("content = %d\n", *((word_t*)(source) + index));*/
            memcpy(&writable_copy[index + offset], (word_t*)(source) + index, sizeof(word_t));
            target_seg->control[index + offset].access_set[WRITER] = (uintptr_t)&(*tx);
            target_seg->control[index + offset].written = current_epoch;
            //printf("TM account[1] = %d or %d\n", target_seg->readable_copy[1], target_seg->writable_copy[1]);

            return true;
        }
    }
}

// RESET word control

int reset_word_control(word_control_t* control) {
    control->access_set[READERS] = NOT_MODIFIED;
    control->access_set[WRITER] = NOT_MODIFIED;
    return 0;
}

int commit(region_t* region, transaction_t* transaction) {
    int current_epoch = get_epoch(region->batcher);
    for(size_t i = 0; i < transaction->size; i++) {
        segment_t* segment = transaction->allocated[i];
        size_t size = segment->size / region->align;
        //printf("current epoch = %d\n", current_epoch);
        for(size_t j = 0; j < size ; j++) {
            if (segment->control[j].written == current_epoch) {
                // grab a lock on the batcher
                // defer the swap, of which copy for word index is the “valid" copy
                segment->control[j].valid = 1 - segment->control[j].valid; // swap valid copy
            }
            //printf("committed word[%d] : %p, valid copy is = %d\n", j, segment, segment->control[j].valid);
            reset_word_control(&segment->control[j]);
        }
        if(transaction->allocated[i]->deregister == 1) {
            //printf("freeing segment = %p\n", transaction->allocated[i]);
            free_segment(transaction->allocated[i]);
        }
    }

//region->segments[region_size] = transaction->allocated[transaction->size];
    return COMMITTED;
}

// -------------------------------------------------------------------------- //
// ------------------------------TRANSACTIONS-------------------------------- //
// -------------------------------------------------------------------------- //

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
    region->segments = calloc(N, sizeof(segment_t*)); // allocate list of N segments pointers
    create_segment(region->segments, INIT_SEGMENT, size, align); // first segment
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
    for(size_t i = 0; i < region->size; i++) {
        free_segment(region->segments[i]);
    }
    free(region);
    //printf("Destroyed region %p", shared);
    return;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    //printf("Start %p\n", shared);
    return &((region_t*)shared)->segments;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    //printf("Size region %p\n", shared);
    region_t* region = (region_t*)shared;
    return region->segments[0]->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    //printf("Align region %p\n", shared);
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
        transaction->allocated = calloc(N, sizeof(segment_t*));
        transaction->size = 0;
        transaction->region = shared;
        //transaction->epoch = 0;//region->batcher.counter;
        //printf("Created tx : %p\n", (uintptr_t)transaction);

        //enter transaction
        enter(transaction, &region->batcher);
        tx_t tx = (uintptr_t)transaction;
        return tx;
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
    // mark all allocated segments for deregistering
    printf("Leaving region\n");
    if(leave(&region->batcher)) { // last tx to leave the region
        commit(region, transaction);
        //printf("Done committing\n");
        atomic_fetch_sub(&region->batcher.to_commit_counter, 1);
        time_to_commit(&region->batcher);
        wake_up(&region->batcher);

    } else {
        //printf("Waiting to commit\n");
        while(atomic_load(&region->batcher.time_to_commit) == 0) {}
        commit(region, transaction);
        //printf("Done committing\n");
        atomic_fetch_sub(&region->batcher.to_commit_counter, 1);
    }
    return true;
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
    printf("------------------------------------\n");
    printf("Read tx: %ld\n", tx);
    transaction_t* transaction = (transaction_t*)tx;
    region_t* region = (region_t*)shared;
    batcher_t batcher = region->batcher;
    size_t align = region->align;

    /*if(size % align != 0) {
        return false;
    }*/

    /*if(&source[0] % align != 0 || &target[0] % align != 0) {
        return false;
    }*/
    size_t num_words = size / align;
    segment_t* source_seg = find_target_segment(region->segments, source, region->size);

    //printf("Reading in segment = %p\n", source_seg);
    if(source_seg != NULL) {
        // TODO find offset
        size_t offset = (uintptr_t)source >= (uintptr_t)source_seg->writable_copy ?
                                            (int)((uintptr_t)source - (uintptr_t)source_seg->writable_copy) / 4 :
                                            (int)((uintptr_t)source - (uintptr_t)source_seg->readable_copy) / 4;
        printf("Offset = %zu\n", offset);
        //segment_t target_seg;
        //target_seg.readable_copy = target;
        //target_seg.writable_copy = NULL;
        //target_seg.control = NULL;
        //target_seg.size = find_segment_size(target_seg);

        /*if (source_seg->size < size || target_seg.size < size) {
            return false;
        }*/
        int current_epoch = get_epoch(batcher);
        for (size_t i = 0; i < num_words; i++) {
            if (read_word(i, offset, transaction, source_seg, target, current_epoch) == false) {
                return false;
            }
        }

        // TODO add yourself to acces set on read
        printf("------------------------------------\n");
        return true;
    }
    //printf("------------------------------------\n");
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
// TODO there's an offset ! Check if / 4 works for any align
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    printf("------------------------------------\n");
    printf("Write tx: %ld,  target : %p\t", tx, target);
    transaction_t* transaction = (transaction_t*)tx;
    region_t* region = (region_t*)shared;
    batcher_t batcher = region->batcher;
    size_t align = region->align;

    /*if(size % align != 0) {
        return false;
    }*/
    /*if(source % align != 0 || target % align != 0) {
        return false;
    }*/
    size_t num_words = size / align;
    segment_t* target_seg = find_target_segment(region->segments, target, region->size);

    if(target_seg != NULL) {
        printf("Writing in segment : %p\n", target_seg);
        //TODO find offset
        size_t offset = (uintptr_t)target >= (uintptr_t)target_seg->writable_copy ?
                        (int)((uintptr_t)target - (uintptr_t)target_seg->writable_copy) / 4 :
                        (int)((uintptr_t)target - (uintptr_t)target_seg->readable_copy) / 4;
        /*printf("target = %p\t seg-> = %p\t, seg->w = %p\n", target, target_seg->readable_copy, target_seg->writable_copy);
        printf("target - seg->r = %d\n", (int)((uintptr_t)target - (uintptr_t)target_seg->readable_copy));
        printf("target - seg->w = %d\n", (int)((uintptr_t)target - (uintptr_t)target_seg->writable_copy));*/
        printf("Offset = %zu\n", offset);

        //segment_t source_seg;
        //source_seg.readable_copy = source;
        //source_seg.writable_copy = NULL;
        //source_seg.control = NULL;
        //source_seg.size = size;
        //source_seg.size = find_segment_size(source_seg);

        //printf("Writing found size: %zu\n", source_seg.size);
        /*if (source_seg.size < size || target_seg->size < size) {
            return false;
        }*/
        //printf("Starting write tx: %u, in epoch %d\n", transaction, current_epoch); */
        int current_epoch = get_epoch(batcher);
        for(size_t i = 0; i < num_words; i++) {
            if(write_word(i, offset, transaction, target_seg, source, current_epoch) == false) {
                //printf("Abort tx: %p\n", transaction);
                return false;
            }
        }
        // TODO add only if not already in
        int add = 1;
        size_t i = 0;
        while(i < transaction->size && add == 1) {
            if(transaction->allocated[i] == target_seg) {
                add = 0;
            }
            i += 1;
        }

        if(add) {
            transaction->allocated[transaction->size] = target_seg;
            transaction->size += 1;
        }

        //printf("Finished write tx: %ld\n", tx);
        printf("------------------------------------\n");
        return true;
    }
    //printf("------------------------------------\n");
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
    printf("------------------------------------\n");
    printf("Alloc tx %ld\n", tx);
    transaction_t* transaction = (transaction_t*) tx;
    region_t* region = (region_t*)shared;
    size_t region_size = region->size;
    size_t align = region->align;
    /*if(size % align != 0) {
        return abort_alloc;
    }*/
    // the size of the region is a multiple of the allocated number of segments then need to allocate N more segments
    /*if(region_size % N == 0) {
        if(realloc(region->segments, region_size + sizeof(segment_t) * N) == NULL) {
            return nomem_alloc;
        }
    }*/

    //printf("tx_seg[size] = %p\n", transaction->allocated[transaction->size]);
    if(create_segment(region->segments, region->size, size, align) != 0) {
        return nomem_alloc;
    }

    transaction->allocated[transaction->size] = region->segments[region_size];

    printf("transaction->allocated[transaction->size] = %p\n", transaction->allocated[transaction->size]);
    printf("transaction->allocated[transaction->size]->r = %p\n", transaction->allocated[transaction->size]->readable_copy);
    printf("transaction->allocated[transaction->size]->w = %p\n", transaction->allocated[transaction->size]->writable_copy);
    *target = region->segments[region_size]->readable_copy;

    transaction->size += 1;
    region->size += 1;
    printf("------------------------------------\n");
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    printf("------------------------------------\n");
    printf("Free tx %ld\n", tx);
    transaction_t* transaction = (transaction_t*) tx;
    region_t* region = (region_t*)shared;

    // mark target for deregistering

    segment_t* segment = find_target_segment(transaction->allocated, target, transaction->size);

    //printf("segment found = %p\n", segment);
    if(segment == region->segments[0]) {
        return false;
    }
    size_t size = segment->size / region->align;
    for(size_t i = 0; i < size; i++) {
        if(segment->control->access_set[WRITER] == tx) {
            segment->deregister = 1;
        }
    }

    printf("------------------------------------\n");
    return true;
}
