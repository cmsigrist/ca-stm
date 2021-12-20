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
#include <stdlib.h> // for calloc() and realloc()
#include <string.h> // for memcpy()
#include <stdatomic.h>
#include <pthread.h>
#include <stdio.h>
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

// -------------------------------------------------------------------------- //
// ---------------------------------STRUCTURE-------------------------------- //
// -------------------------------------------------------------------------- //
// Valid copy
#define READABLE_COPY 0
#define WRITABLE_COPY 1
// Access set
#define WRITER 0
#define READERS 1
#define NOT_MODIFIED 0
#define MULTIPLE_READERS 2
// Write or read
#define NOT_ACCESSED -1 //epoch

#define INIT_SEGMENT 0
#define MAX_REGION_SIZE 0x10000 // 2^16
#define EMPTY -1

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cv;
} lock_t;

typedef struct {
    atomic_int epoch; // current epoch
    atomic_int remaining; // #tx remaining in the batcher in current epoch
    atomic_int commit_counter; // #tx that need to commit in current epoch
    atomic_size_t size; // count the number of blocked tx
    atomic_size_t block_cv; // #tx blocked in current epoch
    lock_t lock_commit;
    lock_t lock_block;
    lock_t lock; // global lock on the batcher
    tx_t leader; // leader of the current epoch starts the new epoch
} batcher_t;

typedef char word_t; // 1 byte

typedef struct {
    int valid; // readable or writable copy is valid
    tx_t access_set[2]; // access_set[0] = writer, access_set[1] = #readers
    int written; // epoch of last write
    int read; // epoch of last read
} word_control_t;

typedef struct {
    size_t size; // in bytes
    word_t* readable_copy; // readable copy
    word_t* writable_copy; // writeable copy
    word_control_t* control; // control structure for each word
    lock_t* locks; // lock for each word
    tx_t deregister; // to be freed at the end of the epoch
    tx_t allocated; // uid of the tx that allocated the segment
} segment_t;

typedef struct{
    int* indices;
    int top;
} stack_t;

typedef struct {
    segment_t** segments; // list of segments_t*
    atomic_size_t size; // number of segments allocated in the region
    stack_t next_free; // index of next free segment
    size_t align; // region align
    batcher_t batcher; // batcher associated to the region
    lock_t global_lock; // global lock on the region for allocation
    lock_t* segment_locks; // a lock for each of the 2^48 allocated segments ....
} region_t;

typedef struct {
    bool is_ro; // read only tx
    int* allocated; // list of modified segment index
    size_t size; // number of indices
    region_t* region; // region associated to the tx
    int epoch; // epoch in which the tx entered the batcher
    bool commit; // true : can commit, false : tx aborted
} transaction_t;

// -------------------------------------------------------------------------- //
// -----------------------------------LOCKS---------------------------------- //
// -------------------------------------------------------------------------- //
bool lock_init(lock_t* lock) {
    return pthread_mutex_init(&(lock->mutex), NULL) == 0
           && pthread_cond_init(&(lock->cv), NULL) == 0;
}

void lock_cleanup(lock_t* lock) {
    pthread_mutex_destroy(&(lock->mutex));
    pthread_cond_destroy(&(lock->cv));
}

bool lock_acquire(lock_t* lock) {
    return pthread_mutex_lock(&(lock->mutex)) == 0;
}

void lock_release(lock_t* lock) {
    pthread_mutex_unlock(&(lock->mutex));
}

void lock_wait(lock_t* lock) {
    pthread_cond_wait(&(lock->cv), &(lock->mutex));
}

void lock_wake_up(lock_t* lock) {
    pthread_cond_broadcast(&(lock->cv));
}

int wait_commit(lock_t* lock, atomic_int* remaining, int size) {
    lock_acquire(lock);
    while(true) {
        if(*remaining == size) break;
        lock_wait(lock);
    }
    lock_release(lock);
    return 0;
}

int wait_remaining(lock_t* lock, atomic_int* remaining, int size) {
    lock_acquire(lock);
    while(true) {
        if(*remaining == size) break;
        lock_wait(lock);
    }
    lock_release(lock);
    return 0;
}
// consume block
int wait_for_wake_up(lock_t* lock, atomic_size_t* block) {
    lock_acquire(lock);
    while(true) {
        if(*block > 0) break;
        lock_wait(lock);
    }
    atomic_fetch_sub(block, 1);
    lock_release(lock);
    lock_wake_up(lock);
    return 0;
}

int produce(lock_t* lock, atomic_size_t* to_produce, int size) {
    lock_acquire(lock);
    atomic_fetch_add(to_produce, size);
    lock_release(lock);
    lock_wake_up(lock);
    return 0;
}

int produce_commit(lock_t* lock, atomic_size_t* to_produce, int size) {
    lock_acquire(lock);
    atomic_fetch_add(to_produce, size);
    lock_release(lock);
    lock_wake_up(lock);
    return 0;
}

bool elect(tx_t tx, tx_t* leader) {
    tx_t expected = 0;
    return atomic_compare_exchange_strong(leader, &expected, tx);
}

// -------------------------------------------------------------------------- //
// -----------------------------------STACK---------------------------------- //
// -------------------------------------------------------------------------- //
bool is_empty(stack_t* stack) {
    return stack->top == EMPTY;
}

bool is_full(stack_t* stack) {
    return stack->top == MAX_REGION_SIZE/2 - 1;
}

int pop(stack_t* stack) {
    int index = EMPTY;
    if(!is_empty(stack)) {
        index = stack->indices[stack->top];
        stack->top -= 1;
    }
    return index;
}

int push(stack_t* stack, int index) {
    if(is_full(stack)) {
        if(realloc(stack->indices, (2 * stack->top) * sizeof(int*)) == NULL) {
            return 1;
        }
    }
    stack->top += 1;
    stack->indices[stack->top] = index;
    return 0;
}

// -------------------------------------------------------------------------- //
// ----------------------------------BATCHER--------------------------------- //
// -------------------------------------------------------------------------- //
/**
 * Initialise the batcher
 * @return the batcher
 */
batcher_t init_batcher() {
    batcher_t batcher;
    batcher.epoch =  0;
    batcher.remaining = 0;
    batcher.commit_counter = 0;
    batcher.size = 0;
    batcher.block_cv= 0;
    batcher.leader = 0;
    lock_init(&batcher.lock);
    lock_init(&batcher.lock_commit);
    lock_init(&batcher.lock_block);
    return batcher;
}

/**
 * Clean the batcher
 * @param batcher the batcher
 */
void clean_batcher(batcher_t* batcher) {
    lock_cleanup(&batcher->lock);
    lock_cleanup(&batcher->lock_commit);
    lock_cleanup(&batcher->lock_block);
}

/**
 *
 * @param batcher the batcher
 * @return the epoch of the current batch
 */
int get_epoch(batcher_t* batcher) {
    return atomic_load(&batcher->epoch);
}

/**
 * Called by a transaction that wants to begin
 * @param batcher the batcher
 * @return
 */
int enter(batcher_t* batcher) {
    int expected = 0;
    lock_acquire(&batcher->lock);
    if (!atomic_compare_exchange_strong(&batcher->remaining, &expected, 1)) {
        atomic_fetch_add(&batcher->size, 1);
        lock_release(&batcher->lock);
        wait_for_wake_up(&batcher->lock_block, &batcher->block_cv);
    } else {
        atomic_fetch_add(&batcher->commit_counter, 1);
        lock_release(&batcher->lock);
    }
    return 0;
}

/**
 * Called by a transaction that is ending
 * @param batcher the batcher
 * @return 0 if there are still transaction running in the batcher, 1 if the last transaction left
 */
int leave(batcher_t* batcher) {
    int expected = 1;
    lock_acquire(&batcher->lock);
    if(atomic_compare_exchange_strong(&batcher->remaining, &expected, -1)) {
        lock_release(&batcher->lock);
        lock_wake_up(&batcher->lock_commit); // signal that last tx left
        return 1;
    } else {
        atomic_fetch_sub(&batcher->remaining, 1);
        lock_release(&batcher->lock);
        return 0;
    }
}

/**
 * The leader starts a new epoch : waits for everyone to commit, and unblocks the threads in the batcher
 * @param batcher the batcher
 * @return 0 on success
 */
int start_new_epoch(batcher_t* batcher) {
    wait_commit(&batcher->lock_commit, (atomic_int*)&batcher->commit_counter, 0);
    size_t size = atomic_load(&batcher->size);
    atomic_fetch_add(&batcher->epoch, 1);
    atomic_store(&batcher->remaining, size); // set remaining to size
    atomic_store(&batcher->commit_counter, size);
    atomic_store(&batcher->size, 0);
    atomic_store(&batcher->leader, 0);

    produce(&batcher->lock_block, &batcher->block_cv, size);
    return 0;
}

// -------------------------------------------------------------------------- //
// ------------------------------SEGMENT_HELPER------------------------------ //
// -------------------------------------------------------------------------- //
/**
 * Initialise the structure of a segment
 * @param segment the address of the segment to be initialised
 * @param index the index of the segment in the shared memory region
 * @param size the size in bytes of the segments
 * @param align region alignment
 * @param transaction Transaction allocating a new segment, NULL if called from tm_create()
 * @return 0 on success
 */
void create_segment(segment_t* segment, size_t size, size_t align, transaction_t* transaction) {

    segment->size = size; // in bytes
    size_t num_words = size / align;
    segment->readable_copy = (word_t *) ((uintptr_t) segment + sizeof(segment_t));
    segment->writable_copy = segment->readable_copy + size;
    memset(segment->readable_copy, 0, size);
    memset(segment->writable_copy, 0, size);
    segment->control = calloc(num_words, sizeof(word_control_t));
    segment->locks = calloc(num_words, sizeof(lock_t));

    if(transaction == NULL) {
        for (size_t i = 0; i < num_words; i++) {
            segment->control[i].valid = READABLE_COPY;
            segment->control[i].access_set[WRITER] = NOT_MODIFIED;
            segment->control[i].access_set[READERS] = NOT_MODIFIED;
            segment->control[i].written = NOT_ACCESSED;
            segment->control[i].read = NOT_ACCESSED;
            lock_init(&segment->locks[i]);
        }
        segment->allocated = 0;
    } else {
        for (size_t i = 0; i < num_words; i++) {
            segment->control[i].valid = READABLE_COPY;
            segment->control[i].access_set[WRITER] = (tx_t) transaction;
            segment->control[i].access_set[READERS] = NOT_MODIFIED;
            segment->control[i].written = transaction->epoch;
            segment->control[i].read = NOT_ACCESSED;
            lock_init(&segment->locks[i]);
        }
        segment->allocated = (tx_t) transaction;
    }

    segment->deregister = 0;
}

/**
 * Free a given segment
 * @param segment the segment to free
 * @param size the number of words in the segment
 */
void free_segment(segment_t* segment, size_t size) {
    free(segment->control);
    for (size_t i = 0; i < size; i++) {
        lock_cleanup(&segment->locks[i]);
    }
    free(segment->locks);
    free(segment);
}

// -------------------------------------------------------------------------- //
// ---------------------------TRANSACTIONS HELPER---------------------------- //
// -------------------------------------------------------------------------- //
/**
 * Find the index of a given segment in the shared memory region
 * @param segments list of segments allocated in the shared memory region
 * @param target address of a word in the segment to find
 * @param size size of the list of allocated segments of the shared memory region
 * @return the index of the segment or EMPTY if the target does not corresponds to any segment
 */
int find_target_segment(segment_t** segments, const void* target, size_t size)
{
    for(size_t i = 0; i < size; i++) {
        if(segments[i] != NULL) {
            word_t *readable = segments[i]->readable_copy;
            if (target >= (void *) readable
            && target < (void *) (readable + segments[i]->size)) {
                return i;
            }
        }
    }
    return EMPTY;
}

/**
 * Add the index of a segment from the shared memory region in the allocated list of
 * segments indices from the transaction
 * @param allocated List of indices corresponding to the segments allocated by the transaction
 *  in the shared memory region
 * @param index the index of the segment in the shared memory region
 * @param size Size of allocated
 */
void add_to_list(int* allocated, int index, size_t* size) {
    for(size_t i = 0; i < *size; i++) {
        if(allocated[i] == index) {
            return;
        }
    }
    if(*size != 0 && *size % MAX_REGION_SIZE == 0) {
        if(realloc(allocated, (*size + MAX_REGION_SIZE) * sizeof(int*)) == NULL) {
            return;
        }
    }
    allocated[*size] = index;
    *size += 1;
}

/**
 * Read align bytes from the source segment and copy the content in target segment
 * @param index index in the target where to copy the word
 * @param offset offset in the readable or writable copy of the source segment
 * @param align region alignment
 * @param transaction Transaction doing the read
 * @param source_seg Source segment containing the word to read
 * @param target_seg Target segment where the word is copied
 * @return true : success, false : the transaction must abort
 */
bool read_word(size_t index,
               size_t offset,
               size_t align,
               transaction_t* transaction,
               segment_t* source_seg,
               void* target_seg)
{
    word_control_t control = source_seg->control[index + offset];
    word_t* readable_copy = control.valid == READABLE_COPY ?
            source_seg->readable_copy : source_seg->writable_copy;
    if(transaction->is_ro) {
        memcpy(target_seg + index, &readable_copy[align * (index + offset)], align);
        return true;
    } else {
        lock_acquire(&source_seg->locks[index + offset]);
        if(control.written == transaction->epoch) {
            if(control.access_set[WRITER] == (tx_t) transaction) {
                word_t* writable_copy = control.valid == READABLE_COPY ?
                                        source_seg->writable_copy : source_seg->readable_copy;
                memcpy(target_seg + index, &writable_copy[align * (index + offset)], align);
                source_seg->control[index + offset].read = transaction->epoch;
                lock_release(&source_seg->locks[index + offset]);
                return true;
            } else {
                lock_release(&source_seg->locks[index + offset]);
                return false;
            }
        } else {
            memcpy(target_seg + index, &readable_copy[align * (index + offset)], align);

            if(control.access_set[READERS] != (tx_t) transaction && control.read == transaction->epoch) {
                source_seg->control[index + offset].access_set[READERS] = MULTIPLE_READERS;
            } else if(control.access_set[READERS] == NOT_MODIFIED || control.read != transaction->epoch) {
                source_seg->control[index + offset].access_set[READERS] = (tx_t) transaction;
            }
            source_seg->control[index + offset].read = transaction->epoch;
            lock_release(&source_seg->locks[index + offset]);
            return true;
        }
    }

}

/**
 * Write align bytes from source to target segment
 * @param index start of the word to write from source
 * @param offset offset in the target_segment
 * @param align region alignment
 * @param transaction Transaction doing the write
 * @param target_seg Target segment in which the word is written
 * @param source Source segment containing the word to be written
 * @return true : success, false : the transaction must abort
 */
bool write_word(size_t index,
                size_t offset,
                size_t align,
                transaction_t* transaction,
                segment_t* target_seg,
                const void* source) {
    lock_acquire(&target_seg->locks[index + offset]);
    word_control_t control = target_seg->control[index + offset];
    word_t* writable_copy = control.valid == READABLE_COPY ?
            target_seg->writable_copy : target_seg->readable_copy;
    if(control.written == transaction->epoch) {
        if(control.access_set[WRITER] == (tx_t) transaction) {
            memcpy(&writable_copy[align*(index + offset)], source + index, align);
            lock_release(&target_seg->locks[index + offset]);
            return true;
        } else {
            lock_release(&target_seg->locks[index + offset]);
            return false;
        }
    } else {
        if(control.access_set[READERS] == MULTIPLE_READERS && control.read == transaction->epoch) {
            lock_release(&target_seg->locks[index + offset]);
            return false;
        } else {
            memcpy(&writable_copy[align*(index + offset)], source + index, align);
            target_seg->control[index + offset].access_set[WRITER] = (tx_t) transaction;
            target_seg->control[index + offset].written = transaction->epoch;
            lock_release(&target_seg->locks[index + offset]);
            return true;
        }
    }
}

/**
 * Abort a transaction
 * @param shared Shared memory region
 * @param transaction Transaction to abort
 * @return false when transaction has ended
 */

bool tm_abort(shared_t shared, transaction_t* transaction) {
    transaction->commit = false;
    return tm_end(shared, (tx_t) transaction);
}

/**
 * Commit all the write/alloc/free from one tx
 * @param region Shared memory region
 * @param transaction Transaction to commit
 * @return 0 on success
 */
int tm_commit(region_t* region, transaction_t* transaction) {
    if(transaction->is_ro == false) {
        for (size_t i = 0; i < transaction->size; i++) {
            int index = transaction->allocated[i];
            segment_t *segment = region->segments[index];
            //lock_acquire(&region->segment_locks[index]);
            //if(segment != NULL) {
                size_t size = segment->size / region->align;
                if (segment->deregister == (tx_t) transaction) { // free a segment
                    lock_acquire(&region->segment_locks[index]);
                    /*printf("Tm commit %p : free segment[%d] epoch = %d\n",
                           transaction, index, transaction->epoch);*/
                    free_segment(segment, size);
                    region->segments[index] = NULL;
                    lock_release(&region->segment_locks[index]);

                    lock_acquire(&region->global_lock);
                    push(&region->next_free, index);
                    lock_release(&region->global_lock);
                } else { // commit
                    /*if(segment->deregister != 0 && ((transaction_t*) segment->deregister)->commit) { // a committing tx will free it
                        printf("Tm commit %p : skip will be freed segment[%d] by %p epoch = %d\n",
                               transaction, index, (transaction_t*)segment->deregister, transaction->epoch);
                        lock_release(&region->segment_locks[index]);
                        continue;
                    }
                    lock_release(&region->segment_locks[index]);*/
                    for (size_t j = 0; j < size; j++) {
                        lock_acquire(&segment->locks[j]);
                        if (segment->control[j].access_set[WRITER] == (tx_t) transaction
                        && segment->control[j].written == transaction->epoch) {
                            /*word_t *readable_copy = segment->control[j].valid == READABLE_COPY ?
                                                    segment->readable_copy : segment->writable_copy;
                            word_t *writable_copy = segment->control[j].valid == READABLE_COPY ?
                                                    segment->writable_copy : segment->readable_copy;
                            printf("Tm committing %p: segment[%d]-[%zu], control->[WRITER] = %p control->write = %d, epoch = %d\n",
                                   transaction, index, j,
                                   (transaction_t *)segment->control[j].access_set[WRITER],
                                   segment->control[j].written,
                                   transaction->epoch);*/
                            segment->control[j].valid = 1 - segment->control[j].valid; // swap valid copy
                            segment->control[j].access_set[WRITER] = NOT_MODIFIED;

                            /*readable_copy = segment->control[j].valid == READABLE_COPY ?
                                                    segment->readable_copy : segment->writable_copy;
                            writable_copy = segment->control[j].valid == READABLE_COPY ?
                                                    segment->writable_copy : segment->readable_copy;
                            printf("Tm commit %p: segment[%d]-[%zu], content = %ld (/ was = %ld), valid = %d epoch = %d\n",
                                   transaction, index, j,
                                   *(u_long * )(readable_copy + j * region->align),
                                   *(u_long * )(writable_copy + j * region->align),
                                   segment->control[j].valid, transaction->epoch);*/
                        }
                        segment->allocated = 0;
                        lock_release(&segment->locks[j]);

                    }
                }
            /*} else {
                lock_release(&region->segment_locks[index]);
            }*/
        }
    }
    return 0;
}

/**
 * On abort, tx rollback (e.g frees the segment it allocated)
 * @param region Shared memory region
 * @param transaction Transaction to rollback
 * @return 0 on success
 */
int tm_rollback(region_t* region, transaction_t* transaction) {
    for (size_t i = 0; i < transaction->size; i++) {
        int index = transaction->allocated[i];
        segment_t* segment = region->segments[index];
        lock_acquire(&region->segment_locks[index]);
        if(segment != NULL) {
            if (segment->deregister == (tx_t) transaction) { // was the only tx suppose to free the segment but aborted
                /*printf("Tm rollback %p: free segment[%d] epoch = %d\n",
                       transaction, index, transaction->epoch);*/
                segment->deregister = 0;
            }
            lock_release(&region->segment_locks[index]);
            if (segment->allocated == (tx_t) transaction) { // allocated segment must be freed
                /*printf("Tm rollback %p : alloc segment[%d] epoch = %d\n",
                       transaction, index, transaction->epoch);*/
                size_t size = segment->size / region->align;
                lock_acquire(&region->global_lock);
                push(&region->next_free, index);
                free_segment(segment, size);
                region->segments[index] = NULL;
                lock_release(&region->global_lock);
            }
        } else {
            lock_release(&region->segment_locks[index]);
        }
    }
    return 0;
}

/*
for (size_t i = 0; i < transaction->size; i++) {
        int index = transaction->allocated[i];
        segment_t* segment = region->segments[index];
        lock_acquire(&region->segment_locks[index]);
        if(segment != NULL) {
            if (segment->deregister == (tx_t) transaction) { // was the only tx suppose to free the segment but aborted
                printf("Tm rollback %p: free segment[%d] epoch = %d\n",
                       transaction, index, transaction->epoch);
                segment->deregister = 0;
            }
            lock_release(&region->segment_locks[index]);
            if (segment->allocated == (tx_t) transaction) { // allocated segment must be freed
                printf("Tm rollback %p : alloc segment[%d] epoch = %d\n",
                       transaction, index, transaction->epoch);
                size_t size = segment->size / region->align;
                lock_acquire(&region->global_lock);
                push(&region->next_free, index);
                free_segment(segment, size);
                region->segments[index] = NULL;
                lock_release(&region->global_lock);
            }
        } else {
            lock_release(&region->segment_locks[index]);
        }
    }
    return 0;
 */

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
    region->segments = calloc(MAX_REGION_SIZE, sizeof(segment_t*));

    if(posix_memalign((void**)&region->segments[INIT_SEGMENT],
                      align,
                      sizeof(segment_t) + 2 * size) != 0) {
        free(region);
        return invalid_shared;
    }
    create_segment(region->segments[INIT_SEGMENT], size, align, NULL);
    region->size = 1;
    region->next_free.indices = calloc(MAX_REGION_SIZE/2, sizeof(int*));
    region->next_free.top = EMPTY;
    region->segment_locks = calloc(MAX_REGION_SIZE, sizeof(lock_t));
    lock_init(&region->segment_locks[INIT_SEGMENT]);
    region->align = align;
    region->batcher = init_batcher();
    lock_init(&region->global_lock);
    shared_t shared = region;
    return shared;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    region_t* region = (region_t*) shared;
    size_t size = atomic_load(&region->size);
    for(size_t i = 0; i < size; i++) {
        if(region->segments[i] != NULL) {
            free_segment(region->segments[i], region->segments[i]->size / region->align);
            lock_cleanup(&region->segment_locks[i]);
        }
    }
    free(region->segments);
    free(region->next_free.indices);
    clean_batcher(&region->batcher);
    lock_cleanup(&region->global_lock);
    free(region);
    return;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    return ((region_t*) shared)->segments[INIT_SEGMENT]->readable_copy;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    return ((region_t*) shared)->segments[INIT_SEGMENT]->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    return ((region_t*) shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    region_t* region = (region_t*) shared;
    transaction_t* transaction = calloc(1, sizeof(transaction_t));
    if(transaction != NULL) {
        transaction->is_ro = is_ro;
        transaction->allocated = calloc(MAX_REGION_SIZE, sizeof(int*));
        transaction->size = 0;
        transaction->region = region;
        transaction->commit = true;
        enter(&region->batcher);
        transaction->epoch = get_epoch(&region->batcher);
        tx_t tx = (tx_t) transaction;
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
    transaction_t *transaction = (transaction_t *) tx;
    region_t *region = (region_t *) shared;
    batcher_t *batcher = &region->batcher;

    if(!leave(batcher)) { // if not last tx then wait
        wait_remaining(&batcher->lock_commit, &batcher->remaining, EMPTY);
    }
    //commit
    if(transaction->commit) {
        tm_commit(region, transaction);
    } else { // rollback e.g. free allocated segments or undo free
        tm_rollback(region, transaction);
    }
    produce_commit(&batcher->lock_commit, (atomic_size_t*)&batcher->commit_counter, -1);
    // one leader elected per epoch
    lock_acquire(&batcher->lock);
    if(transaction->epoch == get_epoch(batcher)) {
        if(elect(tx, &batcher->leader)) {
            start_new_epoch(batcher);
            /*size_t j = 0;
            u_long sum = 0;
            while(j < region->size) {
                if(region->segments[j] != NULL) {
                    word_t *readable_copy = region->segments[j]->control[0].valid == READABLE_COPY ?
                                            region->segments[j]->readable_copy : region->segments[j]->writable_copy;
                    u_long size = *((u_long*)readable_copy);
                    printf("Tm end %p : size = segment[%zu]-[0] = %ld epoch = %d\n", transaction, j,
                           size, transaction->epoch);
                    u_long i = 0;
                    while (i < size) {
                        readable_copy = region->segments[j]->control[i + 3].valid == READABLE_COPY ?
                                        region->segments[j]->readable_copy : region->segments[j]->writable_copy;
                        sum += *(u_long * )(readable_copy + (i + 3) * region->align);
                        if (*(u_long * )(readable_copy + (i + 3) * region->align) == 0) {
                            printf("Tm end %p : segment[%zu]-[%ld] = %ld epoch = %d\n", transaction, j, i + 3,
                                   *(u_long * )(readable_copy + (i + 3) * region->align), transaction->epoch);
                        }
                        i++;
                    }
                }
                j++;
            }
            //size_t sum = 0;
            printf("Tm end %p : remaining %d, sum = %ld, epoch = %d  \n", transaction,
                    batcher->remaining, sum, transaction->epoch);*/
        }
    }
    lock_release(&batcher->lock);

    int commit = transaction->commit;
    free(transaction->allocated);
    free(transaction);
    transaction = NULL;
    return commit;
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
    transaction_t *transaction = (transaction_t*) tx;
    region_t *region = (region_t*) shared;
    size_t align = region->align;

    if(transaction->region != region) {
        return tm_abort(shared, transaction);
    }
    size_t num_words = size / align;
    int index = find_target_segment(region->segments, source, atomic_load(&region->size));
    if(index != EMPTY) {
        segment_t* source_seg = region->segments[index];
        if (source_seg != NULL) {
        lock_acquire(&region->segment_locks[index]);
            if ((source_seg->allocated != tx && source_seg->allocated != 0)
            || source_seg->deregister != 0) {
                lock_release(&region->segment_locks[index]);
                // prevent read if segment was allocated by another tx or will be freed
                return tm_abort(shared, transaction);
            }
        lock_release(&region->segment_locks[index]);

        size_t offset = ((uintptr_t) source - (uintptr_t) source_seg->readable_copy) / align;
            for (size_t i = 0; i < num_words; i++) {
                /*printf("Tm read %p read word: "
                       "segment[%d]->read[%zu] = %p, allocated = %p, control.access[WRITER] = %p"
                       " control.access[READER] = %ld epoch = %d\n",
                       transaction, index, i + offset, source_seg,
                       (transaction_t *)source_seg->allocated,
                       (transaction_t*)source_seg->control[i + offset].access_set[WRITER],
                       source_seg->control[i + offset].access_set[READERS],
                       transaction->epoch);*/
                if (read_word(i, offset, align, transaction, source_seg, target) == false) {
                    /*printf("Tm abort %p read word: "
                           "segment[%d]-[%zu] = %p, allocated = %p, control.access[WRITER] = %p"
                           " control.access[READER] = %ld epoch = %d\n",
                           transaction, index, i + offset, source_seg,
                           (transaction_t *)source_seg->allocated,
                           (transaction_t*)source_seg->control[i + offset].access_set[WRITER],
                           source_seg->control[i + offset].access_set[READERS],
                           transaction->epoch);*/
                    return tm_abort(shared, transaction);
                }
            }
            return true;
        }
    }
    return tm_abort(shared, transaction);
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
    size_t align = region->align;

    if(transaction->region != region) {
        return tm_abort(shared, transaction);
    }
    size_t num_words = size / align;
    int index = find_target_segment(region->segments, target, atomic_load(&region->size));
    if(index != EMPTY) {
        segment_t *target_seg = region->segments[index];
        if (target_seg != NULL) {

        lock_acquire(&region->segment_locks[index]);
            if (target_seg->allocated != tx && target_seg->allocated != 0)
            {
                /*printf("Tm abort %p: writing in alloc segment[%d] by %p epoch == %d\n",
                       transaction, index, (transaction_t*)target_seg->allocated,
                       transaction->epoch);*/
                lock_release(&region->segment_locks[index]);
                // prevent write if segment was allocated by another segment
                return tm_abort(shared, transaction);
            }
            if(target_seg->deregister != 0) {
                printf("Tm abort %p: writing in freed segment[%d] by %p epoch == %d\n",
                       transaction, index, (transaction_t*)target_seg->deregister,
                       transaction->epoch);
                lock_release(&region->segment_locks[index]);
                // prevent write if segment was allocated by another segment
                return tm_abort(shared, transaction);
            }

        lock_release(&region->segment_locks[index]);

            size_t offset = ((uintptr_t) target - (uintptr_t) target_seg->readable_copy) / align;
            for (size_t i = 0; i < num_words; i++) {
                /*printf("Tm write %p write word: "
                       "segment[%d]-[%zu] content = %ld, control.access[WRITER] = %p"
                       " control.access[READER] = %ld epoch = %d\n",
                       transaction, index, i + offset,
                       *(u_long * )(source + index),
                       (transaction_t*)target_seg->control[i + offset].access_set[WRITER],
                       target_seg->control[i + offset].access_set[READERS],
                       transaction->epoch);*/
                if (write_word(i, offset, align, transaction, target_seg, source) == false) {
                    /*printf("Tm abort %p write word: "
                           "segment[%d]-[%zu] content = %ld, control.access[WRITER] = %p"
                           " control.access[READER] = %ld epoch = %d\n",
                           transaction, index, i + offset,
                           *(u_long * )(source + index),
                           (transaction_t*)target_seg->control[i + offset].access_set[WRITER],
                           target_seg->control[i + offset].access_set[READERS],
                           transaction->epoch);*/
                    return tm_abort(shared, transaction);
                }
            }
            add_to_list(transaction->allocated, index, &transaction->size);
            return true;
        }
    }
    return tm_abort(shared, transaction);
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
    region_t* region = (region_t*) shared;
    size_t align = region->align < sizeof(segment_t*) ? sizeof(void*) : region->align;

    if(transaction->region != region || size % align != 0) {
        tm_abort(shared, transaction);
        return abort_alloc;
    }
    // contention point on the stack
    lock_acquire(&region->global_lock);
    int index = EMPTY;
    bool init_new_lock = false;
    if(!is_empty(&region->next_free)) {
        index = pop(&region->next_free);
    } else if(region->size < MAX_REGION_SIZE - 1) {
        index = atomic_fetch_add(&region->size, 1);
        init_new_lock = true;
    } else { // More than 2^48 segments were allocated
        lock_release(&region->global_lock);
        return nomem_alloc;
    }

    if (posix_memalign((void**)&region->segments[index],
                       align,
                       sizeof(segment_t) + 2 * size) != 0) {
        push(&region->next_free, index);
        lock_release(&region->global_lock);
        tm_abort(shared, transaction);
        return abort_alloc;
    }
    lock_release(&region->global_lock);
    create_segment(region->segments[index], size, align, transaction);
    if(init_new_lock) {
        lock_init(&region->segment_locks[index]);
    }
    *target = (void*) region->segments[index]->readable_copy;
    add_to_list(transaction->allocated, index, &transaction->size);
    /*printf("Tm alloc %p : segment[%d] at epoch = %d\n", transaction,
              index, transaction->epoch);*/
    /*printf("Tm alloc %p : segment[%d] = %p, (read)*target = %p (=%ld),"
           " (write)*target = %p, size = %zu, epoch = %d\n",
           transaction, index, region->segments[index],
           *target, (uintptr_t)(*target),region->segments[index]->writable_copy,
           size, transaction->epoch);*/
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
    if(transaction->region != region ||
    target == region->segments[INIT_SEGMENT]->readable_copy) {
        return tm_abort(shared, transaction);
    }

    int index = find_target_segment(region->segments, target, region->size);

    if(index != EMPTY) {
        segment_t *segment = region->segments[index];

        //if (segment != NULL) {
            //lock_acquire(&region->global_lock);
            lock_acquire(&region->segment_locks[index]);
            // ensure that only one tx can free the segment
            if(segment->allocated != 0 || segment->deregister != 0) {
                //lock_release(&region->global_lock);
                lock_release(&region->segment_locks[index]);
                return tm_abort(shared, transaction);
            }
            segment->deregister = (tx_t) transaction;
            // overwrite any tx that might have written during the epoch
            /*size_t size = segment->size / region->align;
            for(size_t i = 0; i < size; i++) {
                lock_acquire(&segment->locks[i]);
                segment->control->access_set[WRITER] = tx;
                lock_release(&segment->locks[i]);
            }*/
            /*printf("Tm free %p : segment[%d]->deregister = %p epoch = %d\n", transaction,
                   index, (transaction_t *)segment->deregister, transaction->epoch);*/
            lock_release(&region->segment_locks[index]);
            //lock_release(&region->global_lock);
            add_to_list(transaction->allocated, index, &transaction->size);
            return true;
        //}
    }
    return tm_abort(shared, transaction);
}