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

// remaining -> size == -1 or commit -> size = commit counter
int wait(lock_t* lock, atomic_int* remaining, int size) {
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
    lock_release(lock);
    lock_wake_up(lock);
    return 0;
}

// produce either a block or a commit
int produce(lock_t* lock, atomic_size_t* to_produce, size_t size) {
    lock_acquire(lock);
    atomic_fetch_add(to_produce, size);
    lock_release(lock);
    lock_wake_up(lock);
    return 0;
}

bool elect(uintptr_t tx, atomic_uintptr_t* leader) {
    uintptr_t expected = 0;
    return atomic_compare_exchange_strong(leader, &expected, tx);
}
// -------------------------------------------------------------------------- //
// ------------------------------BATCHER------------------------------------- //
// -------------------------------------------------------------------------- //

batcher_t init_batcher() {
    batcher_t batcher;
    batcher.epoch =  0;
    batcher.remaining = 0;
    batcher.commit_counter = 0;
    batcher.size = 0;
    batcher.commit_cv = 0;
    batcher.block_cv= 0;
    batcher.leader = 0;
    lock_init(&batcher.lock);
    lock_init(&batcher.lock_commit);
    lock_init(&batcher.lock_block);
    return batcher;
}

/**
 *
 * @param batcher
 * @return Returns an integer that is unique to each batch of threads
 */
int get_epoch(batcher_t* batcher) {
    return atomic_load(&batcher->epoch);
}

int enter(batcher_t* batcher) {
    //printf("Entering batcher tx : %p, remaining = %d\n", transaction, atomic_load(&batcher->remaining));
    //printf("Tx : %p\n", transaction);
    int expected = 0;
    if (!atomic_compare_exchange_strong(&batcher->remaining, &expected, 1)) {
        lock_acquire(&batcher->lock); // st it cannot be acquired while the last tx is leaving the region;
        //printf("Entering batcher tx : %p, remaining = %zu\n", transaction, atomic_load(&batcher->remaining));
        atomic_fetch_add(&batcher->size, 1);
        lock_release(&batcher->lock);
        //printf("Enter : batcher->size = %zu\n", atomic_load(&batcher->size));
        wait_for_wake_up(&batcher->lock_block, &batcher->block_cv);
        //printf("Enter : %p entered\n", transaction);
    } else {
        lock_acquire(&batcher->lock);
        atomic_fetch_add(&batcher->commit_counter, 1);
        lock_release(&batcher->lock);
        //printf("Enter : %p entered\n", transaction);
    }
    return 0;
}

int start_new_epoch(batcher_t* batcher) {
    atomic_fetch_add(&batcher->epoch, 1);

    //printf("Start new epoch %d:  commit->counter = %d\n", batcher->epoch, atomic_load(&batcher->commit_counter));
    wait(&batcher->lock_commit, (atomic_int*)&batcher->commit_cv, batcher->commit_counter);
    size_t size = atomic_load(&batcher->size);


    atomic_store(&batcher->remaining, size); // set remaining to size
    atomic_store(&batcher->commit_counter, size);
    atomic_store(&batcher->size, 0);
    atomic_store(&batcher->leader, 0);
    //printf("Start new epoch %d: deblocking %zu threads\n", batcher->epoch, size);
    produce(&batcher->lock_block, &batcher->block_cv, size);
    wait(&batcher->lock_block, (atomic_int*)&batcher->block_cv, 0);
    printf("Start new epoch %d : remaining %d\n",batcher->epoch,  batcher->remaining);
    return 0;
}

int leave(batcher_t* batcher) {
    int expected = 1;
    if(atomic_compare_exchange_strong(&batcher->remaining, &expected, -1)) {
        //printf("Leaving batcher : remaining = %zu\n", batcher->remaining);
        lock_wake_up(&batcher->lock_commit); // signal that last tx left
        return 1;
    } else {
        atomic_fetch_sub(&batcher->remaining, 1);
        //printf("Leaving batcher : remaining = %zu\n", batcher->remaining);
        return 0;
    }
}


// -------------------------------------------------------------------------- //
// ------------------------------SEGMENT_HELPER------------------------------ //
// -------------------------------------------------------------------------- //
int create_segment(segment_t** segments, size_t index, size_t size, size_t align) {
    segments[index] = calloc(1, sizeof(segment_t));
    if(segments[index] == NULL) {
        return 1;
    }
    segments[index]->size = size; // in bytes
    size_t num_words = size / align;
    segments[index]->readable_copy = calloc(num_words, sizeof(word_t) * align); // a word is align bytes
    segments[index]->writable_copy = calloc(num_words, sizeof(word_t) * align);

    segments[index]->control = calloc(num_words, sizeof(word_control_t));
    segments[index]->locks = calloc(num_words, sizeof(lock_t));
    for(size_t i = 0; i < num_words; i++) {
        segments[index]->control[i].valid = READABLE_COPY;
        segments[index]->control[i].access_set[WRITER] = NOT_MODIFIED;
        segments[index]->control[i].access_set[READERS] = NOT_MODIFIED;
        segments[index]->control[i].written = NOT_WRITTEN;
        lock_init(&segments[index]->locks[i]);
    }

    segments[index]->deregister = 0;
    return 0;
}

void free_segment(segment_t* segment, size_t size) {
    free(segment->readable_copy);
    free(segment->writable_copy);
    free(segment->control);
    for(size_t i = 0; i < size; i++) {
        lock_cleanup(segment->locks);
    }
    free(segment->locks);
    free(segment);
}

// -------------------------------------------------------------------------- //
// ---------------------------TRANSACTIONS HELPER---------------------------- //
// -------------------------------------------------------------------------- //

bool in_copy(const void* target, void* copy, size_t size) {
    if (target >= copy && target < copy + size) {
        return true;
    }
    return false;
}

segment_t* find_target_segment(segment_t** segments,
                        const void* target,
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

    return NULL;
}

bool read_word(size_t index,
               size_t offset,
               size_t align,
               transaction_t* tx,
               segment_t* source_seg,
               void* target_seg,
               int current_epoch)
{
    lock_acquire(&source_seg->locks[index + offset]);
    word_control_t control = source_seg->control[index + offset];

    word_t* readable_copy = control.valid == READABLE_COPY ? source_seg->readable_copy : source_seg->writable_copy;
    word_t* writable_copy = control.valid == READABLE_COPY ? source_seg->writable_copy : source_seg->readable_copy;
    if(tx->is_ro) {
        //printf("Reading readable copy = %p\n", &readable_copy[align * (index + offset)]);
        //printf("Content : %d in %p\n", (char)readable_copy[align * (index + offset)], (word_t*)target_seg + index);
        memcpy((word_t*)target_seg + (align * index), &readable_copy[align * (index + offset)], align);
        lock_release(&source_seg->locks[index + offset]);
        return true;
    } else {
        if(control.written == current_epoch) {
            if(control.access_set[WRITER] == (uintptr_t)&(*tx)) {
                //printf("Current epoch Reading writable copy = %p\n", &writable_copy[align * (index + offset)]);
                //printf("Content : %d in %p\n", writable_copy[index + offset], (word_t*)target_seg + index);
                memcpy((word_t*)target_seg + (align * index), &writable_copy[align * (index + offset)], align);
                lock_release(&source_seg->locks[index + offset]);
                return true;
            } else {
                lock_release(&source_seg->locks[index + offset]);
                return false;
            }
        } else {
            //printf("Reading readable copy = %p\n", &readable_copy[align * (index + offset)]);
            //printf("Content : %d in %p\n", readable_copy[index + offset], (word_t*)target_seg + index);
            memcpy((word_t*)target_seg + (align * index), &readable_copy[align * (index + offset)], align);
            // first to read
            if(control.access_set[READERS] == NOT_MODIFIED) {
                source_seg->control[index + offset].access_set[READERS] = (uintptr_t)&(*tx);
            } // different from first reader
            else if(control.access_set[READERS] != (uintptr_t)&(*tx)) {
                source_seg->control[index + offset].access_set[READERS] = MULTIPLE_READERS;
            }
            lock_release(&source_seg->locks[index + offset]);
            return true;
        }
    }

}

bool write_word(size_t index,
                size_t offset,
                size_t align,
                transaction_t* tx,
                segment_t* target_seg,
                const void* source,
                int current_epoch) {
    lock_acquire(&target_seg->locks[index + offset]);
    /*printf("-----------------\n");
    printf("index + offset = %zu\n", index + offset);*/
    word_control_t control = target_seg->control[index + offset];
    word_t* writable_copy = control.valid == READABLE_COPY ? target_seg->writable_copy : target_seg->readable_copy;

    if(control.written == current_epoch) {
        if(control.access_set[WRITER] == (uintptr_t)&(*tx)) {
            //printf("Writing word to %p\n", &writable_copy[align*(index + offset)]);
            //printf("from : %p\t", (word_t*)(source) + index);
            //printf("content = %d\n", *((word_t*)(source) + index));
            memcpy(&writable_copy[align*(index + offset)], (word_t*)(source) + (align * index), align);
            lock_release(&target_seg->locks[index + offset]);
            return true;
        } else {
            lock_release(&target_seg->locks[index + offset]);
            return false;
        }
    } else {
        if(control.access_set[READERS] == MULTIPLE_READERS) {
            lock_release(&target_seg->locks[index + offset]);
            return false;
        } else {
            //printf("Writing word to %p\n", &writable_copy[align*(index + offset)]);
            //printf("from : %p\t", (word_t*)(source) + index);
            //printf("content = %d\n", *((word_t*)(source) + index));
            memcpy(&writable_copy[align*(index + offset)], (word_t*)(source) + (align * index), align);
            target_seg->control[index + offset].access_set[WRITER] = (uintptr_t)&(*tx);
            target_seg->control[index + offset].written = current_epoch;
            lock_release(&target_seg->locks[index + offset]);
            return true;
        }
    }
}

int commit(region_t* region, transaction_t* transaction) {
    if(transaction->is_ro == false) {
        batcher_t* batcher = &region->batcher;
        lock_acquire(&batcher->lock);
        int current_epoch = get_epoch(batcher);

        for (size_t i = 0; i < transaction->size; i++) { // for each segment
            segment_t *segment = transaction->allocated[i];
            size_t size = segment->size / region->align;
            for (size_t j = 0; j < size; j++) {
                lock_acquire(&segment->locks[j]);
                if (segment->control[j].written == current_epoch) {
                    segment->control[j].valid = 1 - segment->control[j].valid; // swap valid copy
                }
                //printf("Commit : committed word[%zu] : %p, valid copy is = %d\n", j, segment, segment->control[j].valid);
                segment->control[j].access_set[WRITER] = NOT_MODIFIED;
                segment->control[j].access_set[READERS] = NOT_MODIFIED;
                lock_release(&segment->locks[j]);
            }
            if (transaction->allocated[i]->deregister == 1) {
                free_segment(transaction->allocated[i], size);
            }
        }
        lock_release(&batcher->lock);
    }
    return 0;
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
    //printf("CREATE REGION\n");
    if(size % align != 0) {
        return invalid_shared;
    }
    region_t* region = calloc(1, sizeof(region_t));
    region->segments = calloc(N, sizeof(segment_t*)); // allocate list of N segments
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
    //printf("DESTROY REGION\n");
    region_t* region = ((region_t*)shared);
    size_t size = atomic_load(&region->size);
    for(size_t i = 0; i < size; i++) {
        free_segment(region->segments[i], region->segments[i]->size / region->align);
    }
    lock_cleanup(&region->batcher.lock);
    lock_cleanup(&region->batcher.lock_commit);
    lock_cleanup(&region->batcher.lock_block);
    free(region);
    //printf("Destroyed region %p", shared);
    return;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    //printf("Start %p\n", ((region_t*)shared)->segments[0]->readable_copy);
    return((region_t*)shared)->segments[0]->readable_copy;
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
    //printf("Tm begin : %p\n", transaction);
    if(transaction != NULL) {
        transaction->is_ro = is_ro;
        //transaction->is_blocked = NOT_BLOCKED;
        transaction->allocated = calloc(N, sizeof(segment_t*));
        transaction->size = 0;
        transaction->region = region;
        transaction->epoch = get_epoch(&region->batcher);
        //printf("Created tx : %p\n", (uintptr_t)transaction);

        //enter transaction
        enter(&region->batcher);
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
    transaction_t *transaction = (transaction_t *) tx;
    printf("Tm end : %p\n", transaction);
    region_t *region = (region_t *) shared;
    batcher_t *batcher = &region->batcher;

    /* on tm enter wait for wake up
     * on tm end, wait for last tx to leave, when true wait for everybody to commit ->
     * first to commit is the leader that wakes up everybody
     */

    /*
     * somebody set remaining to -1 and wake up the one waiting on the lock
     */
    if(!leave(batcher)) { // left before last tx
        wait(&batcher->lock_commit, &batcher->remaining, -1);
    }
    // last tx left
    commit(region, transaction);
    produce(&batcher->lock_commit, &batcher->commit_cv, 1);
    // one leader elect per epoch
    if(elect(tx, &batcher->leader)) {
        lock_acquire(&batcher->lock);
        start_new_epoch(batcher);
        lock_release(&batcher->lock);
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
    //printf("------------------------------------\n");
    transaction_t* transaction = (transaction_t*)tx;
    //printf("READ : %p\n", transaction);
    region_t* region = (region_t*)shared;
    batcher_t* batcher = &region->batcher;
    size_t align = region->align;

    if(transaction->region != region) {
        return false;
    }
    size_t num_words = size / align;
    segment_t* source_seg = find_target_segment(region->segments, source, atomic_load(&region->size));

    //printf("Reading in segment = %p\n", source_seg);
    if(source_seg != NULL) {
        size_t offset = (uintptr_t)source >= (uintptr_t)source_seg->writable_copy ?
                                            (int)((uintptr_t)source - (uintptr_t)source_seg->writable_copy) / align :
                                            (int)((uintptr_t)source - (uintptr_t)source_seg->readable_copy) / align;
        //printf("Offset = %zu\n", offset);

        int current_epoch = transaction->epoch; // get_epoch()
        for (size_t i = 0; i < num_words; i++) {
            if (read_word(i, offset, align, transaction, source_seg, target, current_epoch) == false) { // TODO problem of remaining and commit counter
                lock_acquire(&batcher->lock);
                //printf("Abort tx: %p\n", transaction);
                if(batcher->remaining > 1) {
                    atomic_fetch_sub(&batcher->remaining, 1);

                } else {
                    atomic_store(&batcher->remaining, -1);
                }
                if(atomic_load(&batcher->commit_counter) > 0) {
                    atomic_fetch_sub(&batcher->commit_counter, 1);
                }
                printf("Abort READ %p : remaining = %d commit counter = %d\n", transaction, batcher->remaining, batcher->commit_counter);
                lock_release(&batcher->lock);
                lock_wake_up(&batcher->lock_commit);
                //printf("Abort : called wake up\n");
                return false;
            }
        }

        //printf("------------------------------------\n");
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

bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    transaction_t* transaction = (transaction_t*)tx;
    //printf("WRITE : %p\n", transaction);
    region_t* region = (region_t*)shared;
    batcher_t* batcher = &region->batcher;
    size_t align = region->align;

    if(transaction->region != region) {
        return false;
    }
    size_t num_words = size / align;
    segment_t* target_seg = find_target_segment(region->segments, target, atomic_load(&region->size));
    /*printf("Writing %zu words\n", num_words);
    printf("Writing in segment : %p\n", target_seg);*/
    if(target_seg != NULL) {
        size_t offset = (uintptr_t)target >= (uintptr_t)target_seg->writable_copy ?
                        (int)((uintptr_t)target - (uintptr_t)target_seg->writable_copy) / align :
                        (int)((uintptr_t)target - (uintptr_t)target_seg->readable_copy) / align;
        //printf("Offset = %zu\n", offset);

        //printf("Starting write tx: %u, in epoch %d\n", transaction, current_epoch); */
        int current_epoch = transaction->epoch; // get_epoch(batcher)
        for(size_t i = 0; i < num_words; i++) {
            if(write_word(i, offset, align, transaction, target_seg, source, current_epoch) == false) {
                lock_acquire(&batcher->lock);
                //printf("Abort tx: %p\n", transaction);
                if(batcher->remaining > 1) {
                    atomic_fetch_sub(&batcher->remaining, 1);

                } else {
                    atomic_store(&batcher->remaining, -1);
                }
                if(atomic_load(&batcher->commit_counter) > 0) {
                    atomic_fetch_sub(&batcher->commit_counter, 1);
                }
                printf("Abort WRITE %p : remaining = %d commit counter = %d\n", transaction, batcher->remaining, batcher->commit_counter);
                lock_release(&batcher->lock);
                lock_wake_up(&batcher->lock_commit);
                //printf("Abort : called wake up\n");
                return false;
            }
        }
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
        //printf("------------------------------------\n");
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

// TODO Bus error on ALLOC
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) {
    //printf("------------------------------------\n");
    transaction_t* transaction = (transaction_t*) tx;
    printf("ALLOC : %p\n", transaction);
    region_t* region = (region_t*)shared;
    size_t align = region->align;

    if(transaction->region != region) {
        return abort_alloc;
    }

    lock_acquire(&region->batcher.lock);
    if(size % align != 0) {
        lock_release(&region->batcher.lock);
        return abort_alloc;
    }
    size_t region_size = atomic_fetch_add(&region_size, 1);
    // the size of the region is a multiple of the allocated number of segments then need to allocate N more segments
    if(region_size % N == 0) {
        if(realloc(region->segments, region_size + sizeof(segment_t) * N) == NULL) {
            lock_release(&region->batcher.lock);
            return nomem_alloc;
        }
    }
    if(transaction->size % N == 0) {
        if(realloc(transaction->allocated, transaction->size + sizeof(segment_t) * N) == NULL) {
            lock_release(&region->batcher.lock);
            return nomem_alloc;
        }
    }
    //printf("tx_seg[size] = %p\n", transaction->allocated[transaction->size]);
    if(create_segment(region->segments, region_size, size, align) != 0) {
        printf("Nomem tx: %p\n", transaction);
        lock_release(&region->batcher.lock);
        return nomem_alloc;
    }

    transaction->allocated[transaction->size] = region->segments[region_size];

    /*printf("transaction->allocated[transaction->size] = %p\n", transaction->allocated[transaction->size]);
    printf("transaction->allocated[transaction->size]->r = %p\n", transaction->allocated[transaction->size]->readable_copy);
    printf("transaction->allocated[transaction->size]->w = %p\n", transaction->allocated[transaction->size]->writable_copy);*/
    *target = region->segments[region_size]->readable_copy;

    transaction->size += 1;
    lock_release(&region->batcher.lock);
    //printf("------------------------------------\n");
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    //printf("------------------------------------\n");
    transaction_t* transaction = (transaction_t*) tx;
    printf("FREE : %p\n", transaction);
    region_t* region = (region_t*)shared;

    if(transaction->region != region) {
        return false;
    }
    if(target == region->segments[0]->readable_copy) {
        return false;
    }

    // mark target for deregistering
    segment_t* segment = find_target_segment(region->segments, target, region->size);
    //printf("segment found = %p\n", segment);
    segment->deregister = 1;

    //printf("------------------------------------\n");
    return true;
}
