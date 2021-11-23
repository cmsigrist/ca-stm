/**
 * @file   tm.h
 * @author Sébastien ROUAULT <sebastien.rouault@epfl.ch>
 * @author Antoine MURAT <antoine.murat@epfl.ch>
 *
 * @section LICENSE
 *
 * Copyright © 2018-2021 Sébastien ROUAULT.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * any later version. Please see https://gnu.org/licenses/gpl.html
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * @section DESCRIPTION
 *
 * Interface declaration for the transaction manager to use (C version).
 * YOU SHOULD NOT MODIFY THIS FILE.
**/

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h> // for calloc() and realloc()
#include <string.h> // for memcpy()
#include <stdatomic.h>

// DEBUG
#include<stdio.h>

// Valid copy
#define READABLE_COPY 0
#define WRITABLE_COPY 1
// Access set
#define NOT_MODIFIED 0
#define MULTIPLE_READERS 2

#define WRITER 0
#define READERS 1
// Written
#define NOT_WRITTEN -1
// Thread
#define BLOCKED 1
#define NOT_BLOCKED 0

#define INIT_SEGMENT 0
#define N 50

#define COMMITTED 1
#define ABORTED 0

#define START_NEW_EPOCH 1
#define WAIT 0

// -------------------------------------------------------------------------- //

typedef void* shared_t; // The type of a shared memory region
static shared_t const invalid_shared = NULL; // Invalid shared memory region

// Note: a uintptr_t is an unsigned integer that is big enough to store an
// address. Said differently, you can either use an integer to identify
// transactions, or an address (e.g., if you created an associated data
// structure).
typedef uintptr_t tx_t; // The type of a transaction identifier
static tx_t const invalid_tx = ~((tx_t) 0); // Invalid transaction constant

typedef int alloc_t;
static alloc_t const success_alloc = 0; // Allocation successful and the TX can continue
static alloc_t const abort_alloc   = 1; // TX was aborted and could be retried
static alloc_t const nomem_alloc   = 2; // Memory allocation failed but TX was not aborted

typedef int word_t;

typedef struct {
    int valid; // readable or writable copy is valid
    atomic_uintptr_t access_set[2]; // access_set[0] = writer, access_set[1] = #readers
    atomic_int written; // epoch in which it was written
} word_control_t;

typedef struct {
    size_t size; // in bytes, there are size/align words, offset 0
    word_t* readable_copy; // readable copy, offset 8
    word_t* writable_copy; // writeable copy, offset 16
    word_control_t* control; // offset 24
    int deregister;
} segment_t;

typedef struct {
    atomic_int epoch; //Atomic
    atomic_int to_commit_counter;
    atomic_int time_to_commit;
    atomic_int remaining; // to consume when thread leaves
    tx_t** blocked; // list of uid of tx
    atomic_size_t size;
} batcher_t;

// Region contains the first segment
typedef struct {
    segment_t** segments; // list of segments
    size_t size; // number of segments allocated in the region
    size_t align;
    batcher_t batcher;
} region_t;

// TODO assert that tx accesses only region
typedef struct {
    bool is_ro;
    int is_blocked;
    segment_t** allocated;
    size_t size;
    shared_t* region;
    //int epoch;
    //bool committed;
} transaction_t;
// -------------------------------------------------------------------------- //

shared_t tm_create(size_t, size_t);
void     tm_destroy(shared_t);
void*    tm_start(shared_t);
size_t   tm_size(shared_t);
size_t   tm_align(shared_t);
tx_t     tm_begin(shared_t, bool);
bool     tm_end(shared_t, tx_t);
bool     tm_read(shared_t, tx_t, void const*, size_t, void*);
bool     tm_write(shared_t, tx_t, void const*, size_t, void*);
alloc_t  tm_alloc(shared_t, tx_t, size_t, void**);
bool     tm_free(shared_t, tx_t, void*);




int create_segment(segment_t**, size_t, size_t, size_t);
