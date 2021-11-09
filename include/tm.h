/**
 * @file   tm.h
 * @author Sébastien ROUAULT <sebastien.rouault@epfl.ch>
 *
 * @section LICENSE
 *
 * Copyright © 2018-2019 Sébastien ROUAULT.
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

// DEBUG
#include<stdio.h>

// Valid copy
#define READABLE_COPY 0
#define WRITABLE_COPY 1
// Access set
#define NOT_MODIFIED 0
#define MODIFIED 1
#define MODIFIED_BY_CURRENT_TX 2
// Written
#define NOT_WRITTEN 0
#define WRITTEN 1
// Thread
#define BLOCKED 1
#define NOT_BLOCKED 0

#define INIT_SEGMENT 0
#define N 50


// -------------------------------------------------------------------------- //

typedef void* shared_t; // should point to a list of segments
static shared_t const invalid_shared = NULL; // Invalid shared memory region

typedef uintptr_t tx_t;
static tx_t const invalid_tx = ~((tx_t) 0); // Invalid transaction constant

typedef int alloc_t;
static alloc_t const success_alloc = 0; // Allocation successful and the TX can continue
static alloc_t const abort_alloc   = 1; // TX was aborted and could be retried
static alloc_t const nomem_alloc   = 2; // Memory allocation failed but TX was not aborted

typedef struct {
    bool is_ro;
    int is_blocked;
    //private_mem_region_t region;
    //bool committed;
} transaction_t;

typedef struct {
    int counter;
    int  remaining;
    transaction_t** blocked; // contains a list of transactions waiting to be started
    size_t size;
} batcher_t;

//typedef void* word_t; // is it useful ?

typedef struct {
    int valid; // readable or writable copy is valid
    int access_set;
    int written; // in the current epoch
} word_control_t;

typedef struct {
    size_t size; // in bytes, there are size/align words
    void* readable_copy; // readable copy
    void* writable_copy; // writeable copy
    word_control_t* control;
} segment_t;

// Region contains the first segment
typedef struct {
    segment_t* segments; // list of segments
    size_t size; // number of segments allocated in the region
    size_t align;
    batcher_t batcher;
} region_t;
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



//int init_batcher(batcher_t* batcher);
//int get_epoch(batcher_t* batcher);
//int enter(batcher* batcher, thread_t* shared);
//int leave(batcher* batcher, thread_t* shared);