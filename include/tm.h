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
#include <stdlib.h>

// DEBUG
#include<stdio.h>

#define READABLE 0
#define WRITABLE 1
#define VALID  1
#define INVALID 0
#define MODIFIED 1
#define NOT_MODIFIED 0

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
    int counter;
    int  remaining;
    shared_t* blocked;
    size_t size;
} batcher_t;

typedef struct {
    int valid;
    int writable;
    int modified;
} segment_control_t;

// Linked list of segments
typedef struct seg {
    struct seg* next;
    struct seg* prev;
    size_t size; // number of words time the alignment (in bytes)
    void* words; // words are size of alignment, points to the first word in the segment
    void* words_copy;
    segment_control_t control;
} segment_t;

// Region contains the first segment
typedef struct {
    segment_t init_segment;
    size_t size;
    size_t align;
    batcher_t batcher;
} region_t;


typedef struct {
    shared_t shared;
    bool is_ro;
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



//int init_batcher(batcher_t* batcher);
//int get_epoch(batcher_t* batcher);
//int enter(batcher* batcher, thread_t* shared);
//int leave(batcher* batcher, thread_t* shared);