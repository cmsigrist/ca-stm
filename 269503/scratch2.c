//
// Created by capucine on 27/11/2021.
//
#include <pthread.h>
#include "../include/tm.h"
#include <stdio.h>
#include <stdatomic.h>

#include <assert.h>

#define THREADS 300
#define ALIGN 8

static atomic_int counter = 0;

void* stm(shared_t shared) {
    tx_t tx = tm_begin(shared, true);
    void* target = calloc(2, sizeof(word_t) * ALIGN);
    if(tm_read(shared, tx, tm_start(shared), ALIGN, target)) {
        printf("READ OK : %d\n", atomic_fetch_add(&counter, 1) + 1);
    }
    tm_end(shared, tx);
    free(target);
    return NULL;
}

int main() {
    pthread_t handlers[THREADS];
    printf("Creating region\n");
    shared_t shared_region = tm_create(256 * ALIGN, ALIGN);

    counter = 0;
    for (intptr_t i = 0; i < THREADS; i++) {
        int res = pthread_create(&handlers[i], NULL, stm, shared_region);
        assert(!res);
    }

    for (int i = 0; i < THREADS; i++) {
        int res = pthread_join(handlers[i], NULL);
        assert(!res);
    }
    assert(counter == THREADS);
    printf("DONE\n");
}

