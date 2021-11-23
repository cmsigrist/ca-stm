//
// Created by capucine on 16/11/2021.
//
#include <pthread.h>
#include "../include/tm.h"
#include <stdio.h>

#include <assert.h>

#define THREADS 3
#define ALIGN 32

//align 16 -> 32 bytes = 2 words -> space of 32 bytes between r and w
// align 8 -> 32 bytes = 4 ords -> space of 48 bytes between r and w

void* init_sm(shared_t shared_region) {
    tx_t tx = tm_begin(shared_region, false);
    printf("_________________________________________________________________\n");
    printf("INIT TX : %ld began\n", tx);

    word_t* accounts = calloc(4, sizeof(word_t)); // = to readable copy
    word_t* accounts1 = calloc(4, sizeof(word_t));
    word_t* accounts2 = calloc(4096, sizeof(word_t));
    word_t* accounts3 = calloc(16*4096, sizeof(word_t));

    tm_alloc(shared_region, tx, ALIGN*4, (void**)&accounts);
    tm_alloc(shared_region, tx, ALIGN*4, (void**)&accounts1);
    tm_alloc(shared_region, tx, ALIGN * 4096, (void**)&accounts2);
    tm_alloc(shared_region, tx, ALIGN*16*4096, (void**)&accounts3);

    for(int i = 0; i < 4; i++) { // = readable_copy, accounts - 0x30 == seg, accounts + 30 == writable
        //printf("r &accounts[%i] = %p\n", i, &(accounts[i])); // word_t
        printf("r accounts + %i = %p\n", i, accounts + i);
    }

    for(int i = 0; i < 4; i++) { // = readable_copy, accounts - 0x30 == seg, accounts + 30 == writable
        printf("r accounts[%d]= %d\n", i, accounts[i]); // word_t
    }

    word_t* account_init = calloc(4, sizeof(word_t));
    account_init[0] = 12;
    account_init[1] = 10;
    account_init[2] = 13;
    account_init[3] = 14;

    for(int i = 0; i < 4; i++) {
        printf("&account_init[%d] = %p\n", i, &account_init[i]);
    }
    /*for(int i = 0; i < 4; i++) {
        printf("* account_init + %i = %d\n", i, *((word_t*)(account_init) + i));
    }*/

    // write source in target, where is target

    // must write only in a segment allocated by tx,
    printf("write = %d\n", tm_write(shared_region, tx, &account_init[2], 2*ALIGN, accounts + 2) == true); // or accounts + 2 ?
    printf("Wrote accounts[2] and [3]\n");

    word_t* target = calloc(4, sizeof(word_t));
    tm_read(shared_region, tx, &accounts[2], 2*ALIGN, target + 2);

    for(int i  = 0; i < 4; i++) {
        printf("target + %d = %p\n", i, target + i);
    }

    for(int i = 0; i < 4; i++) {
        printf("accounts[%d] = %d\n", i, *(target + i));
    }
    printf("write = %d\n", tm_write(shared_region, tx, &account_init[0], ALIGN * 1, accounts) == true);
    printf("Wrote accounts[0]\n");

    tm_write(shared_region, tx, &account_init[1], ALIGN * 1, accounts + 1);

    tm_read(shared_region, tx, &accounts[0], ALIGN * 4, target);

    for(int i = 0; i < 4; i++) {
        printf("accounts[%d] = %d\n", i, *(target + i));
    }

    tm_end(shared_region, tx);
    printf("End tx\n");
    printf("_________________________________________________________________\n");
    return NULL;
}

void* bob(shared_t shared_region) {
    region_t* region = (region_t*) shared_region;
    tx_t tx = tm_begin(shared_region, false);
    printf("_________________________________________________________________\n");
    printf("BOB TX : %ld began\n", tx);

    word_t* accounts = calloc(2, sizeof(word_t));

    printf("account + 0 = %p\n", accounts);
    printf("account + 1 = %p\n", accounts + 1);
    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[1], 8, accounts + 1) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }
    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[0], 8, accounts + 0) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    int source = 20;
    if(tm_write(shared_region, tx, &source, 8, &region->segments[1]->readable_copy[1]) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[0], 16, accounts) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    source = 2;
    if(tm_write(shared_region, tx, &source, 8, &region->segments[1]->readable_copy[0]) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[0], 16, accounts) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }
    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    tm_end(shared_region, tx);
    printf("_________________________________________________________________\n");

    return accounts;
}

void* charlie(shared_t shared_region) {
    region_t* region = (region_t*) shared_region;
    tx_t tx = tm_begin(shared_region, false);
    printf("_________________________________________________________________\n");
    printf("Charlie TX : %ld began\n", tx);

    word_t* accounts = calloc(2, sizeof(word_t));

    printf("account + 0 = %p\n", accounts);
    printf("account + 1 = %p\n", accounts + 1);
    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[1], 8, accounts + 1) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }
    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[0], 8, accounts + 0) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    int source = 27;
    if(tm_write(shared_region, tx, &source, 8, &region->segments[1]->readable_copy[1]) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[0], 16, accounts) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    source = 5;
    if(tm_write(shared_region, tx, &source, 8, &region->segments[1]->readable_copy[0]) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }

    if(tm_read(shared_region, tx, &region->segments[1]->readable_copy[0], 16, accounts) == false) {
        tm_end(shared_region, tx);
        return accounts;
    }
    printf("* account + 0 = %d\n", *(accounts + 0));
    printf("* account + 1 = %d\n", *(accounts + 1));

    // TODO test free

    tm_end(shared_region, tx);
    printf("_________________________________________________________________\n");
    return accounts;
}


int main() {
    pthread_t handlers[THREADS];
    printf("Creating region\n");
    shared_t shared_region = tm_create(2 * ALIGN, ALIGN);

    int res_init = pthread_create(&handlers[0], NULL, init_sm, shared_region);
    assert(!res_init);
    int res = pthread_join(handlers[0], NULL);
    assert(!res);

    /*int res_bob = pthread_create(&handlers[1], NULL, bob, shared_region);
    assert(!res_bob);
    int res_charlie = pthread_create(&handlers[2], NULL, charlie, shared_region);
    assert(!res_charlie);

    void* bob_ret;
    void* charlie_ret;
    res = pthread_join(handlers[1], &bob_ret);
    assert(!res);
    res = pthread_join(handlers[2], &charlie_ret);
    assert(!res);*/

    // TODO on free put yourself in access set

    /*
    bool     tm_end(shared_t, tx_t);
    bool     tm_read(shared_t, tx_t, void const*, size_t, void*);
    bool     tm_write(shared_t, tx_t, void const*, size_t, void*);
    alloc_t  tm_alloc(shared_t, tx_t, size_t, void**);
    bool     tm_free(shared_t, tx_t, void*);*/

    tm_destroy(shared_region);

}