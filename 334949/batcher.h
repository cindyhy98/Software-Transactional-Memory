#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h> 
#include <errno.h>

#include "lock.h"

#include <time.h>
#include <assert.h>
#include <unistd.h>

/*
struct batcher {
    struct lock_t tx_lock;
    struct lock_t cm_lock;
    atomic_int epoch;
    atomic_int tx;      //every thread is able to change this value
    atomic_int cm;      //every thread is able to change this value
};
*/

struct batcher {
    struct lock_t lock_total;     //contains mutex and cv
    struct lock_t lock_cm;     
    //pthread_mutex_t lock_cm;       //mutex
    atomic_int epoch;
    atomic_int total_remaining;
    atomic_int cm_remaining;
    atomic_int blocked;
    int tx_id;
};

void batcher_init(struct batcher* batcher);
void batcher_destory(struct batcher* batcher);
//int batcher_enter(struct batcher* batcher, int index);
//bool batcher_before_commit(struct batcher* batcher, int index);
//void batcher_leave(struct batcher* batcher, int index);
int batcher_enter(struct batcher* batcher);
bool batcher_before_commit(struct batcher* batcher);
void batcher_leave(struct batcher* batcher);
//void* batch_op(void* arg);



