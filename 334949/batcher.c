#include "batcher.h"


#define NUMBER_OF_THREAD 5

struct batcher batcher;

// struct batcher {
//     struct lock_t lock;     //contains mutex and cv
//     atomic_int epoch;
//     atomic_int remaining;
//     atomic_int blocked;
//     int tx_id;
// };

void batcher_init(struct batcher* batcher){
    lock_init(&(batcher->lock_total));
    //lock_init(&(batcher->lock_cm));
    //pthread_mutex_init(&(batcher->lock_cm), NULL);
    batcher->epoch = 0;
    batcher->total_remaining = 0;
    batcher->cm_remaining = 1;
    batcher->blocked = 0;
    batcher->tx_id = 0;
}

void batcher_destory(struct batcher* batcher){
    lock_cleanup(&(batcher->lock_total));
    //lock_cleanup(&(batcher->lock_cm));
    //pthread_mutex_destroy(&(batcher->lock_cm));
}

//int batcher_enter(struct batcher* batcher, int index){
int batcher_enter(struct batcher* batcher){
    //int total_expected = 0;
    int ret = 1;
    lock_acquire(&(batcher->lock_total));
    if(batcher->total_remaining == 0){
        //printf("======================EPOCH %d START========================\n", batcher->epoch+1);
        batcher->total_remaining++;
        //printf("[ENTER] Thread %d is the only thread of this batch\n", index);
        //lock_acquire(&(batcher->lock_cm));
        batcher->cm_remaining = 1;
        //lock_release(&(batcher->lock_cm));
        //printf("[ENTER] Thread %d enter directly\n", ret);
        //atomic_compare_exchange_strong(&(batcher->cm_remaining), &cm_expected, 1);
            //remaining is not 0
        lock_release(&(batcher->lock_total));
    } else {
        
        //remaining is not 0
        
        //printf("[COND_WAITING] Thread %d is waiting for the cond_var total\n", index);
        batcher->blocked++;
        //printf("***[TRANS_WAIT]*** %d trans waiting\n", batcher->blocked);
        lock_wait(&(batcher->lock_total));
        //printf("======================EPOCH %d START========================\n", batcher->epoch+1);
        
        batcher->tx_id++;
        ret = batcher->tx_id;
        //printf("[WAKE_UP] Thread %d\n", ret);
        //printf("[ENTER] Thread %d wake up and enter\n", ret);
        lock_release(&(batcher->lock_total));
    }
    
    return ret; 
}

//bool batcher_before_commit(struct batcher* batcher, int index){
bool batcher_before_commit(struct batcher* batcher){
    //return true -> is the last one of this batch -> need to commit
    //return false -> not the last one of this batch


    return (atomic_fetch_sub(&(batcher->cm_remaining), 1) == 1);


}

//void batcher_leave(struct batcher* batcher, int index){
void batcher_leave(struct batcher* batcher){
    

    lock_acquire(&(batcher->lock_total));
    if(batcher->total_remaining == 1){

        batcher->total_remaining--;
        batcher->epoch++;
        batcher->total_remaining = batcher->blocked;
        batcher->cm_remaining = batcher->blocked;
        batcher->blocked = 0;
        batcher->tx_id = 0;
        lock_wake_up(&(batcher->lock_total));
        //printf("[ENTER] Thread %d is leaving\n", index);
        //printf("[LEAVE] Thread %d is leaving\n", batcher->tx_id);
        //printf("[LEAVE] Thread %d is the last one to leave, epoch = %d, total_remaining = %d\n", index, batcher->epoch, batcher->total_remaining);
        //printf("======================EPOCH %d END========================\n", batcher->epoch);
        //printf("[LEAVE] Thread %d is the last one to leave, epoch = %d, total_remaining = %d\n", index, batcher->epoch, batcher->total_remaining);
        lock_release(&(batcher->lock_total));
    }else{
        batcher->total_remaining--;
        //printf("[DO NOTHING] Thread %d\n", index);
        lock_release(&(batcher->lock_total));
    }
    
    // if(atomic_fetch_sub(&(batcher->total_remaining), 1) == 1){
    //     lock_acquire(&(batcher->lock_total));
        
        
    //     batcher->epoch++;
    //     batcher->total_remaining = batcher->blocked;
    //     batcher->cm_remaining = batcher->blocked;
        
    //     lock_wake_up(&(batcher->lock_total));
    //     //printf("[ENTER] Thread %d is leaving\n", index);
    //     printf("[ENTER] Thread %d is leaving\n", batcher->tx_id);
    //     printf("======================EPOCH %d END========================\n", batcher->epoch);
    //     batcher->blocked = 0;
    //     batcher->tx_id = 0;
        
        
    //     //printf("[LEAVE] Thread %d is the last one to leave, epoch = %d, total_remaining = %d\n", index, batcher->epoch, batcher->total_remaining);
        
    //     lock_release(&(batcher->lock_total));
    // }else{
    //     //printf("[DO NOTHING] Thread %d\n", index);
    // }
}


/*
void* batch_op(void* arg){
    
    int index = *(int*)arg;

    int ret = batcher_enter(&batcher, index);
    bool is_last = batcher_before_commit(&batcher, index);
    
    batcher_leave(&batcher, index);
   
    
    
    // int ret = batcher_enter(&batcher);
    // batcher_before_commit(&batcher);
    // batcher_leave(&batcher);

    

    free(arg);

}


int main(){
    pthread_t th[NUMBER_OF_THREAD];  

    batcher_init(&batcher);
    int res;


    for(int i = 0; i < NUMBER_OF_THREAD; i++){
        int* a = malloc(sizeof(int));
        *a = i;
        
        if(i == 3)  sleep(2);
        
        res = pthread_create(&th[i], NULL, batch_op, a);
        assert(!res);
        
    }
    for(int i = 0; i < NUMBER_OF_THREAD; i++){
        res = pthread_join(th[i], NULL);
        assert(!res);
    }

    batcher_destory(&batcher);


    return 0;
}

*/