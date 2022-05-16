/**
 * @file   tm.c
 * @author [...]
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
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
// Internal headers
#include <tm.h>
//#include "tm.h"

#include "macros.h"
#include "batcher.h"

#define MAX_THREAD 30
#define MAX_DIRTY_WORD 1000
#define ACCESS_BY_MULTIPLE 2147483647



/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/

struct transaction {
    atomic_int id;     //from batcher 
    bool is_ro;
    bool aborted;
    atomic_int next_dirty_id;
    struct word* dirty_words[MAX_DIRTY_WORD];
};

struct word {   
    pthread_mutex_t mutex;
    atomic_int readable;    // 0 -> copyA is readable(latest), copyB is writable(old)
                            // 1 -> copyB is readable(latest), copyA is writable(old)
    bool written;           // has it been written or not
    atomic_int access_set;  // access by which transaction
    int64_t copyA;        //in 64 bits machine, void* is 8 bytes
    int64_t copyB;
};

struct segment {
    size_t size;
    //struct word *words;  
};

struct region {
    //void *start;        // Start of the shared memory region (i.e., of the non-deallocable memory segment)
    size_t size;        // Size of the non-deallocable memory segment (in bytes)
    size_t align;       // Size of a word in the shared memory region (in bytes)
    struct batcher batcher;

    // Next id that is available. (2 ~ 65535)
    // id = 1 is reserved for the non-deallocable memory segment
    atomic_int next_allocable_id; 
    //pthread_mutex_t trans_mutex;
    atomic_int total_trans;
    
    struct transaction trans_pool[MAX_THREAD];
    struct segment* segment_table[65536];   //max id = 2^16
};

struct segment* allocate_segment(size_t size, size_t align){
    // [FINISH]
    //printf("[ALLOCATE_SEGMENT]\n");
    align = align < 8 ? align : 8;  // if align = 16 -> store it as 8

    int num_of_words = size / align;
    size_t actual_size = sizeof (struct segment) + sizeof(struct word)*num_of_words;
    struct segment* segment = malloc(actual_size);

    memset(segment, 0, actual_size);

    //init word lock
    //printf("[ALLOCATE_SEGMENT] num of words = %d\n",num_of_words);
    struct word* src_word = ((struct word*) (segment+1));
    for(int i = 0; i < num_of_words; i++){
        if(pthread_mutex_init(&(src_word->mutex), NULL) != 0){
            printf("Alloc word mutex error\n");
        }
        
        src_word += 1;
    }
    

    segment->size = size;
    return segment;
}

shared_t tm_create(size_t size, size_t align) {
    // [FINISH] TODO: tm_create(size_t, size_t)
    struct region* region = (struct region*) malloc(sizeof(struct region));
    if (unlikely(!region)) {
        return invalid_shared;
    }

    region->size        = size;
    region->align       = align;
    region->next_allocable_id = 2;   
    region->total_trans = 0;
    //pthread_mutex_init(&(region->trans_mutex), NULL);
    //don't need to init trans_pool;
    //memset(region->segment_table, 0, sizeof(struct segment*)* 65536);
    region->segment_table[1] = (struct segment*)allocate_segment(size, align);
    batcher_init(&region->batcher);
    
    return region;
}


/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    //[FINISH] TODO: tm_destroy(shared_t)
    struct region* region = (struct region*) shared;
    //struct segment* this_segment = region->segment_table[0];
    batcher_destory(&(region->batcher));

    //Destory all Lock of words
    
    for(int i = 1; i < region->next_allocable_id; i++){
        struct segment* ptr_segment = region->segment_table[i];
        //printf("[TM_DESTORY] segment %d\n", i);
        size_t word_size = region->align < 8 ? region->align : 8; 
        int num_of_words = (ptr_segment->size) / word_size;  
        struct word* src_word = (struct word*) (ptr_segment+1);
        
        //printf("num of words = %d\n",num_of_words);
        for(int j = 0; j < num_of_words; j++){
            pthread_mutex_destroy(&(src_word->mutex)); 
            src_word += 1;
        }
        free(ptr_segment);
    }
    
    

    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) {
    // [FINISH] TODO: tm_start(shared_t)
    // first 16 bits -> id, following 48 bits -> offset
    int64_t segment_id = 1; 
    int64_t start_from = 0;
    int64_t result = (segment_id << 48) + start_from;
    return (void*)result;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    // [FINISH] TODO: tm_size(shared_t)
    return ((struct region*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    // [FINISH] TODO: tm_align(shared_t)
    return ((struct region*) shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    // TODO: tm_begin(shared_t)
    
    //printf("[TM_BEGIN]\n");
    // SOMETHINGS GO WRONG HERE!!
    struct region* region = (struct region*) shared;
    
    //atomic_int total_trx = region->total_trans;
    
    //printf("total_trx = %d, region->total_trans = %d\n", total_trx, region->total_trans);
    
    atomic_int id = batcher_enter(&(region->batcher));  //id at least 1
    region->trans_pool[id].id = id;
    
    (region->trans_pool[id]).is_ro = is_ro;
    (region->trans_pool[id]).aborted = false;
    (region->trans_pool[id]).next_dirty_id = 0;
    memset((region->trans_pool[id]).dirty_words, 0, sizeof((region->trans_pool[id]).dirty_words));

    // if(is_ro){
    //     //printf("Read_Only Transaction id = %d\n", id);
    //     //transaction
    // }else{
    //     //printf("Read_Write Transaction id = %d\n", id);
    //     //transaction
    // }

    atomic_fetch_add(&(region->total_trans), 1);

    return (tx_t)(&region->trans_pool[id]);
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t unused(tx)) {
    // TODO: tm_end(shared_t, tx_t)
    struct region* region = (struct region*) shared;
    //struct transaction* this_trx = (struct transaction*) tx;
    //printf("Start to commit\n");

    if(batcher_before_commit(&(region->batcher))){
        // commit

        for(int id = 1; id <= region->total_trans; id++){
            struct transaction* trx = &(region->trans_pool[id]);
            //printf("Trans %d, total dirty words = %d\n", id, trx->next_dirty_id);
            //printf("trx id = %d, next_dirty_word = %d\n", trx->id, trx->next_dirty_id);
            for(int i = 0; i < trx->next_dirty_id; i++){
                struct word* ptr_word = trx->dirty_words[i];

                //pthread_mutex_lock(&(ptr_word->mutex));
                if(!trx->aborted && ptr_word->written){
                    //swap read/write
                    if(ptr_word->readable == 0)  ptr_word->readable = 1;
                    else ptr_word->readable = 0;
                }
                ptr_word->access_set = 0;
                ptr_word->written = false;
                //pthread_mutex_unlock(&(ptr_word->mutex));
            }
            trx->next_dirty_id = 0;
        }
        region->total_trans = 0;
        //pthread_mutex_unlock(&(region->trans_mutex));
        
        //printf("REACH HERE\n");
    }else{
        //Not the last to leave -> don't need to commit
    }
    

    batcher_leave(&(region->batcher));
    
    return true;
    // if(this_trx->aborted){
    //     return false;
    // }else{
    //     return true;   
    // }
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
    //[FINISH] TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    //printf("Start to read\n");
    // source = tm_start(shared)+align*n to access the (n+1)-th word
    struct region* region = (struct region*) shared;
    struct transaction* trx = (struct transaction*) tx;
    
    
    size_t word_size = region->align < 8 ? region->align : 8; 

    int64_t segment_id = (int64_t)source >> 48;
    struct segment *ptr_segment = region->segment_table[segment_id];
    
    int64_t word_offset = (((int64_t)source << 16) >> 16);  //word index

    if(word_offset != 0){
        word_offset /= word_size;
    }
    
    int num_of_words = size / word_size;     //word index -> 0 ~ num_of_words-1
    //printf("Read number of words = %d\n", num_of_words);

    struct word* src_word = ((struct word*) (ptr_segment+1)) + word_offset;  //ptr_segment + 1 => ptr to the first word of the segment
    char* target_byte = (char*) target;

    //printf("[TM_READ] %p\n", src_word);
    int64_t *readable = NULL;
    int64_t *writable = NULL;

    
    for(int i = 0; i < num_of_words; i++){
        
        if(src_word->readable == 0){
            readable = &(src_word->copyA);
            writable = &(src_word->copyB);
            //printf("Readable = A its address is %p, Writable = B its address is %p\n", &(src_word->copyA), &(src_word->copyB));
        }else{
            readable = &(src_word->copyB);
            writable = &(src_word->copyA);
            //printf("Readable = B its address is %p, Writable = A its address is %p\n", &(src_word->copyB), &(src_word->copyA));
        }
        
        
        if(trx->is_ro){
            
            //printf("[BEFORE READ ONLY]\n");
            memcpy(target_byte, readable, word_size);
            //printf("[READ ONLY] word = %p\n", src_word);
            
        }else{  //trx is read_write transaction
            
            if(unlikely(pthread_mutex_lock(&(src_word->mutex)))){
                printf("ERROR! LOCK\n");
            }
            if(src_word->written){     //has been written by other in the epoch
                //printf("Written? %d\n", src_word->written);
                
                if(src_word->access_set == trx->id){    
                    
                    //the transaction is already in the "access set"
                    //printf("[BEFORE READ] Has been written by me\n");
                    memcpy(target_byte, writable, word_size);
                    //printf("[READ THE WRITTEN WORD (BY ME)] Thread %d Seg id = %d Offset = %d\n",trx->id,segment_id, word_offset+i); 
                    
                    if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                        printf("ERROR! UNLOCK\n");
                    }
                }else{
                    //printf("my id = %d, ptr access_set = %p\n",trx->id, &(src_word->access_set));
                    trx->aborted = true;
                    //printf("[READ ABORT] Thread %d Seg id = %d Offset = %d\n",trx->id,segment_id, word_offset+i); 
                    
                    if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                        printf("ERROR! UNLOCK\n");
                    }
                    tm_end(region, trx);
                    
                    return  false;
                }
            }else{  //[READ_WRITE] Hasn't been written
                //printf("[BEFORE READ] Has not been written\n");
                memcpy(target_byte, readable, word_size);
                //printf("[READ THE UNWRITTEN WORD] Thread %d Seg id = %d Offset = %d\n",trx->id,segment_id, word_offset+i); 

                if(src_word->access_set == 0){
                    //pthread_mutex_lock(&(region->trans_mutex));
                    src_word->access_set = trx->id;
                    
                    trx->dirty_words[trx->next_dirty_id++] = src_word;
                    //trx->next_dirty_id++;
                    //pthread_mutex_unlock(&(region->trans_mutex));
                    //printf("[ADD TO DIRTY LIST]\n");
                } else if(src_word->access_set != trx->id){
                    src_word->access_set = ACCESS_BY_MULTIPLE;
                    //printf("[THIS WORD HAS BEEN READ BEFORE, DON'T NEED TO ADD TO DIRTY LIST]\n");
                    // access is not empty -> read_write trans has been read before -> don't need to add again
                } else {
                    //Do Nothing
                }

                if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                    printf("ERROR! UNLOCK\n");
                }
                

            
            }
            
            
        }
        src_word += 1;
        target_byte += word_size;
        
        
    }

    return true;
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
    //[FINISH] TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)

    struct region *region = (struct region*) shared;
    struct transaction* trx = (struct transaction*) tx;

    size_t word_size = region->align < 8 ? region->align : 8; 

    int64_t segment_id = (int64_t)target >> 48;
    //printf("Segment id = %d\n", segment_id);
    struct segment *ptr_segment = region->segment_table[segment_id];
    
    int64_t word_offset = (((int64_t)target << 16) >> 16);  //word index
    if(word_offset != 0){
        word_offset /= word_size;
    }
    
    int num_of_words = size / word_size;     //word index -> 0 ~ num_of_words-1
    //printf("Write number of words = %d\n", num_of_words);
    struct word* src_word = ((struct word*) (ptr_segment+1)) + word_offset;  //ptr_segment + 1 => ptr to the first word of the segment
    //printf("[TM_WRITE] %p\n", src_word);
    char* source_byte = (char*) source;
    
    //int64_t *readable = NULL;
    int64_t *writable = NULL;

    //printf("Start to write\n");
    
    for(int i = 0; i < num_of_words; i++){
        if(src_word->readable == 0){
            //readable = src_word->copyA;
            writable = &(src_word->copyB);
            //printf("Writable is B, its address is %p\n", writable);
        }else{
            //readable = src_word->copyB;
            writable = &(src_word->copyA);
            //printf("Writable is A, its address is %p\n", writable);

        }
        
        if(unlikely(pthread_mutex_lock(&(src_word->mutex)))){
            printf("ERROR! LOCK\n");
        }
        if(src_word->written){
            
            if(src_word->access_set == trx->id){    // the transaction is already in the "access set"
                
                memcpy(writable, source_byte, word_size); 
                //printf("[WRITE THE WORD WRITTEN BY ME] Thread %d Seg id = %d Offset = %d\n",trx->id,segment_id, word_offset+i); 
                if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                    printf("ERROR! UNLOCK\n");
                }
            }else{
                trx->aborted = true;
                //printf("[WRITE ABORT, CAN'T WRITE ON DIRTY COPY]] Thread %d Seg id = %d Offset = %d\n",trx->id,segment_id, word_offset+i); 
                
                if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                    printf("ERROR! UNLOCK\n");
                }
                tm_end(region, trx);
                
                return  false;
            }
        }else{  //Hasn't written before
            
            if(src_word->access_set != 0 && src_word->access_set != trx->id){ 
                //origin -> src_word->access_set != 0 (WRONG?)
                trx->aborted = true;
                //printf("[WRITE ABORT ACCESS SET IS SOMEONEELSE] Thread %d Seg id = %d Offset = %d\n",trx->id,segment_id, word_offset+i); 
                
                if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                    printf("ERROR! UNLOCK\n");
                }
                tm_end(region, trx);
                
                return  false;
            }else{
                memcpy(writable, source_byte, word_size);   
                //printf("[WRITE THE UNWRITTEN WORD] Thread %d Seg id = %d Offset = %d\n",trx->id,segment_id, word_offset+i); 
                

                src_word->access_set = trx->id;
                src_word->written = true;
                
                trx->dirty_words[trx->next_dirty_id++] = src_word;
                //trx->next_dirty_id++;
                if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                    printf("ERROR! UNLOCK\n");
                }
                // if(src_word->access_set != trx->id || src_word->access_set == 0){
                //     src_word->access_set = trx->id;
                //     src_word->written = true;
                    
                //     trx->dirty_words[trx->next_dirty_id] = src_word;
                //     trx->next_dirty_id++;
                //     if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                //         printf("ERROR! UNLOCK\n");
                //     }
                // }else{
                //     //already in access set & add to dirty word list

                //     src_word->written = true;
                //     if(unlikely(pthread_mutex_unlock(&(src_word->mutex)))){
                //         printf("ERROR! UNLOCK\n");
                //     }
                // }
                
            }
        }
        
        
        src_word += 1;
        source_byte += word_size;
    }

    return true;
}


/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void** target) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    //printf("[TM_ALLOC]\n");
    struct region* region = (struct region*) shared;
    struct transaction* trx = (struct transaction*) tx;
    size_t align = region->align;

    if(trx->aborted)    return abort_alloc;

    int64_t id = atomic_fetch_add(&region->next_allocable_id, 1);
    //printf("id = %d,  region->next_allocable_id = %d\n", id, region->next_allocable_id);

    region->segment_table[id] = (struct segment*)allocate_segment(size, align);


    //int64_t segment_id = region->next_allocable_id; 
    //int64_t start_from = 0;
    //int64_t result = (id << 48) + 0;
    *target = (void*)(id << 48);

    
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    //printf("[TM_FREE]\n");
    // TODO: tm_free(shared_t, tx_t, void*)
    return true;
}