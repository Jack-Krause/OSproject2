#include <stdio.h>
#include <pthread.h>
#include "journal.h"

int is_write_data_complete;
int is_journal_txb_complete;
int is_journal_bitmap_complete;
int is_journal_inode_complete;
int is_journal_txe_complete;
int is_write_bitmap_complete;
int is_write_inode_complete;


// add a struct for the circular buffer
// requirements: BUFFER_SIZE (journal.h) -> fixed-size buffers
// implement as a simple array
typedef struct {
    int buffer[BUFFER_SIZE];
    int head;
    int tail;
    int count; // how many items currently
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} circ_bbuf_t;

static circ_bbuf_t buf1; // request-buffer
static circ_bbuf_t buf2; // journal metadata completed buffer
static circ_bbuf_t buf3; // journal commit completed buffer

static pthread_mutex_t stage1_lock;
static pthread_cond_t stage1_cond;

/*
Thread 1: journal-metadata-write thread
Thread 1 should be an infinite loop that:
 1. waits for buffer 1 to not be empty
 2. removes the next request
 3. issues writing the requests data and
   3b. journaling the requests metadata
 4. waits for all the issues to complete
 5. puts the request into buffer 2 (waiting if nec.)
repeat.
*/
static pthread_t thread_1; //journal-metadate-write thread
/*
Thread 2: journal-commit-write-thread
Thread 2  should be an infinite loop that:
 1. waits for buffer 2 to not be empty
 2. removes the next request
 3. issues writing TxE to the journal
 4. waits for the issue to complete
 5. puts the requests into buffer 3 (waiting if nec.)
repeat.
*/
static pthread_t thread_2;
/*
Thread 3: checkpoint-metadata-thread
Thread 3 should be another infinite loop that:
 1. waits for buffer 3 to not be empty
 2. removes the next request
 3. issues writing the metadata
 4. waits for the issues to complete
 5. calls write_complete()
repeat.
*/
static pthread_t thread_3;


static int stage1_flags() {
    // pthread_mutex_lock(&stage1_lock);
    int finished_four = 
        is_write_data_complete + 
        is_journal_txb_complete + 
        is_journal_bitmap_complete + 
        is_journal_inode_complete;
    return finished_four == 4;
}

/*
For thread 1
*/
static void *journal_metadata_write_thread(void *arg) {
    while(1) {
        // buffer_get blocks until something is available
        int write_id = buffer_get(&buf1);

        // reset stage1 flags
        pthread_mutex_lock(&stage1_lock);
        is_write_data_complete = 0;
        is_journal_txb_complete = 0;
        is_journal_bitmap_complete = 0;
        is_journal_inode_complete = 0;
        pthread_mutex_unlock(&stage1_lock);

        // issue writing the request data
        issue_write_data(write_id);

        // issue journal request metadata
        issue_journal_txb(write_id);
        issue_journal_bitmap(write_id);
        issue_journal_inode(write_id);

        // wait for the thread to finish
        pthread_mutex_lock(&stage1_lock);
        while (!stage1_flags()) {
            pthread_cond_wait(&stage1_cond, &stage1_lock);
        }
        pthread_mutex_unlock(&stage1_lock);

        // put the request into buffer2 (waiting if nec.)
        buffer_put(&buf2, write_id);
    }
    return NULL;
}

static void *journal_commit_write_thread(void *arg) {
    while(1) {
        sleep(1);
    }

    return NULL;

}

static void *checkpoint_metadata_thread(void *arg) {
    while(1) {
        sleep(1);
    }

    return NULL;
}


// helper function(s) to follow specified producer/consumer pattern from the specifications 
static void buffer_put(circ_bbuf_t *buff, int write_id) {
    pthread_mutex_lock(&buff->lock);

    // when the buffer is full, wait until it is not full
    while (buff->count == BUFFER_SIZE) {
        pthread_cond_wait(&buff->not_full, &buff->lock);
    }

    buff->buffer[buff->tail] = write_id;
    buff->tail = (buff->tail + 1) % BUFFER_SIZE;
    buff->count++;

    pthread_cond_signal(&buff->not_empty);
    pthread_mutex_unlock(&buff->lock);
}

static int buffer_get(circ_bbuf_t *buff) {
    pthread_mutex_lock(&buff->lock);

    // when the buffer is empty, wait until it is not empty
    while (buff->count == 0) {
        pthread_cond_wait(&buff->not_empty, &buff->lock);
    }
    // continue with the assumption that the buffer isn't empty
    int val_head = buff->buffer[buff->head];
    buff->head = (buff->head + 1) % BUFFER_SIZE;
    buff->count--;

    pthread_cond_signal(&buff->not_full);
    pthread_mutex_unlock(&buff->lock);

    return val_head;
}

// helper method for initializing the circular buffers
static void init_buffer(circ_bbuf_t *buff) {
    buff->head = buff->tail = buff->count = 0;
    pthread_mutex_init(&buff->lock, NULL);
    pthread_cond_init(&buff->not_empty, NULL);
    pthread_cond_init(&buff->not_full, NULL);
}

/* This function can be used to initialize the buffers and threads.
 */
void init_journal() {
	// initialize buffers and threads here
    init_buffer(&buf1);
    init_buffer(&buf2);
    init_buffer(&buf3);

    pthread_mutex_init(&stage1_lock, NULL);
    pthread_cond_init(&stage1_cond, NULL);

    pthread_create(&thread_1, NULL, journal_metadata_write_thread, NULL);
    pthread_create(&thread_2, NULL, journal_commit_write_thread, NULL);
    pthread_create(&thread_3, NULL, checkpoint_metadata_thread, NULL);

}


// per-spec:
// request_write is called by the file system to enqueue a write request into the first buffer
void request_write(int write_id) {
    buffer_put(&buf1, write_id);
}

/* This function is called by the block service when writing the txb block
 * to persistent storage is complete (e.g., it is physically written to  
 * disk).
 */
void journal_txb_complete(int write_id) {
    pthread_mutex_lock(&stage1_lock);
    is_journal_txb_complete = 1;
    pthread_cond_signal(&stage1_cond);
    pthread_mutex_unlock(&stage1_lock);    
}

void journal_bitmap_complete(int write_id) {
    pthread_mutex_lock(&stage1_lock);
    is_journal_bitmap_complete = 1;
    pthread_cond_signal(&stage1_cond);
    pthread_mutex_unlock(&stage1_lock);    
}

void journal_inode_complete(int write_id) {
    pthread_mutex_lock(&stage1_lock);
    is_journal_inode_complete = 1;
    pthread_cond_signal(&stage1_cond);
    pthread_mutex_unlock(&stage1_lock);    
}

void write_data_complete(int write_id) {
    pthread_mutex_lock(&stage1_lock);
    is_write_data_complete = 1;
    pthread_cond_signal(&stage1_cond);
    pthread_mutex_unlock(&stage1_lock);    
}

void journal_txe_complete(int write_id) {
        is_journal_txe_complete = 1;
}

void write_bitmap_complete(int write_id) {
        is_write_bitmap_complete = 1;
}

void write_inode_complete(int write_id) {
        is_write_inode_complete = 1;
}


