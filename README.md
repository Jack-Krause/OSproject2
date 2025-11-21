# OSproject2
Practice using threads and concurrency controls (locks, condition variables, semaphores) in C

## Overall Approach
This project heavily modeled the solution to the Producer-Consumer (Bounded Buffer) problem from Lecture 16. My implementation builds a three-stage pipeline, with each stage running its own thread. Each thread handles one part of the metadata journaling process, and the stages pass work to each other through three bounded circular buffers. 

To coordinate the concurrency, I used pthread mutex locks and condition variables, not semaphores. Each circular buffer has its own mutex plus a pair of condition variables `not_empty` and `not_full`. This allowed me to implement the classic producer-consumer pattern:
* A thread blocks when the buffer it wants to read from is empty.
* A thread blocks when the buffer it wants to write to is full.
* No busy waiting is used.

Here is an example from the code, where items are safely consumed:
```
pthread_mutex_lock(&buff->lock);
while (buff->count == 0) {
    pthread_cond_wait(&buff->not_empty, &buff->lock);
}
int id = buff->buffer[buff->head];
buff->head = (buff->head + 1) % BUFFER_SIZE;
buff->count--;
pthread_cond_signal(&buff->not_full);
pthread_mutex_unlock(&buff->lock);
```
And here is the matching producer pattern:
```
pthread_mutex_lock(&buff->lock);
while (buff->count == BUFFER_SIZE) {
    pthread_cond_wait(&buff->not_full, &buff->lock);
}
buff->buffer[buff->tail] = write_id;
buff->tail = (buff->tail + 1) % BUFFER_SIZE;
buff->count++;
pthread_cond_signal(&buff->not_empty);
pthread_mutex_unlock(&buff->lock);
```

We also added a second set of mutexes and condition variables (one per pipeline stage). Each stage resets its “completion flags,” issues its I/O requests, and then waits on its stage’s condition variable. The block-service completes the cycle by acquiring the same lock, updating the relevant flag, and signaling the waiting thread. This ensures correct ordering and prevents race conditions — each flag is only read or written while holding its stage’s lock.

This locking discipline prevents race conditions: 
* Threads block only when:
    * input buffers are empty,
    * output buffers are full, or
    * I/O operations have not yet completed.

For the testing requirement in section 2.2, we added an optional delay controlled by a macro. When enabled, the first `TxE` completion is intentionally slowed down, causing buffer 2 to fill and forcing Thread 1 to block on a full buffer. In this mode, our program prints a one-time message such as:

```---2.2 TEST: thread stuck because of full buffer---```

This confirms the delay mechanism works as intended. Here is a long-form example of this testing functionality output:

```
test1
NOTE: testing functionality (2.2) is active.
-- Disable this functionality by setting ACTIVATE_DELAY => 0
-- in journal.c and block_service.c
requesting test write 0
requesting test write 1
requesting test write 2
requesting test write 3
requesting test write 4
requesting test write 5
requesting test write 6
requesting test write 7
requesting test write 8
issue write data 0
issue journal txb 0
issue journal bitmap 0
issue journal inode 0
requesting test write 9
issue write data 1
issue journal txb 1
issue journal bitmap 1
issue journal inode 1
issue write data 2
issue journal txb 2
issue journal bitmap 2
issue journal inode 2
issue write data 3
issue journal txb 3
issue journal bitmap 3
issue journal inode 3
issue write data 4
issue journal txb 4
issue journal bitmap 4
issue journal inode 4
issue write data 5
issue journal txb 5
issue journal bitmap 5
issue journal inode 5
issue write data 6
issue journal txb 6
issue journal bitmap 6
issue journal inode 6
issue write data 7
issue journal txb 7
issue journal bitmap 7
requesting test write 10
issue journal inode 7
requesting test write 11
requesting test write 12
issue write data 8
requesting test write 13
issue journal txb 8
issue journal txe (modified) 0
requesting test write 14
issue journal bitmap 8
issue journal inode 8
issue write data 9
issue journal txb 9
issue journal bitmap 9
issue journal inode 9
---2.2 TEST: thread stuck because of full buffer---
requesting test write 15
```