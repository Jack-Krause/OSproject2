# OSproject2
Practice using threads and concurrency controls (locks, condition variables, semaphores) in C

## Overall Approach
This project heavily modeled the solution to the Producer-Consumer (Bounded Buffer) problem, used in Lecture 16. Here are the main topics of the solution, and how we used them here.

Overall, my implementation builds a three-stage pipeline, with each stage running its own thread. Each thread handles one part of the metadata journaling process, and the stages pass work to each other through three bounded circular buffers. 

To coordinate the concurrency, my approach was to use mutex locks and condition variables, rather than semaphores. Each circular buffer has its own mutex plus a pair of condition variables `not_empty` and `not_full`. This allowed me to implement the classic producer-consumer pattern:
* A thread blocks when the buffer it wants to read from is empty.
* A thread blocks when the buffer it wants to write to is full.
* No busy waiting is used.

We also added a second set of mutexes and condition variables (one per stage). A stage sets is "completion flags" to zero, issues the requests, and then waits on its stage's condition variable. When the block-service calls the corresponding `_complete()` function, we take the lock, set the flag, and signal the waiting thread. This guarantees that no stage moves forward until all of its required operations are done.

This combination of threads + locks + CVs gives us the exact behavior the spec requires:
* Threads block only when:
    * input buffers are empty\,
    * output buffers are full
    * requests are incomplete

For the testing section in (2.2), we added an optional delay, controlle dby a macro. 