#include <stdio.h>
#include <pthread.h>
#include <unistd.h> //  needed for sleep(1)
#include "journal.h"

/*
ACTIVATE_DELAY
FOR GRADERS:
switch this to 0 for default behavior

switch this to 1 for testing requirement in 2.2
1 delays first TXE to try to fill buffer 2
Note: this behavior seems to be non-deterministic. It may require several runs
for the buffer to fill up and show in the output.
*/
#define ACTIVATE_DELAY 1 // set to 0 for submission (default behavior)

void issue_journal_txb(int write_id) {
	printf("issue journal txb %d\n", write_id);
	journal_txb_complete(write_id);
}

void issue_journal_bitmap(int write_id) {
	printf("issue journal bitmap %d\n", write_id);
	journal_bitmap_complete(write_id);
}

void issue_journal_inode(int write_id) {
	printf("issue journal inode %d\n", write_id);
	journal_inode_complete(write_id);
}

void issue_write_data(int write_id) {
	printf("issue write data %d\n", write_id);
	write_data_complete(write_id);
}


#if ACTIVATE_DELAY
void *delayed_txe(void *arg) {
	int write_id = *(int *) arg;
	// sleep(1);
	sleep(10); 
	journal_txe_complete(write_id);
	return NULL;
}

void issue_journal_txe(int write_id) {
	printf("issue journal txe (modified) %d\n", write_id);

	static int ids[1024];
	static int first = 1;

	// only delay for the first txe
	if (first) {
		first = 0;
		ids[write_id] = write_id;
		pthread_t thread_a;
		pthread_create(&thread_a, NULL, delayed_txe, &ids[write_id]);
		pthread_detach(thread_a);
		// the above should cause buffer 2 to fill up and thread1 to be stuck waiting
		// journal_txe_complete(write_id);
	} else {
		journal_txe_complete(write_id);
	}
}
#else // ACTIVATE_TEST == 0
void issue_journal_txe(int write_id) {
	printf("issue journal txe %d\n", write_id);
	journal_txe_complete(write_id);
}
#endif

void issue_write_bitmap(int write_id) {
	printf("issue write bitmap %d\n", write_id);
	write_bitmap_complete(write_id);
}

void issue_write_inode(int write_id) {
	printf("issue write inode %d\n", write_id);
	write_inode_complete(write_id);
}
