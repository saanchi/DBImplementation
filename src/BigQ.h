#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <queue>

using namespace std;

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

private:
	pthread_t worker;		  // Only worker thread
	queue<Record> min_queue;  // in-memory priority queue for phase II of TPMMS
    File *X, *Y;              // Temporary files used for external sorting.
    vector<Page> run_buffer;  // In-memory buffer for each of the 'm' runs
    Page *output_buffer;      // Buffer to hold output from the multi-way merge.
    Pipe *in, *out;			  // Input pipe output pipe
    int runlen;				  // Run length of Pages to be kept in memory
    OrderMaker *sortorder;    // Order maker object
    vector<off_t> run_offset; // Store the offsets for the runs generated
};

	/**
     * Starting point for thread.
     * Reads data from the input pipe.
     * Sorts them using TPMMS
     * Writes to output buffer
     */
    void * run( void *arg);

    /**
	 * Read the sorted buffer into pages.
	 * Write the pages into disk
	 */
	void write_buffer( File *Y, vector<Record> buffer);

    /**
     * Implements Phase I of TPMMS.
     * Reads the pipe for #records equal to runlen in buffer.
     * Sort them, append the sorted run to FILE Y.
     * Repeat it till the pipe is exhausted.
     */
    void generate_runs( BigQ *bq );

    /**
     * Implements Phase II of TPMMS.
     * Initialize the in memory buffer corresponding to each run.
     * Initialize the priority queue with the head element of each run.
     * Keep extracting min from the queue and write to output buffer.
     *	If the run gets empty load from the disk.
     *	When the output buffer gets full write to disk.
     * Repeat till all the elements are sorted.
     */
    void multiway_merge( BigQ *bq );

    /**
     *  Initialize the in memory buffer for each run.
     *  Initialize the priority queue.
     */
    void initialize_buffers( BigQ *bq );


#endif
