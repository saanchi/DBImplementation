#include "BigQ.h"
#include "Defs.h"
#include <vector>
#include <algorithm>

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	if( in == NULL || out == NULL || runlen == 0 ){
		cerr << " Wrong input " << endl;
	}
	// initialize the member objects
	this->in  = in;
	this->out = out;
	this->runlen = runlen;
	X 		  	 = new File();
	Y 		  	 = new File();
	this->sortorder  = sortorder;
	output_buffer 	 = new Page<Record>();
	run_offset.push_back(1);	// Initialize for the first run
	pthread_create (&worker, NULL, run, (void *)this );
}

BigQ::~BigQ () {
}

void * run( void *arg ){

	BigQ *bq = (BigQ *) arg;
	generate_runs(bq);   // Phase I of TPMMS
	initialize_buffers(bq);
	multiway_merge(bq);    // Phase II  of TPMMS
    // finally shut down the out pipe
	bq->out->ShutDown();
}

void generate_runs( BigQ *bq ){
// read data from in pipe sort them into runlen pages
	Record rec;
	Page page;
	vector<Record> buffer;
	int count   = 0; 					// keep track of number of pages
	off_t run_num = 0;					// keep track of number of runs
	while( bq->in->Remove(&rec)){  		// keep reading from the pipe
		buffer.push_back(rec);          // Put record in buffer
		int status = page.Append(&buffer.back()); // Try adding the record to page
		if( status == 0){				// if page is full
			count++;
			page = NULL;
			if( count == bq->runlen ){		 // if records equal to run length of pages
				off_t page_offset = bq->Y->GetLength();  // Get the current offset of the file
				bq->run_offset.push_back(page_offset);   // Push the offset for the next run
				sort(buffer.begin(), buffer.end(), bq->sortorder); // sort the buffer
				write_buffer( bq->Y, buffer );	 // write the records from buffer into a file
				count = 0;					     // initialize the count to 0
			}
			page = new Page();
			page.Append(&rec);
		}
	}
	// Sort and write the last run
	sort(buffer.begin(), buffer.end(), bq->sortorder); // sort the buffer
	write_buffer( bq->Y, buffer );	 				   // write the records from buffer into a file
}

void write_buffer( File *Y, vector<Record> buffer){
	Page page = new Page();
	Record rec;
	for( vector<int>::iterator itr = buffer.begin(); itr != buffer.end(); ++itr){
		rec = buffer.pop_back();
		int status = page.Append(&rec);
		if( status == 0 ){ 					// if page is full
			off_t offset = Y->GetLength();
			Y->AddPage(&page, ++offset); 	// write the page into file
			page = new Page();
		}
	}
}

void initialize_buffers( BigQ *bq){
	for( vector<int>::iterator itr = bq->run_offset.begin(); itr != bq->run_offset.end(); ++itr){


	}
}

void multiway_merge(BigQ *bq){

}



