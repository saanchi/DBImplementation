#include "BigQ.h"
#include "Defs.h"
#include <vector>
#include <algorithm>
#include <utility>
#include <queue>
#include <iterator>
#include <string.h>
#include <cmath>

PairComparison :: PairComparison(){
        this->comparison_engine = new ComparisonEngine;
    }
bool PairComparison:: operator()(P const& a, P const& b) const{
	Record *r1 = a.first;
	Record *r2 = b.first;
    return comparison_engine->Compare( r1, r2, &sortorder) <= 0;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): in(in), out(out), sortorder(sortorder) {

    if( runlen == 0 ){
        cerr << " Wrong input " << endl;
    }
    // initialize the member objects
    this->runlen = runlen;
    Y            = new File();
    Y->Open(0,"tmp.bin");
    cout<< " offset "<<Y->GetLength()<<endl;
    pthread_create (&worker, NULL, run, (void *)this );
}

BigQ::~BigQ () {
	pthread_join(worker, NULL);
}

void * run( void *arg ){
    BigQ *bq = (BigQ *) arg;
    sortorder = bq->sortorder;
    generate_runs(bq);     // Phase I of TPMMS
    bq->Y->Open(1,"tmp.bin");
    multiway_merge(bq);    // Phase II  of TPMMS
    bq->Y->Close();
    bq->out.ShutDown();   // finally shut down the out pipe
}

int compare(const void *r1,const void* r2){
	ComparisonEngine* compEngine = new ComparisonEngine();
	return (compEngine->Compare((Record*)r1,(Record*)r2,&sortorder));
}

void generate_runs( BigQ *bq ){
	cout << " Generating Runs " << endl;
    Record *rec = new Record;
    Page *page  = new Page();
    vector<Record*> buffer;
    int count     = 0;                         // keep track of number of pages
    while( bq->in.Remove(rec) != 0){           // keep reading from the pipe
        //cout << "Reading Records" << endl;
        Record *dup_record = new Record ;
        dup_record->Copy(rec);                 // Creating a copy of the fetched record
        int status = page->Append(rec);   // Try adding the record to page
        if( status == 0){                  // if page is full
            cout << " Page is Full " << endl;
        	count++;
            if( count == bq->runlen ){         // if records equal to run length of pages
                qsort(buffer[0], buffer.size(), sizeof(Record *), compare); // sort the buffer
                write_buffer( bq->Y, buffer );     // write the records from buffer into a file
                count = 0;                         // initialize the count to 0 for next run
                for( std::vector<Record*>::iterator i = buffer.begin(), endI = buffer.end(); i != endI; ++i){
                   delete *i;
                }
                buffer.clear();                    // clear the vector
            }
            page->EmptyItOut();              // Empty the current page
            page->Append(rec);
        }
        buffer.push_back(dup_record);             // Put record in buffer
    }
    // File can end before a page is completely filled. Sort and write the last run
    qsort(buffer[0], buffer.size(), sizeof(Record *), compare);
    write_buffer( bq->Y, buffer );                        // write the records from buffer into a file
    buffer.clear();
    cout << " End of generate runs " << endl;
}

void write_buffer( File *Y, vector<Record *> &buffer){
    Page *page = new Page;
    Record *rec;
    // Iterate through the records and add to page
    cout<<" Writing the buffer into File "<<endl;
    for( vector<Record *>::iterator itr = buffer.begin(); itr != buffer.end(); ++itr){
    	//cout<<" Iterating over the records in the buffer "<<endl;
    	rec = *itr;
        int status = page->Append(rec);
        if( status == 0 ){                     // if page is full
        	off_t offset = Y->GetLength();
            Y->AddPage(page, offset);          // write the page into file
            page->EmptyItOut();
            page->Append(rec);
        }
    }
    off_t offset = Y->GetLength();
    Y->AddPage(page, offset);        // write the page into file
    page->EmptyItOut();
    buffer.clear();
}

void multiway_merge(BigQ *bq){
	cout<<" Entering 2nd phase "<<endl;
	cout<<" Initializing the buffers and priority queue"<<endl;
    priority_queue<P, vector<P>, PairComparison> pq;
    vector<off_t> run_offset;
    int file_len = bq->Y->GetLength()-1;
    int num_runs = ceil(file_len*1.0f/bq->runlen);
    Page *run_buffer = new Page[num_runs];
    cout<<" File Length "<<file_len<<" Num Runs "<<num_runs<<endl;
    for(int j=0; j<file_len; j+=bq->runlen ){
    	run_offset.push_back(j);
	}
    // Initialize the buffers and the priority queue.
    // Fetch the corresponding first pages from the File into the buffer
    for( int i =0; i < run_offset.size(); i++){
        cout <<" Initializing  Run " << " " << i << " with page "<< run_offset[i];
    	bq->Y->GetPage( &run_buffer[i], run_offset[i]);        // Fetch the page
    	run_offset[i]++;                             // increase the offset of the current buffer
        Record *rec = new Record;
        run_buffer[i].GetFirst(rec);                            // Fetch the first record from the page
        P pr = make_pair(rec,i);                                    // Create a pair
        pq.push(pr);                                     // Insert the pair into priority queue
    }
    cout<< "Done with initialization " << endl;
    // Do the merge.
    Record *rec;
    while( !pq.empty()){
        P pr = pq.top();                                  // Extract the top pair from the queue
        pq.pop();
        rec = pr.first;			                          // Get the record
        bq->out.Insert(rec);                              // insert the record into the output pipe
        int index  = pr.second;                           // Get the buffer index
        // Try to fetch the next record of the removed page from the buffer
        Record *new_record = new Record;
        if( run_buffer[index].GetFirst(new_record) == 0){                // if the page is empty
            off_t curr_run_offset = run_offset[index];
            // check if the current run's runlen pages have not been read or
            // current run is the end run and not yet finished
            if( curr_run_offset% bq->runlen != 0 && ( curr_run_offset != file_len)){
                bq->Y->GetPage(&run_buffer[index], curr_run_offset);    // Fetch the page
                run_offset[index]++;                    // increase the counter
                if( run_buffer[index].GetFirst(new_record) == 0 ){
                	continue;
                }
            }
            else{ // if the run is finished or reached end of file
                  // Do nothing with the priority queue
                continue;
            }
        }
        pr = make_pair(new_record, index);                  // create a new pair
        pq.push(pr);                                   // push the new pair into queue
    }
}

