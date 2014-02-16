#include "BigQ.h"
#include "Defs.h"
#include <vector>
#include <algorithm>
#include <utility>
#include <queue>
#include <iterator>

PairComparison :: PairComparison(){
        this->comparison_engine = new ComparisonEngine;
    }
bool PairComparison:: operator()(P const& a, P const& b) const{
	Record r1 = a.first;
	Record r2 = b.first;
    return comparison_engine->Compare( &r1, &r2, &sortorder) <= 0;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): in(in), out(out), sortorder(sortorder) {

    if( runlen == 0 ){
        cerr << " Wrong input " << endl;
    }
    // initialize the member objects
    this->runlen = runlen;
    Y            = new File();
    pthread_create (&worker, NULL, run, (void *)this );
}

BigQ::~BigQ () {
}

void * run( void *arg ){
    BigQ *bq = (BigQ *) arg;
    sortorder = bq->sortorder;
    generate_runs(bq);     // Phase I of TPMMS
    multiway_merge(bq);    // Phase II  of TPMMS
    bq->out.ShutDown();   // finally shut down the out pipe
}

int compare(Record r1, Record r2)
{
	ComparisonEngine * compEngine = new ComparisonEngine;
	return compEngine->Compare(&r1,&r2,&sortorder)<=0;
}
void generate_runs( BigQ *bq ){
// read data from in pipe sort them into runlen pages
	cout << " Entered generate run " << endl;
    Record rec;
    Page *page = new Page();
    vector<Record> buffer;
    int count     = 0;                     // keep track of number of pages
    while( bq->in.Remove(&rec)){           // keep reading from the pipe
        cout << " Read record " << endl;
    	buffer.push_back(rec);             // Put record in buffer
        int status = page->Append(&rec);   // Try adding the record to page
        if( status == 0){                  // if page is full
            count++;
            page = NULL;
            if( count == bq->runlen ){         // if records equal to run length of pages
                off_t page_offset = bq->Y->GetLength();  // Get the current offset of the file
                bq->run_offset.push_back(page_offset);   // Push the offset for the current run
                sort(buffer.begin(), buffer.end(), compare); // sort the buffer
                write_buffer( bq->Y, buffer );     // write the records from buffer into a file
                count = 0;                         // initialize the count to 0
                buffer.clear();                    // clear the vector
                cout << "Count : " << count << endl;
            }
            page = new Page();
            rec  = buffer.back();
            page->Append(&rec);
        }
    }
    cout << " End of generate runs " << endl;
    // File can end before a page is completely filled. Sort and write the last run
    sort(buffer.begin(), buffer.end(), compare);    // sort the buffer
    write_buffer( bq->Y, buffer );                        // write the records from buffer into a file
    buffer.clear();
}

void write_buffer( File *Y, vector<Record> &buffer){
    Page *page = new Page();
    Record rec;
    // Iterate through the records and add to page
    for( vector<Record>::iterator itr = buffer.begin(); itr != buffer.end(); ++itr){
    	rec = *itr;
        int status = page->Append(&rec);
        if( status == 0 ){                     // if page is full
            off_t offset = Y->GetLength();
            Y->AddPage(page, ++offset);        // write the page into file
            page = new Page();
        }
    }
}

void multiway_merge(BigQ *bq){
    priority_queue<P, vector<P>, PairComparison> pq;
    Page *page;
    // Initialize the buffers and the priority queue.
    // Fetch the corresponding first pages from the File into the buffer
    for( int i =0; i < bq->run_offset.size(); i++){
        bq->Y->GetPage( page, bq->run_offset[i]);        // Fetch the page
        bq->run_buffer[i] = page;                        // Put in the buffer
        bq->run_offset[i]++;                             // increase the offset of the current buffer
        Record rec ;
        page->GetFirst(&rec);                            // Fetch the first record from the page
        P pr(rec, i);                                    // Create a pair
        pq.push(pr);                                     // Insert the pair into priority queue
    }

    // Do the merge.
    page = NULL;
    while( !pq.empty()){
        P pr = pq.top();                                 // Extract the top pair from the queue
        Record rec = pr.first;                           // Get the record
        bq->out.Insert(&rec);                            // insert the record into the output pipe
        int index  = pr.second;                          // Get the buffer index
        page       = bq->run_buffer[index];              // Get the buffered page from the index
        Record new_record;
        // Try to fetch the next record of the removed page from the buffer
        if( !page->GetFirst(&new_record)){               // if the page is empty
            off_t curr_run_offset = bq->run_offset[index];
            // check if the current run's runlen pages have not been read or
            // current run is the end run and not yet finished
            if( curr_run_offset% bq->runlen != 0 || ( curr_run_offset != bq->Y->GetLength())){
                page = new Page;
                bq->Y->GetPage(page, curr_run_offset+1);    // Fetch the page
                delete bq->run_buffer[index];               // delete the existing buffer from the memory
                bq->run_buffer[index] = page;               // Place the fetched page into buffer
                bq->run_offset[index]++;                    // increase the counter
            }
            else{ // if the run is finished or reached end of file
                  // Do nothing with the priority queue
                delete &new_record;
                continue;
            }
        }
        pr = make_pair(new_record, index);                  // create a new pair
        pq.push(pr);                                   // push the new pair into queue
    }

}
