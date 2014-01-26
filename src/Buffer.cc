/*
 * Buffer.cpp
 *
 *  Created on: Jan 24, 2014
 *      Author: sanchit
 */

#include "Buffer.h"
#include "math.h"

Buffer::Buffer() {
	num_page_used = 0;
	map = NULL;
}

Buffer::~Buffer() {

}

off_t Buffer :: get_page_number( off_t num_records, off_t record_size ){
	off_t records_per_page = PAGE_SIZE/record_size;
	off_t page_num = ceil( num_records/records_per_page );
	return off_t;
}

int Buffer :: get_new_page( File *file, Page *page ){
	if(is_buffer_full()){
		write_buffer(file);
	}
	page = new Page();
	num_page_used++;
	return 1; // Always returns 1 as page constructor exits when its not able to allocate memory.
}

void Buffer :: add( File *file, Record *record, off_t num_records){
	int record_size=((int*)record->bits)[0];
	off_t page_num = 0;
	Page *page;
	page_num = get_page_number( num_records, record_size );
	if( is_page_exists( page_num )){  // if page exists in the buffer. Return the page
			struct page_meta_info temp =  page_table.find(page_num)->second;
			// if page exists in the write buffer.
			if( temp.is_write_list ){
				page = *temp.iter;
				add_new_record( page, record );
			}
			// if page exists in the read buffer
			else if( !temp.is_write_list ){
				// Detach the node from the read list
				// Add the node to the write list
				// Get the page pointer.
				// change the is_write_node variable
				add_new_record( page, record );
			}
	}
	else if( page_num < file->curLength ){  // means that the page is in the db file
			file->GetPage(page, page_num);
			if( !is_buffer_full()){   // if there is space in buffer
				// Add page to write list.
				// increase the number of pages present in buffer
				// Do the map entry.
				add_new_record(page, record);
			}
			else if( is_buffer_full()){
				get_new_page(file, page);
				// do the entry in the map
				add_new_record( page, record);
			}
	}
	else{  // if a new page has to be allocated
		get_new_page(file, page);
		add_new_record(page, record);
	}
}

void Buffer :: add_new_record( Page *page, Record *record){
	int status = page->Append(record);
	if( status == 0){    // Failed to append to the page
		int status = 0;
		// Get a new page.
		// append the record to the page
	}
	// Attach the page to write head
}

void Buffer :: read_record( File *file, int curr_record, int num_records){
	//int record_size=((int*)record->bits)[0];
	// how to calculate record size here?
	int record_size = 0;
	off_t page_num = 0;
	Page *page;
	page_num = get_page_number( num_records, record_size );

}

void Buffer :: write_buffer( File *file ){
	off_t current_length = file->GetLength();
}

void Buffer :: clean(){
	read_head.clear();
	write_head.clear();
	num_page_used = 0;
	page_table.clear();
}

bool Buffer :: is_buffer_full(){
	return num_page_used == BUFFER_SIZE;
}

bool Buffer :: is_page_dirty( off_t page_number ){
	return page_table.find(page_number);
}

bool Buffer :: is_page_exists( off_t page_number ){
	return page_table.find(page_number) == page_table.end();
}


