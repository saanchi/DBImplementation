/*
 * Buffer.cpp
 *
 *  Created on: Jan 24, 2014
 *      Author: sanchit
 */

#include "Buffer.h"

Buffer::Buffer() {
	read_head = NULL;
	write_head = NULL;
	num_page_used = 0;
	map = NULL;
}

Buffer::~Buffer() {

}

int Buffer :: get_new_page( File *file, Page *page ){
	if(is_buffer_full()){
		write_buffer(file);
	}
	page = new Page();
	num_page_used++;
	return 1; // Always returns 1 as page constructor exits when its not able to allocate memory.
}

int Buffer :: add( File *file, Record *record){
	Page *page = NULL;
	int status = 0;
	if( write_head == NULL){
		write_head = new TwoWayList<Page>();
		status = add_new_record( record );
	}
	if( status == 0){
		page = get_new_page(file, page);
		int page_number = file->GetLength();
		status = add_new_record( record );
	}
	return status;
}

void Buffer :: write_buffer( File *file ){
	off_t current_length = file->GetLength();
	write_head->MoveToStart();
	Page *curr_page = write_head->Current(0);

	while( write_head->RightLength() != 0 ){
		file->AddPage(curr_page, current_length+1);   // Write the page into file
		write_head->Remove(curr_page);
		current_length++;
	}
}

int Buffer :: add_new_record( Record *record){
	write_head->MoveToFinish();
	return write_head->Current(0)->Append(record);
}

bool Buffer :: is_buffer_full(){
	return num_page_used == BUFFER_SIZE;
}

bool Buffer :: is_page_dirty( int page_number ){
	return page_table.find(page_number);
}

bool Buffer :: is_page_exists( int page_number ){
	return page_table.find(page_number) == page_table.end();
}


