/*
 * Buffer.cpp
 *
 *  Created on: Jan 24, 2014
 *      Author: sanchit
 */

#include "Buffer.h"

Buffer::Buffer() {
	num_page_used = 0;
	map = NULL;
}

Buffer::~Buffer() {

}

Page* Buffer :: get_page( File *file, off_t num_records ){
	// Fetch the page_number
	off_t page_num = 0;
	if( is_page_exists( page_num )){  // if page exists in the map. Return the page
		struct page_meta_info temp =  page_table.find(page_num)->second;
		return *temp.iter;
	}
	else if( page_num < file->curLength ){  // means that the page is in the db file
		Page *page;
		file->GetPage(page, page_num);

		// Add the page into the map
		struct page_meta_info meta( false, );
		page_table.insert( page_num,   );
		num_page_used++;
		return page;
	}
	else{   // means that a  new page needs to be created
		Page *page;
		get_new_page( file, page );
		return page;
	}
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
	Page *page = NULL;
	int status = 0;
	Page *page = get_page( file, num_records); // returns a page where it needs to be added
	add_new_record( page, record);
	if( write_head.empty()){     // if the write page has not been used
		write_head.push_back(page);
	}
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

void Buffer :: add_new_record( Page *page, Record *record){
	int status = page->Append(record);
	if( status == 0){    // Failed to append to the page
		// Get a new page
	}


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


