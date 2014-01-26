/*
 * Buffer.h
 *
 *  Created on: Jan 24, 2014
 *      Author: sanchit
 */

#ifndef BUFFER_H_
#define BUFFER_H_

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "map"
#include "list"

using namespace::std;

class Buffer {

	private:
		const int BUFFER_SIZE = 100;
		list<Page*> read_head;
		list<Page*> write_head;
		int num_page_used;

		struct page_meta_info{
			page_meta_info( bool is_write_list, list<Page*>::iterator iter ){
				this->is_write_list = is_write_list;
				this->iter = iter;
			}
			bool is_write_list;          // to denote whether the node is in write list or read list
			list<Page*>::iterator iter;   // location of the node in the list
		};

		map<int, page_meta_info> page_table; // contains the meta information about the page

		/*
		 * Get the page number
		 */
		off_t get_page_number(off_t num_records, off_t record_size );

		/*
		 * Returns a new page.
		 * If the buffer is not full returns a new page.
		 * If buffer is full. Writes the content into db and allocates a fresh page.
		 * Returns 1 on success and 0 on failure.
		 */
		int get_new_page( File *file, Page *page );

		/*
		 * Does the clean up tasks.
		 * Free the lists.
		 */
		void clean();

		/*
		 * To check if the current page exists in the buffer
		 */
		bool is_page_exists( off_t page_number );

		/*
		 * To check if the buffer is full to the capacity
		 */
		bool is_buffer_full();

		/*
		 * To check if the page is dirty
		 */
		bool is_page_dirty(off_t page_number);

		/*
		 * Attaches a new record to the end of the page passed.
		 * if not able to add record. fetch a new page and attaches it there.
		 * Returns 1 on success and 0 on failure
		 */
		void Buffer :: add_new_record( Page *page, Record *record);

	public:
		Buffer();
		virtual ~Buffer();
		/**
		 * Attaches the given record to the current write_head node.
		 *
		 */
		void add( File *file, Record *record, off_t num_records);

		/**
		 *  Write the current contents of the buffer into the file.
		 *  Returns 1 on success and 0 on failure
		 */
		void write_buffer( File *file);

		/**
		 * To read the curr_record number from the buffer.
		 * Buffer knows if the current record is in file or buffer.
		 */
		void read_record( File *file, int curr_record);

	};

#endif /* BUFFER_H_ */
