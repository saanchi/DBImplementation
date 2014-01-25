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
		TwoWayList<Page> *read_head;
		TwoWayList<Page> *write_head;
		int num_page_used;
		map<int, bool> page_table; // boolean to denote whether the page is dirty or not

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
		bool is_page_exists( int page_number );

		/*
		 * To check if the buffer is full to the capacity
		 */
		bool is_buffer_full();

		/*
		 * To check if the page is dirty
		 */
		bool is_page_dirty(int page_number);

		/*
		 * Add a new record to the end of the page.
		 * Returns 1 on success and 0 on failure
		 */
		int Buffer :: add_new_record( Record *record);

	public:
		Buffer();
		virtual ~Buffer();
		/**
		 * Attaches the given record to the current write_head node.
		 * Checks if write_head contains the space for adding new record.
		 * If yes add there else ask for a new page from get_new_page().
		 * Returns 1 on success and 0 on failure.
		 */
		int add( File *file, Record *record );

		/**
		 *  Write the current contents of the buffer into the file.
		 *  Returns 1 on success and 0 on failure
		 */
		void write_buffer( File *file);

		/**
		 *
		 */
		void read_next( File *file, int curr_record);

	};

#endif /* BUFFER_H_ */
