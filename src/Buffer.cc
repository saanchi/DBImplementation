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

void Buffer :: add( File *file, Record *record, int num_records){
	
	    int recordSize=((int*)Copyme->bits)[0];
        int totalSize=recordSize*num_records;
		//We need to do TotalSize+recordSize, So that we need to check that whether 
		int internalfrag=PAGE_SIZE%recordSize;
		int PageCount=file.GetLength();
		int pageNo=(totalSize+recordSize+(internalfrage*PageCount-1)/PAGE_SIZE);
		//There could be internal fragmentation in the page so number of totalsize in byte will not give
		//the same record 
		//Now pageNo Excatly gives me  the page where my record should go 
		//No need to worry about half pages as that will overwrite that page
		/*
		addMe->ToBinary (bits);
	    lseek (myFilDes, PAGE_SIZE * whichPage, SEEK_SET);
		write (myFilDes, bits, PAGE_SIZE)
		*/  
		
		map<int,page_meta_info>::iterator it;
		it=page_table.find(pageNo);
		page *temp;
		if(it!=page_table.end())
		{
			//Page is already in buffer
			
			if(!page_table[pageNo].is_write_list)
			{
				//Page is Not Dirty ..Make it Dirty
				page_table[pageNo].is_write_list=true;
				temp=read_head.top();
				read_head.pop_back();
				write_head.push(temp);				
			}
			
			page *temp=write_head.top();
			temp.Append(record);	         
			
		}
		else   
		{
			// Create a new Page and add the record in that page 
			//Now add that record to the Buffer
			//Just append in the page
			page *temp=new page();
			temp.Append(record);
			
			//Page is in the Disk we need to take it from there 
			if(num_page_used+1>BUFFER_SIZE)//Buffer Full
			{
				//Write all page on the disk,empty the write_head list by calling void AddPage (Page *addMe, off_t whichPage);
			    //and decreasing numof page used
				
				//let if there is no page ,Remove a  page from front of the list read_head 
				//and increase the size of the buffer
			}
			
			 //Add this page into writehead and  map also
			 //write_head.push()
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
