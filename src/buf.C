/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/


#include "buf.h"


// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char* bufErrMsgs[] = { 
  // error message strings go here
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR,bufErrMsgs);



//@kasin i will implement error tomorrow or at midnight coz i am not sure yet
BufMgr::BufMgr (int numbuf, Replacer *replacer) {
  // put your code here
//	cerr << "_________________BufMgr_______________ @kasin "<<endl;
	//@kasin We are going to allocate numbuf pages for buffer pool in main memory
	//bufPool is Page*
	bufPool = new Page[numbuf]; // in C++, call gouzao function, delete
	bufDescr = new Descriptors[numbuf];
	this->numbuf = numbuf;
}

//@kasin
		// Check if this page is in buffer pool, otherwise
        // find a frame for this page, read in and pin it.
        // also write out the old page if it's dirty before reading
        // if emptyPage==TRUE, then actually no read is done to bring
        // the page
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage) {
  // put your code here
//	cerr << "_________________pinPage_______________ @kasin "<<endl;
	int frame_number = findHash(PageId_in_a_DB);
//	cerr << "pin: findHash -> pageId = "<<PageId_in_a_DB<<" to frame_number = "<<frame_number<<endl;
	if(frame_number != -1) // is in buffer pool
	{
		bufDescr[frame_number].pin_count++;
		page = bufPool + frame_number;
	}
	else
	{
		frame_number = getFrame();
		if(frame_number < 0)
		{
			for(int num = 0; num < numbuf; ++num)
			{
				if((bufDescr[num].pin_count == 0) && (bufDescr[num].dirty_bit == 0)) 
				{
					frame_number = num;
					break;
				}
			}
			if(frame_number < 0) return FAIL;
		}
		if(bufDescr[frame_number].dirty_bit == true)
			if(MINIBASE_DB->write_page(bufDescr[frame_number].page_number, bufPool+frame_number) != OK)
				return FAIL;
		if(!emptyPage)
			if(MINIBASE_DB->read_page(PageId_in_a_DB, bufPool + frame_number) != OK)
				return FAIL;
		page = bufPool + frame_number;
		bufDescr[frame_number].page_number = PageId_in_a_DB;
		bufDescr[frame_number].pin_count = 1;
		bufDescr[frame_number].dirty_bit = false;
	//	cerr << "bufDescr["<<frame_number<<"] = "<<PageId_in_a_DB <<"1 false"<<endl; 
		if(!insertHash(PageId_in_a_DB, frame_number)) 
		{
			cerr << "insertHash Error @kasin"<<endl;
			return FAIL;
		}		
	}
  return OK;
}//end pinPage

//@kasin
		// call DB object to allocate a run of new pages and 
        // find a frame in the buffer pool for the first page
        // and pin it. If buffer is full, ask DB to deallocate 
        // all these pages and return error
Status BufMgr::newPage(PageId& firstPageId, Page*& firstpage, int howmany) {
  // put your code here
//	cerr << "_________________newPage_______________ @kasin "<<endl;
	bool full = true;
	for(int num = 0; num < numbuf; ++num)
	{
		if(bufDescr[num].pin_count == 0) 
		{
			full = false;
			break;
		}
	}
	if(full) //If buffer is full, ask DB to deallocate all these pages and return error
	{
		for(int num = 0; num < numbuf; ++num)
		{
			MINIBASE_DB->deallocate_page(bufDescr[num].page_number, 1);
		}
		return FAIL; //return error
	}
	if(MINIBASE_DB->allocate_page(firstPageId, howmany) != OK) 
	{
		return FAIL; //cannot allocate
	}	
	return pinPage(firstPageId, firstpage, true);
  return OK;
}

//@kasin
		// Used to flush a particular page of the buffer pool to disk
        // Should call the write_page method of the DB class
Status BufMgr::flushPage(PageId pageid) {
  // put your code here
//	cerr << "_________________flushPage_______________ @kasin "<<endl;
	int frame_number = findHash(pageid);
	if(frame_number == -1) return FAIL;
  return MINIBASE_DB->write_page(pageid, bufPool+frame_number);

  return OK;
}
    
	  
//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr(){
//cerr << "_________________~BufMgr_______________ @kasin "<<endl;
  // put your code here
	//@kasin 
	//@kasin 1. should flush all dirty pages in the pool to disk
	//@kasin 2. shutting down
	//@kasin 3. deallocate the buffer pool in main memory	
	flushAllPages();
	delete[] bufPool;
	delete[] bufDescr;
}


//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
//@kasin		
		// hate should be TRUE if the page is hated and FALSE otherwise
        // if pincount>0, decrement it and if it becomes zero,
        // put it in a group of replacement candidates.
        // if pincount=0 before this call, return error.
Status BufMgr::unpinPage(PageId page_num, int dirty=FALSE, int hate = FALSE){
  // put your code here
//cerr << "_________________unpinPage_______________ @kasin "<<endl;
	int frame_number = findHash(page_num);
	if(frame_number == -1) return FAIL;
	if(bufDescr[frame_number].pin_count <= 0) return FAIL;
	bufDescr[frame_number].dirty_bit = dirty;
	bufDescr[frame_number].pin_count--;
	if(bufDescr[frame_number].pin_count == 0)
	{
		if(hate)
		{
			for(list<int>::iterator it = hated.begin(); it != hated.end(); ++it)
			{
				if (*it == frame_number) 
				{
					it = hated.erase(it);
					break;
				}
			}
			hated.push_front(frame_number);
			return OK;
		}
		else
		{
			for(list<int>::iterator it = loved.begin(); it != loved.end(); ++it)
			{
				if(*it == frame_number)
				{
					it = loved.erase(it);
					break;
				}
			}
			loved.push_back(frame_number);
			for(list<int>::iterator it = hated.begin(); it != hated.end(); ++it)
			{
				if(*it == frame_number)
				{
					it = hated.erase(it);
					break;
				}
			}
			return OK;
		}
	} 
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
//@kasin		
		// user should call this method if it needs to delete a page
        // this routine will call DB to deallocate the page 
Status BufMgr::freePage(PageId globalPageId){
  // put your code here
//cerr << "_________________freePage_______________ @kasin "<<globalPageId<<endl;
	int frame_number = findHash(globalPageId);
	if(frame_number == -1) 
	{
//		cerr << "freePage: findHash -> not found"<<endl;
		return FAIL;
	}
	if(bufDescr[frame_number].pin_count > 0) return FAIL;
	if(bufDescr[frame_number].dirty_bit)
	{
		return MINIBASE_DB->deallocate_page(globalPageId, 1);
	}
  return OK;
}

Status BufMgr::flushAllPages(){
  //put your code here
//cerr << "_________________flushAllPage_______________ @kasin "<<endl;
	for(int num = 0; num < numbuf; ++num)
	{
		if(bufDescr[num].dirty_bit == true)
		{
			if(MINIBASE_DB->write_page(bufDescr[num].page_number, bufPool+num) == OK)
			{
				bufDescr[num].dirty_bit = false;
			} 
			else return FAIL; //why not bufPool+num		
		}
	}
  return OK;

}

//@kasin this is about hash directory
bool BufMgr::insertHash(PageId page_number, int frame_number)
{
//cerr << "_________________insertHash_______________ @kasin "<<page_number << " "<<frame_number<< endl;
	int bucket_number = calBucket(page_number);
	if(findHash(page_number) >= 0)
	{
//		cerr << "findHash before insert @kasin"<<endl;
		 return false;
	}
//	cerr << "~"<<endl;
	entry e(page_number, frame_number);
	hashDir[bucket_number].push_back(e);
	return true;	
}

bool BufMgr::eraseHash(PageId page_number)
{
//cerr << "_________________eraseHash_______________ @kasin "<<endl;
	int bucket_number = calBucket(page_number);
	for(list<entry>::iterator it = hashDir[bucket_number].begin(); it != hashDir[bucket_number].end(); ++it)
	{
		if(it->page_number == page_number)
		{
			it = hashDir[bucket_number].erase(it);
			return true;
		}	
	}
	return false;
}
	
int BufMgr::findHash(PageId page_number)
{
//cerr << "_________________findHash_______________ @kasin "<<endl;
	int bucket_number = calBucket(page_number);
//	cerr << "page = "<<page_number<<" in b_number = "<<bucket_number <<endl;
	for(list<entry>::iterator it = hashDir[bucket_number].begin(); it != hashDir[bucket_number].end(); ++it)
	{
//		cerr<<"for444444444444444"<<endl;
		if(it->page_number == page_number)
			return it->frame_number;	
	}
//	cerr << "222222222222222222222222222222"<<endl;
	return -1;
	
}

int BufMgr::getFrame()
{
//cerr << "_________________getFrame_______________ @kasin "<<endl;
	int frame_number;
	if(hated.empty())
	{
		if(loved.empty())
		{
			bool notAllZero = true;
			for(int num = 0; num < numbuf; ++num)
			{
				if(bufDescr[num].pin_count != 0)
				{
					notAllZero = false;
				}
			}
			if(!notAllZero) return -1;
		}
		else
		{
			list<int>::iterator it = loved.begin();
			frame_number = *it;
			loved.pop_front();
			if(!eraseHash(bufDescr[frame_number].page_number)) 
			{
				cerr << "erase error !! @kasin"<<endl;
				while(1);
			}
			return frame_number;
		}
	}
	else
	{
		list<int>::iterator it = hated.begin();
		frame_number = *it;
		hated.pop_front();
		if(!eraseHash(bufDescr[frame_number].page_number))
		{
			cerr << "erase error !! @kasin "<<endl;
			while(1);		
		}
		return frame_number;
	}		
}

