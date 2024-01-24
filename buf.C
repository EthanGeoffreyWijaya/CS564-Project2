//	Team Members:
//	Regan Kastelie		Ethan Geoffrey Wijaya
//	kastelie@wisc.edu	egwijaya@wisc.edu
//	9083606088		9082652331
//
//	File Description:
//	This file implements the buffer manager class. It is responsible for allocating
//	frames in the buffer pool according to a clock algorithm implementation as well as
//	allocating and reading pages on the buffer pool. 
#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;


    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


// Allocates frames on the buffer pool based on the clock algorithm
// if a free frame is found, it will be allocated. Otherwise, loop
// through buffer frames until a frame with false reference bit is found.
// Can write to disk if dirty bit is true.
// Mainly a helper method for readPage() and allocPage() below.
//
// Input: A buffer which will receive an int denoting the frame number 
// of the allocated frame
// Output: Status OK if succesful, BUFFEREXCEEDED if all frames pinned,
// UNIXERR if IO failed.
const Status BufMgr::allocBuf(int & frame) 
{
    Status status;
    int count = 0;
    while (count < (numBufs*2)){
        BufDesc* tmpBuf = &(bufTable[clockHand]);
        if (tmpBuf->valid == false){
            frame = clockHand;
            tmpBuf->frameNo = clockHand;
            tmpBuf->Set(tmpBuf->file,tmpBuf->pageNo);
            status = OK;
            return status;
        }
        else{
            if (tmpBuf->refbit == true){
                tmpBuf->refbit = false;
                advanceClock();
            }
            else{
                if (tmpBuf->pinCnt > 0){
                    advanceClock();
                }
                else{
                    if (tmpBuf->dirty == true){
                       if((status = tmpBuf->file->writePage(tmpBuf->pageNo, 
                            &(bufPool[clockHand]))) != OK){
                                status = UNIXERR;
                                return status;
                            } 
                    }
                    hashTable->remove(tmpBuf->file,tmpBuf->pageNo);
                    frame = clockHand;
                    tmpBuf->frameNo = clockHand;
                    //tmpBuf->Set(tmpBuf->file,tmpBuf->pageNo);
                    status = OK;
                    return status;
                }
            }
        }
        count++;
    }
    status = BUFFEREXCEEDED;
    return status;
}

// Reads a page from the database. Will read it directly from the
// buffer pool if it exists. If not, will use allocBuf() to allocate
// a new frame in the buffer pool from which the page will be read.
//
// Input: The file and page number of the specifiec page, file and PageNo
// respectively. And a buffer page which holds a pointer to the allocated frame.
// Output: Status OK if succesful, UNIXERR if there was a unix error, 
// BUFFEREXCEEDED if all buffer frames pinned, HASHTBLERROR if there was a hash table error
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
   int frame;
   Status status = hashTable->lookup(file, PageNo, frame);
   BufDesc* tmpbuf;
   if (status == OK) {
	tmpbuf = &(bufTable[frame]);
	tmpbuf->refbit = true;
    	tmpbuf->pinCnt++;
	page = &(bufPool[frame]);
	return status;
   } else if (status == HASHNOTFOUND) {
	if ((status = allocBuf(frame)) != OK) return status;
	if ((status = file->readPage(PageNo, &(bufPool[frame])))!= OK) return status;
	if ((status = hashTable->insert(file, PageNo, frame)) != OK) return status;
	tmpbuf = &(bufTable[frame]);
	tmpbuf->Set(file, PageNo);
	page = &(bufPool[frame]);
	return OK;
   }
   return status;
}

// Decrements the pin count of a specified frame and sets the dirty bit.
//
// Input: file, the file containing the specified page; PageNo, the page number
// of the specified page; dirty, the value with which to set the dirty bit.
// Output: Status OK if succesful, HASHNOTFOUND if page not in hash table, 
// PAGENOTPINNED if pin count is 0.
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    Status status = HASHNOTFOUND;
    int i = 0;
    if ((hashTable->lookup(file,PageNo,i)) == HASHNOTFOUND){
        return status;
    }
    //for (int i = 0; i < numBufs; i++){
        BufDesc* tmpBuf = &(bufTable[i]);
        if ((tmpBuf->file == file)&&(tmpBuf->pageNo == PageNo)){
            if (tmpBuf->pinCnt == 0){
                status = PAGENOTPINNED;
                return status;
            }
            else
                tmpBuf->pinCnt--;
            if (dirty == true){
                tmpBuf->dirty = true;
            }
            status = OK;
            return status;
        }
    //}
        status = HASHNOTFOUND;
        return status;

}

// Allocates an empty page in the buffer manager.
//
// Input: file, the file containing the specified page; pageNo, 
// a buffer to be set with the page number of the allocated page;
// page, a buffer to be set with a pointer to the allocated frame.
// Output: Status OK if succesful, UNIXERR if unix error occurred,
// BUFFEREXCEEDED if all buffer frames pinned, HASHTBLERROR if
// hash table error occurred.
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
   int frame;
   Status status;
   BufDesc* tmpbuf;
   if ((status = file->allocatePage(pageNo)) != OK) return status;
   if ((status = allocBuf(frame)) != OK) return status;
   if ((status = hashTable->insert(file, pageNo, frame))!= OK) return status;
   tmpbuf = &(bufTable[frame]);
   tmpbuf->Set(file, pageNo);
   page = &(bufPool[frame]);
   return OK;

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


