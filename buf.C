#include <errno.h>
#include <fcntl.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>

#include "buf.h"
#include "page.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c)) {                                            \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs) {
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr() {
    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {
#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/**
 * @brief Allocates a free buffer frame using the clock algorithm.
 *
 * This method allocates a free buffer frame using the clock algorithm. If necessary,
 * it writes a dirty page back to disk before allocating the frame.
 *
 * If the buffer frame allocated has a valid page in it, the appropriate
 * entry is removed from the hash table.
 *
 * @param[out] frame An integer reference parameter where the index of the allocated
 *                   buffer frame will be stored.
 * @return Status BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if an error
 *         occurred during disk I/O, and OK otherwise.
 */
const Status BufMgr::allocBuf(int& frame) {
    BufDesc* tmpbuf;
    Status rc;
    int numPins = 0;

    // Looping to find replacable frame unless all pages are pinned
    while (numPins < numBufs) {
        // Increment clock ptr.
        clockHand = (clockHand + 1) % numBufs;
        tmpbuf = &bufTable[clockHand];

        // Checking valid bit
        if (!tmpbuf->valid) {
            break;
        }

        // Checking refBit
        if (!tmpbuf->refbit) {
            // Checking pins
            if (tmpbuf->pinCnt == 0) {
                break;
            }
            numPins++;
            continue;
        }

        tmpbuf->refbit = false;
    }

    // Checking if all pages are pinned
    if (numPins >= numBufs) {
        return BUFFEREXCEEDED;
    }

    // If page to be replaced is valid, remove entry from hash table
    // If dirty, flush to disk
    if (tmpbuf->valid) {
        rc = hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
        if (rc != OK) {
            return rc;
        }

        if (tmpbuf->dirty) {
            rc = tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[clockHand]));
            if (rc != OK) {
                return UNIXERR;
            }
        }
    }

    frame = tmpbuf->frameNo;
    return OK;
}

/**
 * @brief Reads a page from disk into the buffer pool.
 *
 * First, checks whether the page is already in the buffer pool. Then, two
 * cases to be handled :
 *
 * Case 1) Page is not in the buffer pool:
 *    - Calls allocBuf() to allocate a buffer frame.
 *    - Calls the method file->readPage() to read the page from disk into the buffer pool frame.
 *    - Inserts the page into the hashtable.
 *    - Invokes Set() on the frame to set it up properly. Set() will leave the pinCnt for the page set to 1.
 *    - Returns a pointer to the frame containing the page via the page parameter.
 *
 * Case 2) Page is in the buffer pool:
 *    - Sets the appropriate refbit.
 *    - Increments the pinCnt for the page.
 *    - Returns a pointer to the frame containing the page via the page parameter.
 *
 * @param file A pointer to the file from which to read the page.
 * @param PageNo The number of the page to read.
 * @param[out] page A reference to a pointer that will be set to the frame containing the page.
 * @return Status OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED
 *         if all buffer frames are pinned, HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
    Status rc;

    // Checking whether page is already in buffer pool
    int frameno;
    rc = hashTable->lookup(file, PageNo, frameno);
    if (rc != OK || rc != HASHNOTFOUND) {
        return rc;
    }

    // Case 1: lookup was unsuccessful
    if (rc == HASHNOTFOUND) {
        // Allocating new buffer frame
        int repframe;
        rc = allocBuf(repframe);
        if (rc != OK) {
            return rc;
        }

        // Reading from disk to buffer frame
        rc = file->readPage(PageNo, &(bufPool[repframe]));
        if (rc != OK) {
            return UNIXERR;
        }

        // Inserting page into hashtable
        rc = hashTable->insert(file, PageNo, repframe);
        if (rc != OK) {
            return HASHTBLERROR;
        }

        // Setting up frame and return page
        bufTable[repframe].Set(file, PageNo);
        page = &(bufPool[repframe]);
    } else {  // Case 2: lookup was successful

        // Setting refbit to true, incrementing pinCnt, and return page
        bufTable[frameno].refbit = true;
        bufTable[frameno].pinCnt++;
        page = &(bufPool[frameno]);
    }

    return OK;
}

/**
 * @brief Decrements the pinCnt of the frame containing (file, PageNo).
 *        If dirty == true, sets the dirty bit.
 *
 * Decrements the pinCnt of the frame containing the specified (file, PageNo) pair.
 * If the dirty flag is true, sets the dirty bit for the page.
 *
 * @param file A pointer to the file containing the page.
 * @param PageNo The number of the page to decrement the pin count.
 * @param dirty A boolean flag indicating whether the page is dirty.
 * @return Status OK if no errors occurred, HASHNOTFOUND if the page is not in
 *         the buffer pool hash table, PAGENOTPINNED if the pin count is already 0.
 */
const Status BufMgr::unPinPage(File* file, const int PageNo,
                               const bool dirty) {
    Status rc;
    int frameno;
    rc = hashTable->lookup(file, PageNo, frameno);
    if (rc != OK) {
        return HASHNOTFOUND;
    }

    // Decrementing pinCnt unless already 0, setting dirty bit
    if (bufTable[frameno].pinCnt == 0) {
        return PAGENOTPINNED;
    }
    bufTable[frameno].pinCnt--;
    if (dirty) {
        bufTable[frameno].dirty = dirty;
    }

    return OK;
}

/**
 * @brief Allocates an empty page in the specified file and obtains a buffer pool frame for it.
 *
 * Allocates an empty page in the specified file and obtains a buffer pool frame.
 * An entry is inserted into the hash table, and Set() is invoked on the frame to set it up properly.
 *
 * @param file A pointer to the file in which to allocate the page.
 * @param[out] PageNo A reference to an integer where the page number of the newly allocated page will be stored.
 * @param[out] page A reference to a pointer where the allocated buffer pool frame for the page will be stored.
 * @return Status OK if no errors occurred, UNIXERR if a Unix error occurred,
 *         BUFFEREXCEEDED if all buffer frames are pinned, and HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {
    Status rc;

    // Allocating an empty page in the file and obtaning new buffer pool frame
    int frameno;
    file->allocatePage(pageNo);
    rc = allocBuf(frameno);
    if (rc != OK) {
        return rc;
    }

    // Inserting new entry in hash table
    rc = hashTable->insert(file, pageNo, frameno);
    if (rc != OK) {
        return HASHTBLERROR;
    }

    bufTable[frameno].Set(file, pageNo);
    page = &(bufPool[frameno]);

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) {
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK) {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) {
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

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void) {
    BufDesc* tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
