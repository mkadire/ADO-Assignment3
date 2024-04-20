#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

FILE *filePointer;

//initilize file pointer to null.
void initStorageManager(void)
{
    filePointer = NULL;
}

//This function creates Page file with default size 1 and defualt bytes as '/0' with page size of 4096
//if resulting size of file is not 4096 then return RC_FILE_HANDLE_NOT_INIT
//if success return RC_OK
RC createPageFile(char *fileName)
{

    char chrs[PAGE_SIZE];
    filePointer = NULL;
    filePointer = fopen(fileName, "w+");

    if (filePointer == NULL)
    {
        fclose(filePointer);
        printf("\ncreate RC_FILE_HANDLE_NOT_INIT\n");

        return RC_FILE_HANDLE_NOT_INIT;
    }

    memset(chrs, '\0', sizeof(chrs));
    int size = 1;
    int res = fwrite(chrs, size, PAGE_SIZE, filePointer);
    printf("\n result of fwrite %d", res);
    if (res != PAGE_SIZE)
    {
        fclose(filePointer);
        destroyPageFile(fileName);
        printf("\ncreate RC_FILE_HANDLE_NOT_INIT\n");

        return RC_FILE_HANDLE_NOT_INIT;
    }

    fclose(filePointer);
    printf("\ncreate RC_OK\n");

    return RC_OK;
}

//This function opens the page file for specified file Name and fileHandle pointer
//checks for non null pointer and populates the struct of fHandle.
//Seeks to the start of the file.
//If the current value of the position indicator for the file pointer is divisible by PAGE_SIZE i.e. 4096, set the number of pages as current value of the position indicator/PAGE_SIZE else add 1 to it.
//if sucess return RC_OK else RC_FILE_NOT_FOUND
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    filePointer = NULL;
    filePointer = fopen(fileName, "rb+");
    if (filePointer != NULL)
    {
        fHandle->fileName = fileName;
        fHandle->curPagePos = 0;
        fseek(filePointer, 0, SEEK_END);

        if (ftell(filePointer) % PAGE_SIZE == 0)
        {
            fHandle->totalNumPages = (ftell(filePointer) / PAGE_SIZE);
        }
        else
        {
            fHandle->totalNumPages = (ftell(filePointer) / PAGE_SIZE + 1);
        }
        printf("/n numPages");
        printf(" %d, %ld", fHandle->totalNumPages, ftell(filePointer));
        printf("/n");

        fclose(filePointer);
        printf("\nopenPageFile RC_OK\n");

        return RC_OK;
    }
    printf("\nopenPageFile RC_FILE_NOT_FOUND\n");

    return RC_FILE_NOT_FOUND;
}

//This function deinits the fHandle struct pointer and returns RC_OK
RC closePageFile(SM_FileHandle *fHandle)
{

    fHandle->fileName = "";
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = 0;
    return RC_OK;
}

//This function removes the fileName passed in the argument and returns RC_OK if success else returns RC_FILE_NOT_FOUND
RC destroyPageFile(char *fileName)
{
    int result = remove(fileName);
    if (result != -1)
    {
        return RC_OK;
    }
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

// This function reads the specified block for the fHandle in the page file and memPage.
// if PageNumer is negetive or greater than the total number of pages for the specified fHandle, return RC_READ_NON_EXISTING_PAGE
// else open the file with the specified file name. Set the position pointer to the offset calculated my myltiplying the pagenumebr with the page size
// Read the block and set the current position as the current pageNum
// return RC_OK if success
//else return RC_READ_NON_EXISTING_PAGE
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    printf("page %d", pageNum);
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
    {

        return RC_READ_NON_EXISTING_PAGE;
    }
    else
    {
        filePointer = fopen(fHandle->fileName, "r");
        if (filePointer != NULL)
        {

            printf("fseek(filePointer, (pageNum * PAGE_SIZE), SEEK_SET); %d", fseek(filePointer, (pageNum * PAGE_SIZE), SEEK_SET));
            fseek(filePointer, (pageNum * PAGE_SIZE), SEEK_SET); //can check if fseek was success by assigning to int and checking == 0

            fread(memPage, sizeof(char), PAGE_SIZE, filePointer);
            fHandle->curPagePos = pageNum;
            fclose(filePointer);
            return RC_OK;
        }

        fclose(filePointer); //removed memory leak
        return RC_READ_NON_EXISTING_PAGE;
    }
}

//This function returns the current postion of the block for the specified fHandle in the page file
int getBlockPos(SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

// This function reads the first block of the specified fhandle in the page file and returns it.
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

// This function reads the previous block of the specified fhandle in the page file and returns it.
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int currentPagePosition = fHandle->curPagePos;
    int currentPageNumber = currentPagePosition / PAGE_SIZE;

    return readBlock(currentPageNumber - 1, fHandle, memPage);
}

// This function reads the current block of the specified fhandle in the page file and returns it.
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int currentPagePosition = fHandle->curPagePos;
    int currentPageNumber = currentPagePosition / PAGE_SIZE;

    return readBlock(currentPageNumber, fHandle, memPage);
}

// This function reads the next block of the specified fhandle in the page file and returns it.
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int currentPagePosition = fHandle->curPagePos;
    int currentPageNumber = currentPagePosition / PAGE_SIZE;

    return readBlock(currentPageNumber + 1, fHandle, memPage);
}

// This function reads the last block of the specified fhandle in the page file and returns it.
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int lastBlockPage = fHandle->totalNumPages - 1;

    return readBlock(lastBlockPage, fHandle, memPage);
}

// This function writes the block by taking arguments like pageNumber fhandle and memPage
// First it checks for the capacity is acceptable else returns RC_FILE_NOT_FOUND
// if the function is not able to seek to the position, return RC_READ_NON_EXISTING_PAGE or is not able to write, return RC_WRITE_FAILED
// else the writeBlock was successfull hence return RC_OK
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (ensureCapacity(pageNum, fHandle) == RC_OK)
    {
        FILE *filePointer;

        filePointer = fopen(fHandle->fileName, "rb+");

        if (fseek(filePointer, pageNum * PAGE_SIZE, SEEK_SET) != 0)
        {

            fclose(filePointer);
            return RC_READ_NON_EXISTING_PAGE;
        }
        else if (fwrite(memPage, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE)
        {

            fclose(filePointer);
            return RC_WRITE_FAILED;
        }
        else
        {

            fHandle->curPagePos = pageNum;
            fclose(filePointer);
            return RC_OK;
        }
    }
    else
    {

        return RC_FILE_NOT_FOUND;
    }
}

// This function writes the current block to file.
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int currentPage = fHandle->curPagePos / PAGE_SIZE;
    int pageNumToWrite = currentPage + 1;
    int writeCode = writeBlock(pageNumToWrite, fHandle, memPage);
    if (writeCode == RC_OK)
    {
        fHandle->totalNumPages++;
    }

    return writeCode;
}

// This function appends an empty block to the end of the file
RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    SM_PageHandle blockStrt = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    int pointer = fseek(filePointer, 0, SEEK_END);

    if (pointer != 0)
    {
        return RC_WRITE_FAILED;
    }
    else
    {
        int pageSize = fwrite(blockStrt, sizeof(char), PAGE_SIZE, filePointer);

        if (pageSize == PAGE_SIZE)
        {
            fHandle->totalNumPages++;
        }
    }

    free(blockStrt);
    return RC_OK;
}

//This function ensures that the capacity is acceptable by the fHandle and appends an empty block.
//if file pointer is null return RC_FILE_NOT_FOUND
//else append empty block at the end of the file.
//return RC_OK for successfull check.
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    filePointer = fopen(fHandle->fileName, "a");

    if (filePointer == NULL)
    {
        fclose(filePointer);
        return RC_FILE_NOT_FOUND;
    }
    else
    {
        while (fHandle->totalNumPages < numberOfPages)
        {
            appendEmptyBlock(fHandle);
        }
    }

    if (filePointer != NULL)
    {
        fclose(filePointer);
    }
    return RC_OK;
}