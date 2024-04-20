#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

typedef struct Page
{
    SM_PageHandle data_pg;
    PageNumber num_pg;
    int ct_pg;
    int hit_num_lru;
    int ref_no_lru;
    int bit_dr;
} PgModel;

int writeCount = 0;
int buf_length = 0;
int currframeTotal = 0;
int RC_PIN_PAGE_IN_BUFFER_POOL = 16;
int zeroValue = 0, defaultValue = 1;
int LFUPosPointer = 0;
int readIdx = 0;

//This function creates a new Buffer Pool

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
    ReplacementStrategy strat = strategy;
    int i = 0;

    bm->strategy = strat;
    bm->pageFile = (char *)pageFileName;
    buf_length = numPages;
    bm->numPages = numPages;

    PgModel *buf_pack = malloc(numPages * sizeof(PgModel));

    while (i < buf_length)
    {

        buf_pack[i].ref_no_lru = zeroValue;
        buf_pack[i].hit_num_lru = 0;
        buf_pack[i].num_pg = -1;
        buf_pack[i].ct_pg = zeroValue;
        buf_pack[i].bit_dr = 0;
        buf_pack[i].data_pg = NULL;
        i++;
    }
    bm->mgmtData = buf_pack;
    writeCount = LFUPosPointer = zeroValue;

    return RC_OK;
}

//This function defines the FIFO algo
void FIFO(BM_BufferPool *const bm, PgModel *page)
{

    PgModel *pgModel = (PgModel *)bm->mgmtData;

    int currentClientCount = page->ct_pg;
    int idx = readIdx % buf_length;

    int currentDirtyBit = page->bit_dr;
    int i = 0;

    int currentPageNum = page->num_pg;

    while (buf_length > i)
    {
        if (pgModel[idx].ct_pg != 0)
        {
            idx++;
            idx = (idx % buf_length == 0) ? 0 : idx;
        }
        else if (zeroValue == pgModel[idx].ct_pg)
        {
            if (defaultValue == pgModel[idx].bit_dr)
            {
                SM_FileHandle f_Handle;

                openPageFile(bm->pageFile, &f_Handle);
                writeBlock(pgModel[idx].num_pg, &f_Handle, pgModel[idx].data_pg);
                writeCount += 1;
            }
            pgModel[idx].bit_dr = currentDirtyBit;
            pgModel[idx].data_pg = page->data_pg;
            pgModel[idx].ct_pg = currentClientCount;

            pgModel[idx].num_pg = currentPageNum;
            break;
        }
        i += 1;
    }
}

//This function defines the LFU algo
void LFU(BM_BufferPool *const bm, PgModel *page)
{
    int i = 0;
    int j = 0;
    PgModel *pgModel = (PgModel *)bm->mgmtData;
    int buffSize = buf_length;
    int LFUReference;
    int k = LFUPosPointer;

    int currentPageNum = page->num_pg;

    int newClientCount = page->ct_pg;

    int currentDirtyBit = page->bit_dr;

    while (i < buffSize)
    {
        if (pgModel[k].ct_pg == 0)
        {
            k = (k + i) % buf_length;
            LFUReference = pgModel[k].ref_no_lru;
            if (true)
                break;
        }
        i++;
    }
    i = (1 + k) % buf_length;
    while (j < buffSize)
    {
        if (pgModel[i].ref_no_lru < LFUReference)
        {
            k = i;
            LFUReference = pgModel[i].ref_no_lru;
        }
        i = (i + 1) % buf_length;
        j++;
    }

    SM_FileHandle f_hndl;
    if (1 == pgModel[k].bit_dr)
    {

        SM_PageHandle m_pg = pgModel[k].data_pg;

        openPageFile(bm->pageFile, &f_hndl);
        writeBlock(pgModel[k].num_pg, &f_hndl, m_pg);

        writeCount++;
    }

    pgModel[k].bit_dr = currentDirtyBit;

    pgModel[k].num_pg = currentPageNum;
    LFUPosPointer = 1 + k;
    pgModel[k].ct_pg = newClientCount;
    pgModel[k].data_pg = page->data_pg;
}

//This function defines the LRU algo
void LRU(BM_BufferPool *const bm, PgModel *page)
{
    int currentClientCount = page->ct_pg;
    int j = 0;
    PgModel *pgModel = (PgModel *)bm->mgmtData;

    int currentPageNum = page->num_pg;
    int l_h_r, lhc;

    int currentDirtyBit = page->bit_dr;

    while (buf_length > j)
    {
        if (0 != pgModel[j].ct_pg)
        {
            continue;
        }
        else if (0 == pgModel[j].ct_pg)
        {

            lhc = pgModel[j].hit_num_lru;
            l_h_r = j;
            break;
        }
        j += 1;
    }
    int i = defaultValue + l_h_r;
    while (buf_length > i)
    {
        if (pgModel[i].hit_num_lru < lhc)
        {
            l_h_r = i;
            lhc = pgModel[i].hit_num_lru;
        }
        else
        {
            i++;
            continue;
        }
        i++;
    }

    SM_PageHandle m_pg = pgModel[l_h_r].data_pg;
    SM_FileHandle f_hndl;

    if (defaultValue == pgModel[l_h_r].bit_dr)
    {
        openPageFile(bm->pageFile, &f_hndl);
        writeBlock(pgModel[l_h_r].num_pg, &f_hndl, m_pg);
        writeCount += 1;
    }
    pgModel[l_h_r].hit_num_lru = page->hit_num_lru;

    pgModel[l_h_r].ct_pg = currentClientCount;

    pgModel[l_h_r].bit_dr = currentDirtyBit;

    pgModel[l_h_r].num_pg = currentPageNum;
    pgModel[l_h_r].data_pg = page->data_pg;
}

//This Function Destroys the Buffer Pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    bool check = false;
    int i = 0;
    PgModel *pgModel = (PgModel *)bm->mgmtData;

    forceFlushPool(bm);

    while (i < buf_length)
    {
        if (pgModel[i].ct_pg != 0)
        {
            check = true;
        }

        free(pgModel[i].data_pg);
        pgModel[i].data_pg = NULL;

        i++;
    }

    bm->mgmtData = NULL;
    free(pgModel);
    if (check)
    {
        return RC_PIN_PAGE_IN_BUFFER_POOL;
    }
    return RC_OK;
}

//this function Causes all dirty pages from the buffer pool to be written to disk
RC forceFlushPool(BM_BufferPool *const bm)
{

    PgModel *pg_model = (PgModel *)bm->mgmtData;
    for (int index = 0; index < buf_length; index++)
    {
        int pNum = pg_model[index].num_pg;

        SM_PageHandle m_pg = pg_model[index].data_pg;
        SM_FileHandle f_hndl;

        if (defaultValue == pg_model[index].bit_dr && zeroValue == pg_model[index].ct_pg)
        {
            char *f_nm = bm->pageFile;
            pg_model[index].bit_dr = zeroValue;
            openPageFile(f_nm, &f_hndl);
            writeBlock(pNum, &f_hndl, m_pg);
            writeCount += 1;
        }
    }
    return RC_OK;
}

//this function unpins the page
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{

    RC rc_cd = RC_OK;

    bool fg = false;
    PgModel *resArr = (PgModel *)bm->mgmtData;

    int ct = 0;

    int i = 0;

    int pCnt = page->pageNum;

    while (i < buf_length)
    {
        int pNum = resArr[i].num_pg;
        if (pNum == pCnt)
        {
            ct += 1;
        }
        if (ct != 0)
        {
            resArr[i].ct_pg -= 1;
            if (!fg)
            {
                break;
            }
        }
        i += 1;
    }
    return rc_cd;
}

//this function pins the page with page number pageNum
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    int no_val = -1;
    int zero_val = 0;

    PgModel *pg_model = (PgModel *)bm->mgmtData;
    RC rc_cd = RC_OK;

    int is_LRU = 1;
    int is_LFU = 3;

    SM_FileHandle f_hndl;

    if (pg_model[zero_val].num_pg != no_val)
    {
        bool flag = true;
        for (int i = 0; i < buf_length; i++)
        {
            if (pg_model[i].num_pg == no_val)
            {

                pg_model[i].data_pg = (SM_PageHandle)malloc(PAGE_SIZE);
                pg_model[i].ct_pg = defaultValue;
                openPageFile(bm->pageFile, &f_hndl);
                pg_model[i].ref_no_lru = zeroValue;
                readBlock(pageNum, &f_hndl, pg_model[i].data_pg);

                pg_model[i].num_pg = pageNum;

                currframeTotal += 1;
                readIdx += 1;

                bool strat_LRU = (bm->strategy == is_LRU) ? true : false;

                if (strat_LRU == true)
                {
                    pg_model[i].hit_num_lru = currframeTotal;
                }

                page->data = pg_model[i].data_pg;
                page->pageNum = pageNum;

                flag = false;
                break;
            }
            else
            {
                if (pg_model[i].num_pg != pageNum)
                {
                    flag = true;
                }
                else if (pg_model[i].num_pg == pageNum)
                {

                    bool strat_LRU = (bm->strategy == is_LRU) ? true : false;

                    bool strat_LFU = false;

                    if (bm->strategy == is_LFU)
                    {
                        strat_LFU = true;
                    }

                    flag = false;

                    pg_model[i].ct_pg++;
                    currframeTotal++;

                    if (strat_LRU == true)
                    {
                        pg_model[i].hit_num_lru = currframeTotal;
                    }
                    if (strat_LFU == true)
                    {
                        pg_model[i].ref_no_lru++;
                    }
                    page->pageNum = pageNum;
                    page->data = pg_model[i].data_pg;
                    break;
                }
            }
        }

        if (flag)
        {
            PgModel *nPage = (PgModel *)malloc(sizeof(PgModel));

            SM_FileHandle f_hndl;

            nPage->data_pg = (SM_PageHandle)malloc(PAGE_SIZE);
            openPageFile(bm->pageFile, &f_hndl);

            nPage->ref_no_lru = zeroValue;
            readBlock(pageNum, &f_hndl, nPage->data_pg);
            readIdx += 1;

            nPage->num_pg = pageNum;

            nPage->bit_dr = zeroValue;
            currframeTotal += 1;
            nPage->ct_pg = defaultValue;

            bool strat_LRU = (bm->strategy == is_LRU) ? true : false;
            if (strat_LRU)
            {
                nPage->hit_num_lru = currframeTotal;
            }
            page->data = nPage->data_pg;
            page->pageNum = pageNum;

            if (bm->strategy == 0)
            {
                FIFO(bm, nPage);
            }
            else if (bm->strategy == 1)
            {
                LRU(bm, nPage);
            }
            else if (bm->strategy == 3)
            {
                LFU(bm, nPage);
            }
        }
        return rc_cd;
    }
    else
    {
        SM_FileHandle f_hdle;
        pg_model[zero_val].data_pg = (SM_PageHandle)malloc(PAGE_SIZE);
        openPageFile(bm->pageFile, &f_hdle);

        ensureCapacity(pageNum, &f_hdle);
        readBlock(pageNum, &f_hdle, pg_model[0].data_pg);

        pg_model[zero_val].ct_pg++;
        pg_model[zero_val].num_pg = pageNum;

        readIdx = currframeTotal = zero_val;
        pg_model[zero_val].ref_no_lru = zero_val;
        page->pageNum = pageNum;

        pg_model[zero_val].hit_num_lru = currframeTotal;

        page->data = pg_model[zero_val].data_pg;

        return rc_cd;
    }
}

// This function will return the Page No.'s array
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    PageNumber *pg_num = malloc(sizeof(PageNumber) * buf_length);
    PgModel *pg_model = (PgModel *)bm->mgmtData;

    for (int i = 0; i < buf_length; i++)
    {
        if (pg_model[i].num_pg == -1)
        {
            pg_num[i] = NO_PAGE;
        }
        else
        {
            pg_num[i] = pg_model[i].num_pg;
        }
    }
    return pg_num;
}

//this function will mark the page dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i = 0;
    RC code_return = RC_ERROR;
    PgModel *result_array = (PgModel *)bm->mgmtData;

    while (true)
    {
        if (result_array[i].num_pg == page->pageNum)
        {
            result_array[i].bit_dr = defaultValue;
            return RC_OK;
        }
        i++;
    }
    return code_return;
}

// This function will return the boolean's array
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    int i = 0;
    bool *dirty_flags = malloc(buf_length * sizeof(bool));
    PgModel *pg_model = (PgModel *)bm->mgmtData;

    while (i < buf_length)
    {
        if (pg_model[i].bit_dr == 1)
        {
            dirty_flags[i] = true;
        }
        else
        {
            dirty_flags[i] = false;
        }
        i++;
    }
    return dirty_flags;
}

// This function returns the no. of pages written to the page file, since buffer pool is initialized.
int getNumWriteIO(BM_BufferPool *const bm)
{
    return writeCount;
}

// This function retuns the no. of pages that are read from disk, since buffer pool is initialized.
int getNumReadIO(BM_BufferPool *const bm)
{
    return readIdx + 1;
}

// This function will return the int's array
int *getFixCounts(BM_BufferPool *const bm)
{
    int *client_cnt = malloc(sizeof(int) * buf_length);
    PgModel *pg_model = (PgModel *)bm->mgmtData;

    for (int cnt = 0; cnt < buf_length; cnt++)
    {
        if (pg_model[cnt].ct_pg == -1)
        {
            client_cnt[cnt] = zeroValue;
        }
        else
        {
            client_cnt[cnt] = pg_model[cnt].ct_pg;
        }
    }
    return client_cnt;
}

// this function writes the current content of the page back to the page file on disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int ct = 0;
    RC code_result = RC_OK;

    PgModel *result_array = (PgModel *)bm->mgmtData;

    while (ct < buf_length)
    {
        SM_FileHandle f_handle;
        SM_PageHandle Page_mem = result_array[ct].data_pg;
        if (result_array[ct].num_pg == page->pageNum)
        {
            openPageFile(bm->pageFile, &f_handle);
            writeBlock(result_array[ct].num_pg, &f_handle, Page_mem);
            result_array[ct].bit_dr = zeroValue;
            writeCount++;
        }
        ct++;
    }
    return code_result;
}