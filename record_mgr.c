#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct RecordManagerModel
{
    int var_count;
    int tuple_count;
    int free_count;
    RID record_id;
    Expr *cond;
    BM_BufferPool buff_pool;
    BM_PageHandle pg_hndle;
} RecordManagerModel;

RecordManagerModel *fInformation;
const int ct_pg = 100;
const int ele_n = 15;

// Init the record Manager
RC initRecordManager(void *mgmtData)
{
    int recordCount = 0;
    initStorageManager();
    recordCount = 1;
    return RC_OK;
}

// sets the Schema
SM_PageHandle fixSchema(RM_TableData *rel)
{
    int fixCount = 0;
    Schema *tb;
    SM_PageHandle hndle = (char *)fInformation->pg_hndle.data;
    fixCount++;
    fInformation->var_count = *(int *)hndle;
    hndle = hndle + sizeof(int);
    fixCount = 1;
    fInformation->free_count = *(int *)hndle;
    hndle = hndle + sizeof(int);
    fixCount = 0;
    int k = *(int *)hndle;
    fixCount = 1;
    hndle += sizeof(int);
    tb = (Schema *)malloc(sizeof(Schema));
    fixCount++;
    tb->dataTypes = (DataType *)malloc(sizeof(DataType) * k);
    tb->typeLength = (int *)malloc(sizeof(int) * k);
    fixCount--;
    tb->numAttr = k;
    tb->attrNames = (char **)malloc(sizeof(char *) * k);
    fixCount = 0;
    for (int m = 0; m < k; m++)
    {
        fixCount++;
        tb->attrNames[m] = (char *)malloc(ele_n);
    }
    fixCount = 1;
    int i = 0;
    while (i < tb->numAttr)
    {
        fixCount = 0;
        strncpy(tb->attrNames[i], hndle, ele_n);
        hndle += ele_n;
        fixCount = 1;
        tb->dataTypes[i] = *(int *)hndle;
        hndle += sizeof(int);
        fixCount++;
        tb->typeLength[i] = *(int *)hndle;
        hndle += sizeof(int);
        fixCount--;
        i++;
    }
    fixCount = 0;
    rel->schema = tb;
    fixCount = 1;
    return hndle;
}

// update()
void update(RecordManagerModel *structRM)
{
    int updateCount = 0;
    structRM->tuple_count = 0;
    structRM->record_id.page = 1;
    updateCount = 1;
    structRM->record_id.slot = 0;
}

void functionUpdate(RecordManagerModel *structRM, RecordManagerModel *rmTable, Record *field, int capacity, int flag)
{
    int fucUpdateCount = 0;
    BM_BufferPool *const bufferMngr = &rmTable->buff_pool;
    BM_PageHandle *const page = &structRM->pg_hndle;
    fucUpdateCount++;
    SM_FileHandle f_handle;
    pinPage(bufferMngr, page, structRM->record_id.page);
    fucUpdateCount--;
    paraUpdate(structRM, field, capacity);
    structRM->tuple_count++;
    fucUpdateCount = 1;
    flag++;
}

void addData(RecordManagerModel *mngr, RID *rid, Record *field, char *pointer, char *k, int fieldCap)
{
    int addCount = 0;
    int m = rid->slot;
    int n = fieldCap;
    addCount++;
    BM_BufferPool *const mnger = &mngr->buff_pool;
    BM_PageHandle *const pg = &mngr->pg_hndle;
    addCount = 1;
    markDirty(mnger, pg);
    pointer = k + (m * n);
    addCount--;
    *pointer = '+';
    memcpy(++pointer, field->data + 1, fieldCap - 1);
    addCount = 0;
    unpinPage(mnger, pg);
    mngr->var_count += 1;
    addCount = 1;
    pinPage(mnger, pg, 0);
}

// Opens the table for operations
RC openTable(RM_TableData *rel, char *name)
{
    int openCount = 0;
    SM_PageHandle handle;
    openCount = 1;
    if (name == false || rel == false)
    {
        openCount++;
        return RC_ERROR;
    }
    else
    {
        openCount--;
        rel->name = name;
        rel->mgmtData = fInformation;
        openCount = 0;
        SM_FileHandle f_handle;
        BM_PageHandle *const pg = &fInformation->pg_hndle;
        openCount = 1;
        BM_BufferPool *const buff_pool = &fInformation->buff_pool;
        pinPage(buff_pool, pg, 0);
        openCount++;
        handle = fixSchema(rel);
        unpinPage(buff_pool, pg);
        openCount--;
        forcePage(buff_pool, pg);
    }
    openCount = 0;
    return RC_OK;
}

// searching the empty block
int searchEmptyBlock(int field, char *k)
{
    int searchCount = 0;
    int i = 0;
    int j = PAGE_SIZE / field;
    searchCount++;
    while (i < j)
    {
        searchCount = 0;
        if (k[i * field] != '+')
        {
            searchCount++;
            return i;
        }
        searchCount = 0;
        i++;
    }
    searchCount = 1;
    return -1;
}

// Frees the memory of the record manager data
RC shutdownRecordManager()
{
    int shutdownCount = 0;
    free(fInformation);
    shutdownCount = 1;
    return RC_OK;
}

// write to the pagefile
RC closeTable(RM_TableData *rel)
{
    int closeCount = 0;
    if (rel == false)
    {
        closeCount++;
        return RC_ERROR;
    }
    else if (rel == true)
    {
        closeCount--;
        rel->mgmtData = NULL;
        BM_BufferPool *const mnger = &fInformation->buff_pool;
        closeCount = 1;
        free(rel->schema);
        shutdownBufferPool(mnger);
        closeCount = 0;
    }
    closeCount = 1;
    return RC_OK;
}

// Creates a table which stores information about the schema
RC createTable(char *name, Schema *schema)
{
    int tableCount = 0;
    char lst[PAGE_SIZE];
    SM_FileHandle f_handle;
    tableCount++;
    ReplacementStrategy r_s = RS_LRU;
    char *buff_hndle = lst;
    tableCount--;
    const int pgs = ct_pg;
    fInformation = (RecordManagerModel *)malloc(sizeof(RecordManagerModel));
    tableCount = 1;
    initBufferPool(&fInformation->buff_pool, name, pgs, r_s, NULL);
    tableCount = 0;
    *(int *)buff_hndle = 0;
    buff_hndle += sizeof(int);
    tableCount++;
    *(int *)buff_hndle = 1;
    buff_hndle += sizeof(int);
    tableCount--;
    *(int *)buff_hndle = schema->numAttr;
    buff_hndle += sizeof(int);
    tableCount = 1;
    *(int *)buff_hndle = schema->keySize;
    buff_hndle += sizeof(int);
    tableCount = 0;
    int i = 0;
    while (schema->numAttr > i)
    {
        tableCount++;
        strncpy(buff_hndle, schema->attrNames[i], ele_n);
        buff_hndle = buff_hndle + ele_n;
        tableCount--;
        *(int *)buff_hndle = (int)schema->dataTypes[i];
        buff_hndle = buff_hndle + sizeof(int);
        tableCount = 1;
        *(int *)buff_hndle = (int)schema->typeLength[i];
        buff_hndle = buff_hndle + sizeof(int);
        tableCount = 0;
        i++;
    }
    tableCount = 0;
    fileOp(name, f_handle, lst);
    tableCount = 1;
    return RC_OK;
}

//deletes the table
RC deleteTable(char *name)
{
    int deleteCount = 0;
    if (name == ((char *)0))
    {
        deleteCount++;
        return RC_ERROR;
    }
    else if (name != ((char *)0))
    {
        deleteCount--;
        destroyPageFile(name);
    }
    deleteCount = 0;
    return RC_OK;
}

// record manager assigns an RID to this record
RC insertRecord(RM_TableData *rel, Record *record)
{
    int insertCount = 0;
    RecordManagerModel *record_mgr = rel->mgmtData;
    RID *pt = &record->id;
    insertCount++;
    char *q;
    pt->page = record_mgr->free_count;
    insertCount--;
    BM_BufferPool *const buff_pool = &record_mgr->buff_pool;
    const PageNumber pg = pt->page;
    insertCount = 1;
    pinPage(buff_pool, &record_mgr->pg_hndle, pg);
    char *p = record_mgr->pg_hndle.data;
    insertCount = 0;
    SM_FileHandle f_handle;
    pt->slot = searchEmptyBlock(getRecordSize(rel->schema), p);
    insertCount++;
    while (0 > pt->slot)
    {
        insertCount = 1;
        unpinPage(buff_pool, &record_mgr->pg_hndle);
        pt->page++;
        insertCount = 0;
        pinPage(buff_pool, &record_mgr->pg_hndle, pt->page);
        pt->slot = searchEmptyBlock(getRecordSize(rel->schema), record_mgr->pg_hndle.data);
        insertCount = 1;
    }
    insertCount = 0;
    addData(record_mgr, pt, record, q, record_mgr->pg_hndle.data, getRecordSize(rel->schema));
    insertCount = -1;
    return RC_OK;
}

// file operations
void fileOp(char *name, SM_FileHandle fileHandle, char *d)
{
    int fileCount = 0;
    SM_FileHandle *file_h = &fileHandle;
    createPageFile(name);
    fileCount++;
    SM_FileHandle *n_file = &file_h;
    openPageFile(name, file_h);
    fileCount--;
    writeBlock(0, file_h, d);
    fileCount = 1;
    closePageFile(file_h);
}

// paraUpdate
void paraUpdate(RecordManagerModel *structRM, Record *field, int capacity)
{
    int paraCount = 0;
    char *pointerName = field->data;
    *pointerName = '-';
    paraCount++;
    int slotNumber = field->id.slot;
    slotNumber = structRM->record_id.slot;
    paraCount = 1;
    int pageNumber = field->id.page;
    pageNumber = structRM->record_id.page;
    paraCount = 0;
    char *valueToCopy = structRM->pg_hndle.data;
    valueToCopy = valueToCopy + (structRM->record_id.slot * capacity);
    paraCount++;
    valueToCopy++;
    paraCount--;
    memcpy(++pointerName, valueToCopy, capacity - 1);
}

// Dealing with records and attribute Functions
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    int getCount = 0;
    int n = 0, k = 0;
    int *o = &n;
    getCount++;
    *o = 1;
    int i = 0;
    getCount--;
    int ct = 0;
    while (i < attrNum)
    {
        getCount = 1;
        if (schema->dataTypes[i] != DT_INT)
        {
            getCount = 0;
            *o += schema->typeLength[i];
        }
        else
        {
            getCount = 1;
            *o += sizeof(int);
        }
        getCount = 0;
        i++;
    }
    getCount = 1;
    Value *fd = (Value *)malloc(sizeof(Value));
    char *d_info = record->data;
    getCount++;
    d_info = d_info + n;
    if (attrNum == 1)
    {
        getCount = 1;
        schema->dataTypes[attrNum] = 1;
    }
    getCount = 0;
    if (schema->dataTypes[attrNum] != DT_INT)
    {
        getCount++;
        k = schema->typeLength[attrNum];
        fd->v.stringV = (char *)malloc(k + 1);
        getCount = 1;
        strncpy(fd->v.stringV, d_info, k);
        fd->dt = DT_STRING;
        getCount = 0;
        fd->v.stringV[k] = '\0';
        getCount = 1;
    }
    else if (schema->dataTypes[attrNum] == DT_INT)
    {
        getCount++;
        memcpy(&ct, d_info, sizeof(int));
        fd->dt = DT_INT;
        getCount--;
        fd->v.intV = ct;
    }
    getCount = 1;
    *value = fd;
    getCount = 0;
    return RC_OK;
}

//Initiates scanning
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    int startCount = 0;
    RecordManagerModel *m_data;
    RecordManagerModel *record_mgr;
    startCount++;
    if (cond == NULL)
    {
        startCount = 1;
        return RC_ERROR;
    }
    else
    {
        startCount = 0;
        openTable(rel, "ScanTable");
        record_mgr = (RecordManagerModel *)malloc(sizeof(RecordManagerModel));
        startCount++;
        scan->mgmtData = record_mgr;
        update(record_mgr);
        startCount--;
        m_data = rel->mgmtData;
        m_data->var_count = ele_n;
        startCount = 1;
        record_mgr->cond = cond;
        scan->rel = rel;
        startCount = 0;
        return RC_OK;
    }
    startCount = 0;
}

//returns number of tuples
int getNumTuples(RM_TableData *rel)
{
    int numCount = 0;
    return ((RecordManagerModel *)rel->mgmtData)->var_count;
}

// Returns the next tuple
RC next(RM_ScanHandle *scan, Record *record)
{
    int nextCount = 0;
    int fieldSize, numRows, blockCount, visited;
    Value *result = (Value *)malloc(sizeof(Value));
    nextCount++;
    RecordManagerModel *m_data = scan->mgmtData;
    RecordManagerModel *record_mgr = scan->rel->mgmtData;
    nextCount--;
    BM_PageHandle *const pg = &m_data->pg_hndle;
    int r_ct = record_mgr->var_count;
    nextCount = 1;
    BM_BufferPool *const buff_pool = &record_mgr->buff_pool;
    SM_FileHandle f_handle;
    nextCount = 0;
    Schema *sch = scan->rel->schema;
    if (m_data->cond != NULL)
    {
        nextCount++;
        if (r_ct == 0)
        {
            nextCount = 1;
            return RC_RM_NO_MORE_TUPLES;
        }
        while (r_ct >= m_data->tuple_count)
        {
            nextCount = 0;
            if (m_data->tuple_count > 0)
            {
                nextCount = 0;
                int slot = m_data->record_id.slot;
                m_data->record_id.slot = slot + 1;
                nextCount++;
                if (m_data->record_id.slot >= PAGE_SIZE / getRecordSize(sch))
                {
                    nextCount = 1;
                    m_data->record_id.slot = 0;
                    m_data->record_id.page = m_data->record_id.page + 1;
                    nextCount = 0;
                }
            }
            else
            {
                nextCount = 1;
                update(m_data);
            }
            nextCount = 0;
            functionUpdate(m_data, record_mgr, record, getRecordSize(sch), m_data->tuple_count);
            evalExpr(record, sch, m_data->cond, &result);
            nextCount++;
            if (result->v.boolV == TRUE)
            {
                nextCount = 1;
                unpinPage(buff_pool, pg);
                nextCount = 0;
                return RC_OK;
            }
            nextCount = 0;
        }
    }
    else
    {
        nextCount = 1;
        return RC_ERROR;
    }
    nextCount = 0;
    unpinPage(buff_pool, pg);
    update(m_data);
    nextCount = 0;
    return RC_RM_NO_MORE_TUPLES;
}

// Deletes the record from the table
RC deleteRecord(RM_TableData *rel, RID id)
{
    int deleteCount = 0;
    RecordManagerModel *mngr = rel->mgmtData;
    BM_PageHandle *const cp = &mngr->pg_hndle;
    deleteCount++;
    SM_FileHandle f_handle;
    BM_BufferPool *const b_mngr = &mngr->buff_pool;
    deleteCount--;
    pinPage(b_mngr, cp, id.page);
    mngr->free_count = id.page;
    deleteCount = 1;
    char *d = mngr->pg_hndle.data;
    d += (id.slot * (getRecordSize(rel->schema)));
    deleteCount = 0;
    *d = '-';
    markDirty(b_mngr, cp);
    deleteCount = 1;
    unpinPage(b_mngr, cp);
    deleteCount = -1;
    return RC_OK;
}

// Updates the record in the table
RC updateRecord(RM_TableData *rel, Record *record)
{
    int updateCount = 0;
    char *currPointer;
    RecordManagerModel *record_mgr = rel->mgmtData;
    updateCount++;
    SM_FileHandle f_handle;
    BM_PageHandle *const c_pg = &record_mgr->pg_hndle;
    updateCount = 1;
    BM_BufferPool *const buff_pool = &record_mgr->buff_pool;
    RID recordID = record->id;
    updateCount = 0;
    pinPage(buff_pool, c_pg, record->id.page);
    currPointer = record_mgr->pg_hndle.data;
    updateCount = 1;
    currPointer = currPointer + recordID.slot * getRecordSize(rel->schema);
    *currPointer = '+';
    updateCount = 0;
    memcpy(++currPointer, record->data + 1, (getRecordSize(rel->schema) - 1));
    markDirty(buff_pool, c_pg);
    updateCount++;
    unpinPage(buff_pool, c_pg);
    updateCount = 1;
    return RC_OK;
}


// This function creates the schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    int schemaCount = 0;
    int type = typeLength;
    Schema *sc_info = (Schema *)malloc(sizeof(Schema));
    schemaCount++;
    if (sc_info != NULL)
    {
        schemaCount = 1;
        sc_info->numAttr = numAttr;
        sc_info->keySize = keySize;
        schemaCount = 0;
        sc_info->typeLength = typeLength;
        sc_info->dataTypes = dataTypes;
        schemaCount = 0;
        sc_info->attrNames = attrNames;
        sc_info->keyAttrs = keys;
        schemaCount = 1;
        return sc_info;
    }
    else
    {
        schemaCount = 1;
        return sc_info;
    }
}

// Dealing with records and attribute Functions
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    int setCount = 0;
    int i = 0;
    int n = 0;
    setCount++;
    int *o = &n;
    *o = 1;
    setCount = 1;
    while (i < attrNum)
    {
        setCount = 0;
        if (schema->dataTypes[i] != DT_INT)
        {
            setCount = 0;
            *o += schema->typeLength[i];
            setCount = 1;
        }
        else
        {
            setCount = 0;
            *o += sizeof(int);
        }
        setCount = 1;
        i++;
    }
    setCount = 0;
    char *datainfo = record->data;
    datainfo += n;
    setCount = 1;
    if (schema->dataTypes[attrNum] != DT_INT)
    {
        setCount = 1;
        strncpy(datainfo, value->v.stringV, schema->typeLength[attrNum]);
        datainfo = datainfo + schema->typeLength[attrNum];
        setCount = 0;
    }
    else if (schema->dataTypes[attrNum] == DT_INT)
    {
        setCount = 1;
        *(int *)datainfo = value->v.intV;
        datainfo += sizeof(int);
        setCount = 0;
    }
    setCount = 1;
    return RC_OK;
}

// Fetches the record from the table
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    int recordCount = 0;
    RecordManagerModel *manage = rel->mgmtData;
    SM_FileHandle f_handle;
    recordCount = 1;
    BM_BufferPool *const buffManager = &manage->buff_pool;
    BM_PageHandle *const currPage = &manage->pg_hndle;
    recordCount++;
    pinPage(buffManager, currPage, id.page);
    char *pointer = manage->pg_hndle.data;
    recordCount--;
    pointer = pointer + id.slot * getRecordSize(rel->schema);
    if (*pointer == '+')
    {
        recordCount = 0;
        record->id = id;
        char *dPointer = record->data;
        recordCount = 1;
        int temp = getRecordSize(rel->schema) - 1;
        memcpy(++dPointer, pointer + 1, temp);
        recordCount++;
    }
    else
    {
        recordCount = 1;
        return RC_ERROR;
    }
    recordCount = 0;
    unpinPage(buffManager, currPage);
    recordCount = 1;
    return RC_OK;
}

// Gets the record size
int getRecordSize(Schema *schema)
{
    int getRecordCount = 0;
    int n = 0;
    int i = 0;
    getRecordCount = 1;
    while (i < schema->numAttr)
    {
        getRecordCount = 1;
        if (schema->dataTypes[i] != DT_INT)
        {
            getRecordCount++;
            n += schema->typeLength[i];
        }
        else if (DT_INT == schema->dataTypes[i])
        {
            getRecordCount--;
            n += sizeof(int);
        }
        getRecordCount = 0;
        i++;
    }
    getRecordCount = 1;
    n++;
    getRecordCount = 0;
    return n;
}

RC closeScan(RM_ScanHandle *scan)
{
    int closeCount = 0;
    SM_FileHandle f_handle;
    RecordManagerModel *pt = scan->rel->mgmtData;
    closeCount++;
    RecordManagerModel *record_mngr = scan->mgmtData;
    int count = record_mngr->free_count;
    closeCount--;
    BM_PageHandle *const pg = &record_mngr->pg_hndle;
    BM_BufferPool *const buff_pool = &pt->buff_pool;
    closeCount = 1;
    if (record_mngr->tuple_count > 0)
    {
        closeCount = 0;
        unpinPage(buff_pool, pg);
        update(record_mngr);
        closeCount = 1;
    }
    closeCount = 0;
    scan->mgmtData = NULL;
    free(scan->mgmtData);
    closeCount = 1;
    return RC_OK;
}

// Creates the new record for the given schema
RC createRecord(Record **record, Schema *schema)
{
    int createCount = 0;
    Record *curd = (Record *)malloc(sizeof(Record));
    char *rd = curd->data;
    createCount++;
    curd->data = (char *)malloc(getRecordSize(schema));
    char *cur_pt = curd->data;
    createCount--;
    *cur_pt = '-';
    char *pt = curd;
    createCount = 1;
    cur_pt++;
    *cur_pt = '\0';
    createCount = 0;
    *record = curd;
    createCount = 1;
    return RC_OK;
}

RC freeSchema(Schema *schema)
{
    int freeCount = 0;
    if (schema != NULL)
    {
        freeCount++;
        free(schema);
        freeCount = 0;
        return RC_OK;
    }
    freeCount = 1;
    return NULL;
}

// free record
RC freeRecord(Record *record)
{
    int recordCount = 0;
    if (record != NULL)
    {
        recordCount = 1;
        free(record);
        recordCount = 0;
        return RC_OK;
    }
    recordCount = 0;
    return NULL;
}