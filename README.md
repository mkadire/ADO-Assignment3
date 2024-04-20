### Assignment 3 (CS-525) Record Manager

### Developed by 
A20547463 : Abhinay Deekonda
A20549975 : Mahendra Reddy Kadire 
A20546275 : Kandimalla Praneeth Reddy

### Assignment description:

The goal of this assignment is to implement a simple record manager. The record manager handles tables with a
fixed schema. Clients can insert records, delete records, update records, and scan through the records in a table.
A scan is associated with a search condition and only returns records that match the search condition. Each table
should be stored in a separate page file and your record manager should access the pages of the file through the
buffer manager implemented in the last assignment.

### How to Run code:

Run the following commands on a linux System. 
Go to the assignment directory in the git repository and then run the following code.

```    
    Step 1 : $ make clean
    Step 2 : $ make

    To run test case of test_assign3_1 
    Step 3 : $ make run

    To run test case of test_expr
    Step 4 : $ make ./test_assign3_1
     
```

###  There are five types of functions in the record manager. They are:-
1. functions for table and record manager management,
2. functions for handling the records in a table,
3. functions related to scans,
4. functions for dealing with schemas, and
5. function for dealing with attribute values and creating records.

### initRecordManager(): 
    - to initialize storage manager and record manager.

### shutdownRecordManager():
    - shutdown the record manager which we created
    - free assigned memory for that 

### createContent():
    - this is helper method to create content while creating table

### openSchemaTable():
    - this is helper function to fill the schema of table

### createTable():
    - creating table with below three function of storage manager and buffer manager
    - 1. initialize the buffer pool
    - 2. create page file for table with given respective name
    - 3. open page file to write content
    - 4. write the block with created content
    - 5. close the file

### openTable():
    - for open table
    - 1. put page in buffer pool using pinPage buffer manager functions
    - 2. open schema table using helper message
    - 3. remove from buffer pool
    - 4. forcefully write to the disk

### closeTable():
    - close table will free all memory and run shutdown record manager functions

### deleteTable():
    - delete given table

### getNumTuples():
    - return numbers of tuples in given table

## Record Functions :
    - These functions are used to retrieve a record with a certain RID , to delete a record with a
certain RID , to insert a new record, and to update an existing record with new values. When a new record is inserted
the record manager should assign an RID to this record and update the record parameter passed to insertRecord .

### insertRecord(): 
    - to insert record will follow below steps
    - 1. put page in buffer pool
    - 2. insert record to page
    - 3. mark page as modified 
    - 4. remove from the buffer pool

### deleteRecord():
    - 1. put page in buffer pool
    - 2. delete record to page
    - 3. mark page as modified 
    - 4. remove from the buffer pool

### updateRecord():
    - 1. put page in buffer pool
    - 2. update record to page
    - 3. mark page as modified 
    - 4. remove from the buffer pool

### getRecord():
    - 1. put page in buffer pool
    - 2. copy record in new table manager
    - 4. remove from the buffer pool

## Scan Functions : 
    - A client can initiate a scan to retrieve all tuples from a table that fulfill a certain condition (represented as an Expr ). Starting a scan initializes the RM ScanHandle data structure passed as an argument to startScan . Afterwards, calls to the next method should return the next tuple that fulfills the scan condition. If NULL is passed as a scan condition, then all tuples of the table should be returned. next should return RC RM NO MORE TUPLES once the scan is completed and RC OK otherwise (unless an error occurs of course) Below is an example of how a client can use a scan.

### startScan():
    - create two table manager scan and pass to others
    - if there is no condition then it will return not condition code

### next():
    - return record which fullfill the current conditon
    - if there is no condition then it will return not condition code
    - if there are no records then return error code of no more records

### closeScan() 
    - close the scan opeation 
    - check scan has done or not using table's metadata of scan count
    - if it is more than 0 that means it was incompleted
    - and free the space 

## Schema Functions:

### getRecordSize()
    - to retrieves the size of a record in the given schema.

### freeSchema()
    - free the space of schema which we allocated earliar