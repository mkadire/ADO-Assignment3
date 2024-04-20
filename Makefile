CC := gcc
SRC := dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c record_mgr.c expr.c rm_serializer.c test_assign3_1.c
OBJ := dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o expr.o rm_serializer.o test_assignment_3.o
test_assignment_3: $(OBJ)
	$(CC) -o test_assignment_3 $(SRC)
$(OBJ): $(SRC)
	$(CC) -g -c $(SRC)
run: test_assignment_3
	./test_assignment_3
clean:
	rm -rf test_table_t test_assignment_3 *.o