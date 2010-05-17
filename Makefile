CC = gcc
SRCS = cl_rbtree.c cldb.c cldb_fmt.c cldb_group.c cldb_group_cache.c cldb_io.c utils.c cldb_cache.c cldb_commit.c cldb_vector.c rbtree.c 
OBJS = $(SRCS:%.c=%.o)
EXTRA_CFLAGS= $(DEBUG)
DEBUG = -g  #-DDEBUG #  -DGROUP_SLOW 
CFLAGS=-Wall  $(EXTRA_CFLAGS)
LIB_TARGETS=libcldb.a
TARGETS = cldb_test_misc cldb_add cldb_update cldb_del cldb_find cldb_test cldb_test_find cldb_dump cldb_record_get gdbm_test 
TARGET = $(LIB_TARGETS) $(TARGETS)
AR=ar
RANLIB=ranlib

all: $(TARGET) 
install:
	install $(LIB_TARGETS) /usr/lib -m 0644

libcldb.a: $(OBJS)
	@ ( \
		$(AR) cr $@ $^;\
		$(RANLIB) $@;\
	)

cldb_test_misc: cldb_test_misc.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_add: cldb_add.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_record_get: cldb_record_get.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_update: cldb_update.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_del: cldb_del.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_test_find: cldb_test_find.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_test: cldb_test.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_find: cldb_find.o
	$(CC) -o $@ $^ -L./. -lcldb

cldb_dump:cldb_dump.o
	$(CC) -o $@ $^ -L./. -lcldb

gdbm_test: gdbm_test.o
	$(CC) -o $@ $^ -L./. -lgdbm

%.o:%.c
	$(CC) -c $(CFLAGS) -o $@ $<

%.i:%.c
	$(CC) -E $(CFLAGS) $< > $@

clean:
	rm -f $(TARGET) $(OBJS) *~ *.i *.o $(TARGETS)
