#ifndef _CLDB_IO_H
#define _CLDB_IO_H

#define CLDB_BLOCK_SIZE (512)
extern int cl_db_read(int fd,char *,int,cldb_offset_t);
extern int cl_db_write(int fd,char *,int,cldb_offset_t);

#endif
