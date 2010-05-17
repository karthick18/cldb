#ifndef _CLDB_VECTOR_H
#define _CLDB_VECTOR_H

#include "dlist.h"

struct cl_db_queue { 
  struct list_head queue;
  int count;
};

struct cl_db_vector { 
  char *data; /*contiguous data*/
  cldb_offset_t offset; /*offset of the data*/
  long len; /*len to stream*/
  int flag;
  struct list_head queue;
};

struct cl_db_vector_queue { 
  struct cl_db_queue ctrl_queue;
  struct cl_db_queue data_queue;
};

struct cl_db_vector_args { 
  struct cl_db_vector_queue *queue;
  long len;/*total record len*/
};

extern int cl_db_vector_alloc(struct cl_db_queue *queue,cldb_offset_t offset,long len,int flag,char **ret);
extern struct cl_db_vector *cl_db_vector_make(cldb_offset_t offset,long len,int flag);
extern int cl_db_vector_commit(int handle,struct cl_db_commit_records *,long *amount);
#endif
