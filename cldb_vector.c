/*vectorize the commit I/Os by trying to fire contiguous I/Os
  to the DB by assembling chunks in the cache commit queue.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "cldb.h"
#include "cldb_io.h"
#include "dlist.h"
#include "cldb_cache.h"
#include "cldb_vector.h"

struct cl_db_vector *cl_db_vector_make(cldb_offset_t offset,long len,int flag) {
  struct cl_db_vector *vector = NULL;
  if(!(vector = malloc(sizeof(*vector)))) {
    output("Error in malloc...");
    goto out;
  }
  memset(vector,0x0,sizeof(*vector));
  vector->offset = offset;
  vector->len = len;
  vector->flag = flag;
  if(!(vector->data = malloc(len))) { 
    output("Error in malloc...");
    goto out_free;
  }
  goto out;
 out_free:
  free((void*)vector);
  vector = NULL;
 out:
  return vector;
}

int cl_db_vector_alloc(struct cl_db_queue *queue,cldb_offset_t offset,long len,int flag,char **ret) {
  struct cl_db_vector *vector = NULL;
  struct list_head *list ;
  int err = -1;
  char *data = NULL;

  if(!queue || len <= 0) {
    output("Invalid arg...");
    goto out;
  }
  list = &queue->queue;

  if(!LIST_EMPTY(list)) { 
    struct list_head *p = list->prev;
    vector = list_entry(p,struct cl_db_vector,queue);
    if((vector->offset + vector->len) != offset) {
      vector = NULL;
    }
  }
  if(!vector) {
    if(!(vector = cl_db_vector_make(offset,len,flag))) {
      output("Error in allocating vector");
      goto out;
    }
    list_add_tail(&vector->queue,list);
    ++queue->count;
    data = vector->data;
  } else {
    vector->data = realloc(vector->data,vector->len + len);
    if(!vector->data) { 
      output("Error in realloc...");
      goto out;
    }
    data = vector->data + vector->len;
    vector->len += len;
  }
  if(ret) 
    *ret = data;
  err = 0;
 out:
  return err;
}

static int cl_db_vector_walk(struct rbtree *root,int (*fun)(struct rbtree *node,void *arg),void *arg) { 
  int err = -1;
  if(!root) {
    err = 0;
    goto out;
  }
  err = cl_db_vector_walk(root->left,fun,arg);
  if(err < 0) {
    goto out;
  }
  err = (*fun)(root,arg);
  if(err < 0 ) {
    goto out;
  }
  err = cl_db_vector_walk(root->right,fun,arg);
 out:
  return err;
}

int cl_db_vector_delete(struct cl_db_queue *queue) { 
  struct list_head *list;
  int err = -1;
  if(!queue) {
    output("Invalid arg...");
    goto out;
  }
  list = &queue->queue;
  while(!LIST_EMPTY(list)) { 
    struct list_head *f = list->next;
    struct cl_db_vector *v = list_entry(f,struct cl_db_vector,queue);
    list_del(f);
    free((void*)v->data);
    free((void*)v);
  }
  queue->count = 0;
  err = 0;
 out:
  return err;
}

int cl_db_vector_destroy(struct cl_db_vector_queue *queue) { 
  int err = -1;
  if(!queue) {
    output("Invalid arg...");
    goto out;
  }
  err |= cl_db_vector_delete(&queue->ctrl_queue);
  err |= cl_db_vector_delete(&queue->data_queue);
 out:
  return err;
}

static int do_vector_io(int fd,struct cl_db_queue *queue) {
  int err = -1;
  struct list_head *list;

  if(!queue) { 
    output("Invalid arg...");
    goto out;
  }
  list =&queue->queue;
  output("\n %d Entries in the ctrl vector queue...",queue->count);

  while(!LIST_EMPTY(list)) {
    struct list_head *f = list->next;
    struct cl_db_vector *v = list_entry(f,struct cl_db_vector,queue);
    int bytes;
    bytes = cl_db_write(fd,v->data,v->len,v->offset);
    if(bytes != v->len) {
      err = -1;
      output("Error writing ctrl data at offset :"CLDB_OFFSET_FMT",len:%ld",v->offset,v->len);
    }
    list_del(f);
    free((void*)v->data);
    free((void*)v);
  }
  queue->count = 0;
  err = 0;
 out:
  return err;
}

/*Fire the vectored I/Os from the ctrl and data queue respectively*/
static int cl_db_vector_io(int handle,struct cl_db_vector_queue *queue) { 
  int fd;
  int err ;
  fd = CL_DB_FD(handle);
  err = do_vector_io(fd,&queue->ctrl_queue);
  if(err < 0 ) {
    output("Error in ctrl vector io...");
    goto out;
  }
  err = do_vector_io(fd,&queue->data_queue);
  if(err < 0 ) {
    output("Error in data vector io...");
  }
 out:
  return err;
}

static void do_vector_display(struct cl_db_queue *queue) { 
  struct list_head *list;
  register struct list_head *temp;
  list = &queue->queue;
  list_for_each(temp,list) { 
    struct cl_db_vector *v  = list_entry(temp,struct cl_db_vector,queue);
    fprintf(stderr,"\nVector data:%p,offset:"CLDB_OFFSET_FMT",len:%ld,flag:%d\n",v->data,v->offset,v->len,v->flag);
  }
}
  
void cl_db_vector_display(struct cl_db_vector_queue *queue) { 
  fprintf(stderr,"\nDumping ctrl queue %d entries\n",queue->ctrl_queue.count);
  do_vector_display(&queue->ctrl_queue);
  fprintf(stderr,"\nDumping data queue %d entries\n",queue->data_queue.count);
  do_vector_display(&queue->data_queue);
}

/*vectorize the I/Os*/
int cl_db_vector_commit(int handle,struct cl_db_commit_records *queue,long *amount) {
  int err = -1;
  struct cl_db_vector_queue vector_queue;
  struct cl_db_vector_args args ;
  if(!queue) {
    output("Invalid args..");
    goto out;
  }
  memset(&vector_queue,0x0,sizeof(vector_queue));
  init_list_head(&vector_queue.ctrl_queue.queue);
  init_list_head(&vector_queue.data_queue.queue);
  args.queue = &vector_queue;
  args.len = 0;
  err = cl_db_vector_walk(queue->root.root,cl_db_cache_vector_callback,(void*)&args);
  if(err < 0 ) {
    output("Error in vector walk...");
    goto out;
  }
  if(amount) 
    *amount = args.len;

  /*Fire the I/Os now*/
#ifdef DEBUG
  cl_db_vector_display(&vector_queue);
#endif

  err = cl_db_vector_io(handle,&vector_queue);
  if(err < 0 ) {
    output("Error in cl_db_vector_io...");
    goto out;
  }
  err = 0;
 out:
  return err;
}

