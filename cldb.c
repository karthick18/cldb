/*
  Database core API routines.
  a_r_karthic@users.sourceforge.net,a.r.karthick@gmail.com
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <fcntl.h>
#include <assert.h>
#include "cldb.h"
#include "cldb_pack.h"
#include "cldb_api.h"
#include "cl_rbtree.h"
#include "cldb_io.h"
#include "cldb_group.h"
#include "utils.h"

static int cl_db_find_record(int handle,struct cldb_data *,struct cl_db **);
struct cl_db_instances cl_db_instances[MAX_CL_DB_INSTANCES];


static int cl_db_header_init(int handle) { 
  int err = -1;
  struct cl_db_header *header = &CL_DB_HEADER(handle);
  long size;
  cldb_offset_t temp;
  char *pad = NULL;
  memset(header,0x0,sizeof(*header));
  header->magic = CLDB_MAGIC;
  header->version = CLDB_VERSION;
  header->offset = CLDB_HEADER_OFFSET;
  header->root = -1;
  header->first = -1;
  header->group = -1;
  header->last_group =-1;
  header->next_free_group = -1;
  header->free_group = -1;
  header->maxlen = -1;
  size = ALIGN_VAL(sizeof(struct cl_db_header), CLDB_BLOCK_SIZE);
  size -= sizeof(struct cl_db_header);
  pad = malloc(size);

  if(!pad) {
    output("Error in malloc...");
    goto out;
  }
  /*First format and write the header*/
  if(cl_db_header_write_fmt(handle,header,header->offset,0) < 0 ) { 
    output("Error in writing the header "CLDB_OFFSET_FMT,header->offset);
    goto out_free;
  }
  memset(pad,0x0,size);
  temp = header->offset + sizeof(struct cl_db_header);
  if(cl_db_write(CL_DB_FD(handle),pad,size,temp) < 0 ) {
    output("Error in writing to the DB at offset "CLDB_OFFSET_FMT,temp);
    goto out_free;
  }
  err = 0;
 out_free:
  if(pad)
    free((void*)pad);
 out:
  return err;
}

/*Start the commit phase for the init*/
static int cl_db_init(int handle) { 
  int err = -1;
  struct cl_db_header header ;

  if(cl_db_header_init(handle) < 0 ) {
    output("Error in writing header...");
    goto out;
  }

  /*Save the last header*/
  memcpy(&header,&CL_DB_HEADER(handle),sizeof(header));

  if(cl_db_commit_start(handle) < 0 ) {
    output("Error in starting commit..");
    goto out;
  }

  if(cl_db_group_init(handle) < 0 ) {
    output("Error in group init...");
    goto out_rollback;
  }

  if(cl_db_touch_header(handle) < 0 ) {
    output("Error in touching header...");
    goto out_rollback;
  }

  if(cl_db_commit_end(handle) < 0 ) { 
    output("Error in commit end...");
    goto out_rollback;
  }

  err = 0;
  goto out;

 out_rollback:
  /*Restore back the header*/
  memcpy(&CL_DB_HEADER(handle),&header,sizeof(struct cl_db_header));
  if(cl_db_commit_rollback(handle) < 0 ) {
    output("Error in commit rollback...");
  }
 out:
  return err;
}

/*Compare the keys*/
static int cl_db_compare(struct cl_db *new_node,struct cl_db *node) { 
  int diff;
  int l1= new_node->keylen,l2=node->keylen;
  if(l1 == l2) {
    diff =  memcmp(new_node->key,node->key,l1);
  } else if(l1 > l2) { 
    diff = memcmp(new_node->key,node->key,l2);
    if(!diff) 
      diff = 1;
  } else { 
    diff = memcmp(new_node->key,node->key,l1);
    if(!diff) 
      diff = -1;
  }
  return diff;
}

static int cl_db_match(struct cl_db *node1,struct cl_db *node2) { 
  return cl_db_compare(node1,node2);
}

static int cl_db_queue_record(int handle,struct cl_db *ptr,cldb_offset_t prev,cldb_offset_t next) { 
  struct cl_db prev_record,next_record;
  int err = -1;
  prev = prev ?: -1;
  next = next ?: -1;
  CL_DB_LAST_OFFSET(handle) = ptr->offset;
  if(prev != -1) { 
    if(cl_db_record_read_unfmt(handle,&prev_record,prev,CLDB_CTRL_DATA) < 0 ) {
      output("Error reading record");
      goto out;
    }
    prev_record.next = ptr->offset;
    CL_DB_TOUCH(handle,&prev_record,CLDB_CTRL_DATA);
  } else { 
    CL_DB_HEADER(handle).first = ptr->offset;
  }
  if(next != -1) { 
    if(cl_db_record_read_unfmt(handle,&next_record,next,CLDB_CTRL_DATA) < 0 ) { 
      output("Error reading record");
      goto out;
    }
    next_record.prev = ptr->offset;
    CL_DB_TOUCH(handle,&next_record,CLDB_CTRL_DATA);
  }
  ptr->next = next;
  ptr->prev = prev;
  output("link next ="CLDB_OFFSET_FMT",prev="CLDB_OFFSET_FMT",offset="CLDB_OFFSET_FMT"\n",next,prev,ptr->offset);
  err = 0;
 out:
  return err;
}

static int cl_db_dequeue_record(int handle,struct cl_db *entry) {
  int err = -1;
  cldb_offset_t prev,next;
  prev = entry->prev;
  next = entry->next;
  /*Check if this is the last linear entry*/
  if(entry->offset == CL_DB_LAST_OFFSET(handle)) {
    CL_DB_LAST_OFFSET(handle) = prev;
  }
  if(prev == -1) { 
    CL_DB_HEADER(handle).first = next;
  } else {
    struct cl_db prev_record;
    if(cl_db_record_read_unfmt(handle,&prev_record,prev,CLDB_CTRL_DATA) < 0 ) {
      output("Error in read");
      goto out;
    }
    prev_record.next = next;
    CL_DB_TOUCH(handle,&prev_record,CLDB_CTRL_DATA);
  }
  if(next != -1) { 
    struct cl_db next_record;
    if(cl_db_record_read_unfmt(handle,&next_record,next,CLDB_CTRL_DATA) < 0 ) { 
      output("Error in read");
      goto out;
    }
    next_record.prev = prev;
    CL_DB_TOUCH(handle,&next_record,CLDB_CTRL_DATA);
  }
  err = 0;
 out:
  return err;
}

/*TODO when group implementation is through:*/
int cl_db_free_offset(int handle,struct cl_db *entry) { 
  int err = -1;
  if(!entry) { 
    goto out;
  }
  /*Recycle this guy back to the group*/
  err = cl_db_group_release_offset(handle,entry);
  if(err < 0 ) {
    output("Error in releasing offset "CLDB_OFFSET_FMT,entry->offset);
    goto out;
  }
  err = 0;
 out:
  return err;
}

int cl_db_get_free_offset(int handle,struct cl_db *ptr) { 
  cldb_offset_t offset = -1;
  cldb_offset_t data_offset=-1,group_offset =-1;
  long maxlen = -1;
  long datalen = 0;
  int err = -1;
  if(!CL_DB_HANDLE_VALID(handle)) { 
    output("Handle invalid");
    goto out;
  }
  if(!ptr || !ptr->key || !ptr->data) { 
    output("Data invalid");
    goto out;
  }
  datalen = ptr->keylen + ptr->datalen;
  offset = cl_db_group_get_offset(handle,datalen,&data_offset,&group_offset,&maxlen);
  if(offset < 0 ) {
    output("Error in getting offset...");
    goto out;
  }
  ptr->offset = offset;
  ptr->data_offset = data_offset;
  ptr->group = group_offset;
  ptr->maxlen = maxlen;
  
  output("Record offset= "CLDB_OFFSET_FMT",data_offset="CLDB_OFFSET_FMT",group offset="CLDB_OFFSET_FMT",maxlen=%ld\n",offset,data_offset,group_offset,maxlen);
  err = 0;
 out:
  return err;
}

int cl_db_touch_header(int handle) {
  struct cl_db_header *ptr = &CL_DB_HEADER(handle);
  ptr->cksum = 0; 
  ptr->magic = CLDB_MAGIC;
  ptr->version = CLDB_VERSION;
  if(!ptr->root) ptr->root = -1;
  if(!ptr->first) ptr->first = -1;
  if(!ptr->last_offset) ptr->last_offset = -1;
  if(cl_db_touch(handle,(void*)ptr,sizeof(*ptr),0,&cl_db_header_operations) < 0 ) { 
    output("Error touching header...");
    return -1;
  }
  return 0;
}

int cl_db_touch_node(int handle,struct cl_db *ptr) { 
  struct cl_db record;
  int err = -1;
  if(cl_db_record_read_unfmt(handle,&record,ptr->offset,CLDB_CTRL_DATA)) {
    output("Error in record read");
    goto out;
  }
  record.keylen = ptr->keylen;
  record.key = ptr->key;
  record.datalen = ptr->datalen;
  record.data =  ptr->data;
  record.group = ptr->group;
  record.data_offset = ptr->data_offset;
  record.maxlen = ptr->maxlen;
  record.next = ptr->next;
  record.prev = ptr->prev;
  CL_DB_TOUCH(handle,&record,CLDB_DATA_ALL);
  err = 0;
 out:
  return err;
}

/*Touch would commit this record*/
int cl_db_touch_record(int handle,struct cl_db *ptr,int flag) { 
  long rec_size = sizeof(*ptr);
  int err;
  if( (flag & CLDB_KEY_DATA) && (flag & CLDB_DATA)) {
    rec_size += ptr->keylen + ptr->datalen;
  }
  if((err = cl_db_touch(handle,(void *)ptr,rec_size,flag,&cl_db_operations)) < 0 ) {
    output("Error in touching record at offset "CLDB_OFFSET_FMT,ptr->offset);
  }
  return err;
}

/*Read the header for the db*/
int cl_db_read_header(int handle) { 
  struct cl_db_header *header = &CL_DB_HEADER(handle);
  int err = -1;
  struct cl_db_header cl_db_header ;

  if(cl_db_header_read_unfmt(handle,&cl_db_header,CLDB_HEADER_OFFSET,0) < 0 ) {
    output("Error in reading header");
    goto out;
  }
  if(cl_db_header.magic != CLDB_MAGIC) { 
    output("Error in header magic:%#08x\n",cl_db_header.magic);
    goto out;
  }
  memcpy(header,&cl_db_header,sizeof(*header));
  err = 0;
 out:
  return err;
}

#ifdef ROOT_DEBUG
static int cl_db_display_root(int handle) { 
  struct cl_db record;
  int err = -1;
  if(cl_db_record_read_unfmt(handle,&record,CL_DB_ROOT(handle),CLDB_DATA_ALL) < 0 ) {
    output("Error reading root");
    goto out;
  }
  cl_db_display((void*)&record);
  err = 0;
 out:
  return err;
}
#else
#define cl_db_display_root(handle) ({ 0; })
#endif
/*
  Add into the rbtree thats on-disk
*/
static int cl_db_link_record(int handle,struct cl_db *ptr) { 
  cldb_offset_t parent = -1;
  int go_left = -1;
  int err = -1;
  cldb_offset_t root = CL_DB_ROOT(handle) ;
  struct cl_db_header old_header;

  /*First take a copy of the header*/
  memcpy(&old_header,&CL_DB_HEADER(handle),sizeof(old_header));

  root = root ?:-1;

  while(root != -1) {
    struct cl_db record;
    int cmp;
    parent = root;
    if(cl_db_record_read_unfmt(handle,&record,parent,CLDB_DATA_ALL) < 0) { 
      output("Error reading record");
      goto out;
    }
    cmp = cl_db_compare(ptr,&record);
    if(!cmp) { 
      output("Found a duplicate key for the record...");
      err = CLDB_DUPLICATE;
      goto out;
    }
    if(cmp < 0) {
      go_left = 1;
      root = record.left;
    } else {
      go_left = 0;
      root = record.right;
    }
    free((void*)record.key);
    free((void*)record.data);
  }

  if(cl_db_commit_start(handle) < 0 ) {
    output("Error in starting commit..");
    goto out;
  }

  /*First get the offsets first*/
  if(cl_db_get_free_offset(handle,ptr) < 0 ) { 
    output("Error in getting free offset...");
    goto out_rollback;
  }

  if(cl_rbtree_insert(handle,ptr,parent,go_left) < 0 ) {
    output("Error in rbtree insert");
    goto out_rollback;
  }

  if(cl_db_queue_record(handle,ptr,CL_DB_LAST_OFFSET(handle),-1) < 0 ) {
    output("Error in queueing record");
    goto out_rollback;
  }

  if(cl_db_touch_node(handle,ptr) < 0 ) { 
    output("Error touching node");
    goto out_rollback;
  }

  ++CL_DB_HEADER(handle).records;
    
  if(cl_db_touch_header(handle) < 0 ) { 
    output("Error updating header");
    goto out_rollback;
  }

  if(cl_db_commit_end(handle) < 0 ) {
    output("Error committing the records to the disk...");
    goto out;
  }

  err = 0;
  cl_db_display_root(handle);
  goto out;

 out_rollback:
  /*Restore the old header*/
  output("Restoring the old db header...");
  memcpy(&CL_DB_HEADER(handle),&old_header,sizeof(old_header));

  if(cl_db_commit_rollback(handle) < 0 ) {
    output("Error in commit rollback...");
  }

 out:
  return err;
}

static __inline__ void cl_db_fetch(struct cldb_data *data,struct cl_db *ptr) {
  if(!data || !ptr)
    return;
  data->keylen = ptr->keylen;
  data->key = cl_dup(ptr->key,ptr->keylen);
  data->datalen = ptr->datalen;
  data->data = cl_dup(ptr->data,ptr->datalen);
}

static int cl_db_add_record(int handle,unsigned int keylen,unsigned char *key,unsigned int datalen,unsigned char *data) {
  struct cl_db node;
  int err = -1;
  memset(&node,0x0,sizeof(node));
  node.keylen = keylen;
  node.key = key;
  node.datalen = datalen;
  node.data = data;
  err = cl_db_link_record(handle,&node);
  if(err) { 
    output("Error linking record...");
    goto out;
  }
  if(datalen > CL_DB_STATS(handle).max_record) 
    CL_DB_STATS(handle).max_record = datalen;
  if(keylen > CL_DB_STATS(handle).max_key)
    CL_DB_STATS(handle).max_key = keylen;
  err = 0;
 out:
  return err;
}

static int cl_db_find_record(int handle,struct cldb_data *data,struct cl_db **ptr) {
  struct cl_db node = {0};
  cldb_offset_t root = CL_DB_ROOT(handle);
  int err = -1;

  if(!data || !data->key || !data->keylen) {
    output("Invalid param to find record");
    goto out;
  }
  node.key = data->key;
  node.keylen = data->keylen;
  root = root ?: -1;
  while(root != -1) { 
    int cmp;
    struct cl_db record = {0};
    if(cl_db_record_read_unfmt(handle,&record,root,CLDB_DATA_ALL) < 0 ) {
      output("Error in read");
      goto out;
    }
    cmp = cl_db_match(&node,&record);
    if(cmp != 0) {
      free((void*)record.key) ;
      free((void*)record.data) ;
    }
    if(cmp < 0 ) {
      root = record.left;
    } else if(cmp > 0 ) {
      root = record.right;
    } else {
      CL_DB_SEARCH_HITS(handle);
      err = 0;
      data->data = record.data;
      data->datalen = record.datalen;
      if(ptr) { 
        *ptr = (struct cl_db *)cl_dup((unsigned char *)&record,sizeof(record));
      }
      goto out;
    }
  }
  CL_DB_SEARCH_MISSES(handle);
 out:
  return err;
}

int cl_db_find(int handle,unsigned int keylen,unsigned char *key,struct cldb_data *data) {
  int err = -1;

  if(!CL_DB_HANDLE_VALID(handle)) {
    output("Handle passed is invalid");
    goto out;
  }
  if(!data) { 
    output("Data passed is NULL");
    goto out;
  }
  data->key = key;
  data->keylen = keylen;
  data->data = NULL;
  data->datalen = 0;
  err = cl_db_find_record(handle,data,NULL);

  if(err < 0) { 
    output("Unable to locate record with key:%.*s",keylen,key);
    goto out;
  }
  if(!data->data) { 
    err = -1;
    output("Find error");
    goto out;
  }
 out:
  return err;
}

static int cl_db_del_record(int handle,unsigned int keylen,unsigned char *key) { 
  int err = -1;
  struct cl_db *entry = NULL;
  struct cldb_data data = {0};
  struct cl_db_header old_header;

  data.key = key;
  data.keylen = keylen;

  if(cl_db_find_record(handle,&data,&entry) < 0 ) {
    output("Error finding the record for deletion:%.*s..",keylen,key);
    goto out;
  }

  memcpy(&old_header,&CL_DB_HEADER(handle),sizeof(old_header));

  if(cl_db_commit_start(handle) < 0 ) {
    output("Error in starting the commit for delete...");
    goto out;
  }

  if(cl_rbtree_erase(handle,entry) < 0 ) { 
    output("Error in rbtree erase for key:%.*s...",keylen,key);
    goto out_rollback;
  }

  if(cl_db_dequeue_record(handle,entry) < 0 ) {
    output("Error in dequeuing record for key:%.*s...",keylen,key);
    goto out_rollback;
  }

  if(cl_db_free_offset(handle,entry) < 0 ) {
    output("Error in releasing offset...");
    goto out_rollback;
  }

  --CL_DB_HEADER(handle).records;

  /*Touch the header now*/
  if(cl_db_touch_header(handle) < 0 ) {
    output("Error in touching the header while deletion for key:%.*s...",keylen,key);
    goto out_rollback;
  }

  if(cl_db_commit_end(handle) < 0 ) {
    output("Error in commit end during deletion...");
    goto out;
  }

  /*Delete the cache footprint*/
  cl_db_cache_del(handle,(void*)entry,&cl_db_operations);
  err = 0;
  goto out;

 out_rollback:
  output("Restoring back the old header...");
  memcpy(&CL_DB_HEADER(handle),&old_header,sizeof(old_header));
  if(cl_db_commit_rollback(handle) < 0 ) {
    output("Error in rollback during deletion...");
  }

 out:
  if(entry) {
    if(entry->key) 
      free((void*)entry->key);
    if(entry->data) 
      free((void*)entry->data);
    free((void*)entry);
  }
  return err;
}

static int cl_db_update_record(int handle,struct cldb_data *data) {
  int err = -1;

  if(!CL_DB_HANDLE_VALID(handle)) { 
    output("Handle passed is invalid");
    goto out;
  }

  /*We delete the record and add a new one.
    Basically sometime down the line, will have some headroom
    in the records data to accomodate for such cases.
  */

  if((err = cl_db_del_record(handle,data->keylen,data->key)) < 0 ) {
    output("Error deleting record during updation");
    goto out;
  }

  if((err = cl_db_add_record(handle,data->keylen,data->key,data->datalen,data->data))) {
    output("Error adding record during updation");
  }
 out:
  return err;
}

int cl_db_read_db(int handle) {
  int err = -1;
  if(cl_db_read_header(handle) < 0 ) {
    output("Error in reading header");
    goto out;
  }
  err = 0;
 out:
  return err;
}

static void cl_db_handle_free(int handle) { 
  if(handle < 0 || handle > MAX_CL_DB_INSTANCES)
    return;
  cl_db_instances[handle].inuse = 0;
}

static int cl_db_handle_get(void) {
  register int i;
  for(i = 0; i < MAX_CL_DB_INSTANCES;++i) {
    if(!cl_db_instances[i].inuse) {
      cl_db_instances[i].inuse = 1;
      return i;
    }
  }
  return -1;
}

static int cl_db_inuse(const char *name) { 
  register int i;
  for(i = 0; i < MAX_CL_DB_INSTANCES;++i) {
    if(cl_db_instances[i].inuse && strcmp(name,cl_db_instances[i].name) == 0 )
      return 1;
  }
  return 0;
}

static int convert_flags(int flags) { 
  int mode = 0;
  if((flags & CLDB_READ)) 
    mode |= O_RDONLY;
  if((flags & CLDB_WRITE)) {
    if((flags & CLDB_READ)) 
      mode = O_RDWR;
    else
      mode = O_WRONLY;
  }
  if((flags & CLDB_CREAT)) 
    mode |= O_CREAT;
  if((flags & CLDB_TRUNC))
    mode |= O_TRUNC;
  if((flags & CLDB_APPEND)) { 
    mode |= O_APPEND;
  }
  return mode;
}

int cl_db_open(const char *name,int flags) {
  int err = -1;
  int fd;
  int handle;
  int mode = 0;
  if(!name) 
    goto out;

  /*Check if already open*/
  if(cl_db_inuse(name)) {
    output("Database %s already in use",name);
    goto out;
  }

  mode = convert_flags(flags);
  fd = open(name,mode,0644);
  if(fd < 0 ) {
    if((flags & CLDB_APPEND) && !(flags & CLDB_CREAT)) { 
      flags |= CLDB_CREAT | CLDB_TRUNC;
      mode |= O_CREAT | O_TRUNC;
      fd = open(name,mode,0644);
    }
    if(fd < 0 ) {
      output("Error in opening file %s with flags:%d",name,flags);
      goto out;
    }
  }
  handle = cl_db_handle_get();
  if(handle < 0 ) {
    output("CL DB instances reached maximum limit of %d",MAX_CL_DB_INSTANCES);
    goto out;
  }
  strncpy(cl_db_instances[handle].name,name,sizeof(cl_db_instances[handle].name)-1);
  cl_db_instances[handle].fd = fd;
  cl_db_cache_init(handle);

  if(!(flags & CLDB_CREAT)) {
    if(cl_db_read_db(handle) < 0) {
      output("Error in reading the database:%s",name);
      close(fd);
      cl_db_handle_free(handle);
      goto out;
    }
  } else { 
    cl_db_init(handle);
  }
  err = handle;
 out:
  return err;
}

int cl_db_close(int handle) { 
  int err = -1;
  char buf[0xff+1] = {0};
  (void)buf;
  output("Max record for process:%d(%s) = %d,Max key=%d,Record count=%ld,Hits:%d,Misses:%d",getpid(),get_process(buf),CL_DB_STATS(handle).max_record,CL_DB_STATS(handle).max_key,CL_DB_HEADER(handle).records,CL_DB_STATS(handle).search_hits,CL_DB_STATS(handle).search_misses);

  if(!CL_DB_HANDLE_VALID(handle)) {
    output("Handle passed to close is invalid");
    goto out;
  }

  cl_db_handle_free(handle);
  cl_db_cache_destroy(handle);
  err = 0;
 out:
  return err;
}

int cl_db_add(int handle,struct cldb_data *data) {
  int err = -1;

  if(!CL_DB_HANDLE_VALID(handle)) {
    output("Invalid handle");
    goto out;
  }

  if(!data || !data->keylen || !data->datalen || !data->key || !data->data) { 
    output("Data is NULL");
    goto out;
  }

  if(!cl_db_instances[handle].inuse) {
    output("Handle not in use");
    goto out;
  }

  if((err = cl_db_add_record(handle,data->keylen,data->key,data->datalen,data->data))) {
    output("Error in adding record");
  }
 out:
  return err;
}


int cl_db_del(int handle,unsigned int keylen,unsigned char *key) { 
  int err = -1;
  if(!CL_DB_HANDLE_VALID(handle)) {
    output("Handle passed is invalid to del");
    goto out;
  }
  err = cl_db_del_record(handle,keylen,key);
  if(err < 0 ) {
    output("Error in deleting the record");
  }

 out:
  return err;
}

int cl_db_update(int handle,struct cldb_data *data) {
  int err = -1;

  if(!CL_DB_HANDLE_VALID(handle)) { 
    output("Handle passed is invalid");
    goto out;
  }

  if(!data || !data->key || !data->keylen) {
    output("Invalid param to update");
    goto out;
  }

  if((err = cl_db_update_record(handle,data))) {
    output("Error in update record");
  }

 out:
  return err;
}

int cl_db_first_record(int handle,struct cldb_data *data) { 
  int err = -1;
  struct cl_db node = {0};
  cldb_offset_t first;
  
  if(!CL_DB_HANDLE_VALID(handle)) { 
    output("Handle passed is invalid");
    goto out;
  }

  if(!data) {
    output("Invalid param");
    goto out;
  }
  
  first = CL_DB_HEADER(handle).first;

  if(first == -1) { 
    output("Database empty...");
    err = CLDB_END;
    goto out;
  }

  if((err = cl_db_record_read_unfmt(handle,&node,first,CLDB_DATA_ALL)) < 0 ) {
    output("Error reading the first record at offset:"CLDB_OFFSET_FMT,first);
    goto out;
  }

  data->key = node.key;
  data->keylen = node.keylen;
  data->data = node.data;
  data->datalen = node.datalen;
  err = 0;
 out:
  return err;
}

/*Get the next record given the previous key*/
int cl_db_next_record(int handle,struct cldb_data *data) { 
  struct cl_db node;
  struct cl_db *entry;
  int err = -1;
  
  if(!CL_DB_HANDLE_VALID(handle)) {
    output("Handle passed is invalid");
    goto out;
  }

  if(!data || !data->key) { 
    output("Invalid param");
    goto out;
  }
 
  if((err = cl_db_find_record(handle,data,&entry)) < 0 ) {
    output("Error finding record");
    goto out;
  }

  if(entry->next == -1) {
    err = CLDB_END;
    goto out;
  }

  /*Now get the next offset*/
  if((err = cl_db_record_read_unfmt(handle,&node,entry->next,CLDB_DATA_ALL)) < 0 ) {
    output("Error reading record at offset :"CLDB_OFFSET_FMT,entry->next);
    goto out_free;
  }

  data->key = node.key;
  data->keylen = node.keylen;
  data->data = node.data;
  data->datalen = node.datalen;
  err = 0;

 out_free:
  /*Free the entry data now*/
  if(entry) { 
    free((void*)entry->key);
    free((void*)entry->data);
    free((void*)entry);
  }
 out:
  return err;
}

static int cl_db_free_data(int handle,void *data) { 
  int err = -1;
  if(!CL_DB_HANDLE_VALID(handle)) {
    output("Handle passed is invalid");
    goto out;
  }
  if(!data) {
    output("Data is NULL");
    goto out;
  }
  free(data);
  err = 0;
 out:
  return err;
}

int cl_db_free_record(int handle,void *data) {
  return cl_db_free_data(handle,data);
}

int cl_db_free_key(int handle,void *data) { 
  return cl_db_free_data(handle,data);
}

void cl_db_display(void *data) { 
  struct cl_db *ptr = (struct cl_db *)data;
  fprintf(stderr,"Keylen:%d,Key:%.*s,Datalen:%d,Data:%.*s\n",ptr->keylen,ptr->keylen,ptr->key,ptr->datalen,ptr->datalen,ptr->data);
  free((void*)ptr->key); free((void*)ptr->data);
}

static void cl_db_traverse(int handle, cldb_offset_t root, void (*display)(void *)) { 
  struct cl_db record;
  if(root <= 0) 
    return;
  if(cl_db_record_read_unfmt(handle,&record,root,CLDB_DATA_ALL) < 0 ) {
    output("Error in record read");
    return;
  }
  cl_db_traverse(handle,record.left,display);
  display(&record);
  cl_db_traverse(handle,record.right,display);

}

int cl_db_dump_tree(int handle,void (*display)(void *)) { 
  cldb_offset_t root = CL_DB_ROOT(handle);
  if(!display) display = cl_db_display;
#ifdef ROOT_DEBUG
  if(root>=0) {
    fprintf(stderr,"\nRoot offset="CLDB_OFFSET_FMT"\n",root);
  }
#endif
  cl_db_traverse(handle,root,display);
  return 0;
}

int cl_db_dump_list(int handle) { 
  cldb_offset_t temp = CL_DB_HEADER(handle).first;
  struct cl_db record;
  int err = -1;
  while(temp != -1 ) { 
    if(cl_db_record_read_unfmt(handle,&record,temp,CLDB_DATA_ALL) < 0 ) {
      output("Error in cl db read");
      goto out;
    }
    temp = record.next;
    cl_db_display((void*)&record);
  }
  err = 0;
 out:
  return err;
}
