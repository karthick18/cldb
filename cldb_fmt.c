/*Database formatting routines
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "cldb.h"
#include "cldb_pack.h"
#include "utils.h"
#include "cldb_io.h"
#include "cldb_group.h"
#include "cldb_cache.h"
#include "cldb_vector.h"

#define CLDB_CHAR_TYPE (0x1)
#define CLDB_SHORT_TYPE (0x2)
#define CLDB_INT_TYPE (0x3)
#define CLDB_LONG_TYPE (0x4)
#define CLDB_OFFSET_TYPE (0x5)
#define CLDB_STR_TYPE (0x6)
#define CLDB_VARCHAR_TYPE (0x7)

struct cl_db_field_type { 
  int type;
  unsigned char *s;
  unsigned char *d;
  union { 
    unsigned char v;
  } char_t;
  union {
    unsigned short v;
  } short_t;
  union { 
    unsigned int v;
  }int_t;
  union { 
    unsigned long v;
  }long_t;
  union { 
    cldb_offset_t v;
  }offset_t;
  union {
    unsigned char *v;
  } str_t;
};

#define CLDB_CHAR_FIELD  char_t.v
#define CLDB_SHORT_FIELD short_t.v
#define CLDB_INT_FIELD   int_t.v
#define CLDB_LONG_FIELD  long_t.v
#define CLDB_OFFSET_FIELD offset_t.v
#define CLDB_STR_FIELD  str_t.v


struct cl_db_fields { 
#define CLDB_PACK 0x1
#define CLDB_UNPACK 0x2
  const char *name; /*field name*/
  int type; /*field type*/
  int size; /*size of the field*/
  int offset; /*field offset*/
  int (*fmt)(void *,int) ; /*format routine for that field*/
};

static int cl_db_fmt(void *,int);
static int cl_db_fmt_group_array(void *,int);

static struct cl_db_fields cl_db_record_fields[] = { 
  { STR(offset),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db,offset),CLDB_OFFSETOF(struct cl_db,offset),cl_db_fmt },
  { STR(left), CLDB_OFFSET_TYPE, CLDB_SIZEOF(struct cl_db,left), CLDB_OFFSETOF(struct cl_db,left), cl_db_fmt },
  { STR(right),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db,right),CLDB_OFFSETOF(struct cl_db,right),cl_db_fmt },
  { STR(parent),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db,parent),CLDB_OFFSETOF(struct cl_db,parent),cl_db_fmt },
  { STR(colour),CLDB_CHAR_TYPE,CLDB_SIZEOF(struct cl_db,colour),CLDB_OFFSETOF(struct cl_db,colour),cl_db_fmt },
  { STR(next),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db,next),CLDB_OFFSETOF(struct cl_db,next),cl_db_fmt },
  { STR(prev),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db,prev),CLDB_OFFSETOF(struct cl_db,prev), cl_db_fmt },
  { STR(group),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db,group),CLDB_OFFSETOF(struct cl_db,group), cl_db_fmt },
  { STR(keylen),CLDB_INT_TYPE,CLDB_SIZEOF(struct cl_db,keylen),CLDB_OFFSETOF(struct cl_db,keylen),cl_db_fmt},
  { STR(datalen),CLDB_INT_TYPE,CLDB_SIZEOF(struct cl_db,datalen),CLDB_OFFSETOF(struct cl_db,datalen),cl_db_fmt },
  { STR(maxlen),CLDB_LONG_TYPE,CLDB_SIZEOF(struct cl_db,maxlen),CLDB_OFFSETOF(struct cl_db,maxlen),cl_db_fmt },
  { STR(data_offset),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db,data_offset),CLDB_OFFSETOF(struct cl_db,data_offset),cl_db_fmt },
  { STR(cksum),CLDB_SHORT_TYPE,CLDB_SIZEOF(struct cl_db,cksum),CLDB_OFFSETOF(struct cl_db,cksum), cl_db_fmt },
  { STR(key),CLDB_STR_TYPE,CLDB_SIZEOF(struct cl_db,key),CLDB_OFFSETOF(struct cl_db,key),cl_db_fmt },
  { STR(data),CLDB_STR_TYPE,CLDB_SIZEOF(struct cl_db,data),CLDB_OFFSETOF(struct cl_db,data),cl_db_fmt },
  { NULL },
};
      
static struct cl_db_fields cl_db_header_fields[] = { 
  { STR(offset),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,offset),CLDB_OFFSETOF(struct cl_db_header,offset),cl_db_fmt},
  { STR(magic),CLDB_INT_TYPE,CLDB_SIZEOF(struct cl_db_header,magic),CLDB_OFFSETOF(struct cl_db_header,magic),cl_db_fmt},
  { STR(version),CLDB_INT_TYPE,CLDB_SIZEOF(struct cl_db_header,version),CLDB_OFFSETOF(struct cl_db_header,version),cl_db_fmt },
  { STR(records),CLDB_LONG_TYPE,CLDB_SIZEOF(struct cl_db_header,records),CLDB_OFFSETOF(struct cl_db_header,records),cl_db_fmt },
  { STR(last_offset),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,last_offset),CLDB_OFFSETOF(struct cl_db_header,last_offset),cl_db_fmt},
  { STR(root),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,root),CLDB_OFFSETOF(struct cl_db_header,root),cl_db_fmt},
  { STR(first),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,first),CLDB_OFFSETOF(struct cl_db_header,first),cl_db_fmt },
  { STR(group),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,group),CLDB_OFFSETOF(struct cl_db_header,group),cl_db_fmt },
  { STR(last_group),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,last_group),CLDB_OFFSETOF(struct cl_db_header,last_group),cl_db_fmt },
  { STR(next_free_group),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,next_free_group),CLDB_OFFSETOF(struct cl_db_header,next_free_group),cl_db_fmt },
  { STR(free_group),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_header,free_group),CLDB_OFFSETOF(struct cl_db_header,free_group),cl_db_fmt },
  { STR(maxlen),CLDB_LONG_TYPE,CLDB_SIZEOF(struct cl_db_header,maxlen),CLDB_OFFSETOF(struct cl_db_header,maxlen),cl_db_fmt },
  { STR(groups),CLDB_LONG_TYPE,CLDB_SIZEOF(struct cl_db_header,groups),CLDB_OFFSETOF(struct cl_db_header,groups),cl_db_fmt },
  { STR(cksum),CLDB_SHORT_TYPE,CLDB_SIZEOF(struct cl_db_header,cksum),CLDB_OFFSETOF(struct cl_db_header,cksum),cl_db_fmt},
  { NULL },
};

static struct cl_db_fields cl_db_group_fields[] = { 
  { STR(offset),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_group,offset),CLDB_OFFSETOF(struct cl_db_group,offset),cl_db_fmt },
  { STR(next),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_group,next),CLDB_OFFSETOF(struct cl_db_group,next),cl_db_fmt},
  { STR(free),CLDB_SHORT_TYPE,CLDB_SIZEOF(struct cl_db_group,free),CLDB_OFFSETOF(struct cl_db_group,free),cl_db_fmt },
  { STR(last_free),CLDB_SHORT_TYPE,CLDB_SIZEOF(struct cl_db_group,last_free),CLDB_OFFSETOF(struct cl_db_group,last_free),cl_db_fmt },
  { STR(tail),CLDB_SHORT_TYPE,CLDB_SIZEOF(struct cl_db_group,tail),CLDB_OFFSETOF(struct cl_db_group,tail),cl_db_fmt },
  { STR(valid),CLDB_SHORT_TYPE,CLDB_SIZEOF(struct cl_db_group,valid),CLDB_OFFSETOF(struct cl_db_group,valid),cl_db_fmt },
  { STR(next_data_offset),CLDB_OFFSET_TYPE,CLDB_SIZEOF(struct cl_db_group,next_data_offset),CLDB_OFFSETOF(struct cl_db_group,next_data_offset),cl_db_fmt },
  { STR(maxlen),CLDB_LONG_TYPE,CLDB_SIZEOF(struct cl_db_group,maxlen),CLDB_OFFSETOF(struct cl_db_group,maxlen),cl_db_fmt },
  { STR(array),CLDB_VARCHAR_TYPE,CLDB_SIZEOF(struct cl_db_group,array),CLDB_OFFSETOF(struct cl_db_group,array),cl_db_fmt_group_array },
  { STR(cksum),CLDB_SHORT_TYPE,CLDB_SIZEOF(struct cl_db_group,cksum),CLDB_OFFSETOF(struct cl_db_group,cksum),cl_db_fmt},
  {NULL},
};

/*The cl_db struct operation routines*/

static void *cl_db_ctor(void *node,int flag,long *size) {
  struct cl_db *record;
  struct cl_db *ptr = (struct cl_db *)node;
  long rec_size = 0;
  record = malloc(sizeof(*record));
  if(!record) { 
    output("Error in allocating record...");
    goto out;
  }
  memset(record,0x0,sizeof(*record));
  memcpy(record,ptr,CLDB_CTRL_BYTES);
  rec_size += CLDB_CTRL_BYTES;
  if((flag & CLDB_KEY_DATA) && ptr->key) {
    if (!(record->key = cl_dup(ptr->key,ptr->keylen))) { 
      output("Error duping record key...");
      goto out_free;
    }
    rec_size += ptr->keylen;
  }
  if((flag & CLDB_DATA) && ptr->data) {
    if(!(record->data = cl_dup(ptr->data,ptr->datalen))) {
      output("Error duping record data...");
      goto out_free;
    }
    rec_size += ptr->datalen;
  }
  if(size)
    *size = rec_size;
  goto out;
 out_free:
  if(record->key)
    free((void*)record->key);
  if(record->data)
    free((void*)record->data);
  free((void*)record);
  record = NULL;
 out:
  return (void*)record;
}

static int cl_db_dtor(void *record) { 
  struct cl_db *ptr = (struct cl_db*)record;
  if(ptr->key) 
    free((void*)ptr->key);
  if(ptr->data)
    free((void*)ptr->data);
  free(record);
  return 0;
}

static int cl_db_find(void *a,void *b) { 
  return ((struct cl_db*)a)->offset ==  ( (struct cl_db *)b)->offset;
}

static unsigned long cl_db_hash_routine(void *record) { 
  struct cl_db *ptr = (struct cl_db *)record;
  return (unsigned long)cl_db_hash_offset(ptr->offset,HASH_TABLE_BITS);
}

static int cl_db_cmp(void *a,void *b) { 
  return ((struct cl_db *)a)->offset - ((struct cl_db *)b)->offset;
}

static int cl_db_sync(void *cache_record,void *record,int flag,int update_data,long *size)  {
  struct cl_db *cache = (struct cl_db *)cache_record;
  struct cl_db *ptr = (struct cl_db *)record;
  int err = -1;
  long data_size = 0;
  long old_size = 0;

  if(cache->key && cache->data) {
    old_size = cache->keylen + cache->datalen;
  }

  if((flag & CLDB_CTRL_DATA)) {
    memcpy(cache_record,record,CLDB_CTRL_BYTES);
    if( (flag & CLDB_CTRL_DATA) == flag) { 
      err = 0;
      goto out;
    }
  }

  if(ptr->key && ptr->data) {

    if(!cache->key && !cache->data) { 

      if(!(cache->key = cl_dup(ptr->key,ptr->keylen))) { 
        output("Error duplicating key");
        goto out;
      }
      if(!(cache->data = cl_dup(ptr->data,ptr->datalen))) {
        output("Error duplicating data");
        goto out;
      }
      data_size = ptr->keylen + ptr->datalen;
    } else if(update_data && cache->key && cache->data) {
      free((void*)cache->key);
      free((void*)cache->data);
      cache->key = cache->data = NULL;
      if(!(cache->key = cl_dup(ptr->key,ptr->keylen))) {
        output("Error in duplicating key");
        if(size) { 
          *size = -old_size;
          goto out;
        }
      }
      if(!(cache->data = cl_dup(ptr->data,ptr->datalen))) {
        output("Error in duplicating data");
        free((void*)cache->key);
        cache->key = NULL;
        if(size) { 
          *size = -old_size;
        }
        goto out;
      }
      data_size =  (ptr->keylen + ptr->datalen - old_size);
    }
  }
  err = 0;
  if(size && data_size) 
    *size = data_size;
out:
  return err;
}

static int cl_db_commit(int handle,void *record,int flag) { 
  struct cl_db *ptr = (struct cl_db *)record;
  int err;
  err = cl_db_record_write_fmt(handle,ptr,ptr->offset,flag | CLDB_SYNC);
  if(err < 0 ) {
    output("Error in writing record at offset "CLDB_OFFSET_FMT,ptr->offset);
  }
  return err;
}

static int cl_db_vector(void *record,long len,int flag,void *arg) { 
  struct cl_db_vector_queue *queue;
  struct cl_db *ptr;
  struct cl_db buffer = { 0 };
  int err = -1;
  char *data = NULL;
  if(!record || !arg) { 
    output("Error in vectorizing...");
    goto out;
  }
  ptr = (struct cl_db *)record;
  queue = ((struct cl_db_vector_args *)arg)->queue;
  
  if((flag & CLDB_CTRL_DATA)) { 
    /*first fmt. the record*/
    if((err = cl_db_record_fmt(ptr,(unsigned char *)&buffer)) < 0 ) { 
      output("Error in formatting record...");
      goto out;
    }
    if((err = cl_db_vector_alloc(&queue->ctrl_queue,ptr->offset,CLDB_CTRL_BYTES,flag,&data)) < 0 ) { 
      output("Error in ctrl vector alloc...");
      goto out;
    }
    memcpy(data,&buffer,CLDB_CTRL_BYTES);
  }

  if((flag & CLDB_KEY_DATA) && (flag & CLDB_DATA)) { 
    long len = ptr->keylen + ptr->datalen;
    if(data)
      data = NULL;
    if((err = cl_db_vector_alloc(&queue->data_queue,ptr->data_offset,len,flag,&data)) < 0 ) { 
      output("Error in data vector alloc...");
      goto out;
    }
    memcpy(data,ptr->key,ptr->keylen);
    memcpy(data+ptr->keylen,ptr->data,ptr->datalen);
  }
  err = 0;
 out:
  return err;
}

struct cl_db_operations cl_db_operations = { 
  .ctor = cl_db_ctor,
  .dtor = cl_db_dtor,
  .find = cl_db_find,
  .sync = cl_db_sync,
  .cmp =  cl_db_cmp,
  .hash = cl_db_hash_routine,
  .display = cl_db_display,
  .commit = cl_db_commit,
  .vector = cl_db_vector,
};

static void *cl_db_header_ctor(void *node,int flag,long *size) { 
  if(size) 
    *size = sizeof(struct cl_db_header);
  return (void*)cl_dup((unsigned char *)node,sizeof(struct cl_db_header));
}

static int cl_db_header_dtor(void *ptr) { 
  if(ptr) 
    free(ptr);
  return 0;
}

/*Just update the cache header*/
static int cl_db_header_sync(void *cache_node,void *node,int flag,int update_data,long *size) {
  memcpy(cache_node,node,sizeof(struct cl_db_header));
  return 0;
}

static void cl_db_header_display(void *node) { 
  struct cl_db_header *ptr = (struct cl_db_header*)node;
  (void)ptr;
  output("Header offset = "CLDB_OFFSET_FMT,ptr->offset);
  output("Header root offset = "CLDB_OFFSET_FMT,ptr->root);
  output("Header list start offset = "CLDB_OFFSET_FMT,ptr->first);
}

static int cl_db_header_commit(int handle,void *node,int flag) { 
  int err ;
  struct cl_db_header *ptr = (struct cl_db_header*)node;
  if( (err = cl_db_header_write_fmt(handle,node,ptr->offset,flag | CLDB_SYNC) ) < 0 ) {
    output("Error in header write for offset "CLDB_OFFSET_FMT,ptr->offset);
  }
  return err;
}

static int cl_db_header_vector(void *record,long len,int flag,void *arg) {
  struct cl_db_vector_queue *queue;
  struct cl_db_header *header;
  struct cl_db_header buffer = {0};
  int err = -1;
  char *data = NULL;

  if(!record || !arg) { 
    output("Invalid arg...");
    goto out;
  }
  queue = ((struct cl_db_vector_args *)arg)->queue;
  header = (struct cl_db_header *)record;
  /*first fmt. the header*/
  if((err = cl_db_header_fmt(header,(unsigned char*)&buffer)) < 0) {
    output("Error in formatting header...");
    goto out;
  }
  if((err = cl_db_vector_alloc(&queue->ctrl_queue,header->offset,sizeof(*header),flag,&data)) < 0 ) {
    output("Error in header vector alloc...");
    goto out;
  }
  memcpy(data,&buffer,sizeof(buffer));
  err = 0;
 out:
  return err;
}

struct cl_db_operations cl_db_header_operations = { 
  .ctor = cl_db_header_ctor,
  .dtor = cl_db_header_dtor,
  .display=cl_db_header_display,
  .find = cl_db_find,
  .hash = cl_db_hash_routine,
  .sync = cl_db_header_sync,
  .cmp = cl_db_cmp,
  .commit = cl_db_header_commit,
  .vector = cl_db_header_vector,
};



static void *cl_db_group_ctor(void *node,int flag,long *size) { 
  if(size)
    *size = sizeof(struct cl_db_group);
  return (void*)cl_dup((unsigned char *)node,sizeof(struct cl_db_group));
}

static int cl_db_group_dtor(void *ptr) { 
  if(ptr) 
    free(ptr);
  return 0;
}

/*Just update the cache header*/
static int cl_db_group_sync(void *cache_node,void *node,int flag,int update_data,long *size) {
  memcpy(cache_node,node,sizeof(struct cl_db_group));
  return 0;
}

static void cl_db_group_display(void *node) { 
  struct cl_db_group *ptr = (struct cl_db_group*)node;
  (void)ptr;
  output("Group offset = "CLDB_OFFSET_FMT,ptr->offset);
  output("Next group offset = "CLDB_OFFSET_FMT,ptr->next);
  output("Group maxlen = %ld",ptr->maxlen);
}

static int cl_db_group_commit(int handle,void *node,int flag) { 
  int err ;
  struct cl_db_group *ptr = (struct cl_db_group*)node;
  if( (err = cl_db_group_write_fmt(handle,node,ptr->offset,flag | CLDB_SYNC) ) < 0 ) {
    output("Error in group write for offset "CLDB_OFFSET_FMT,ptr->offset);
  }
  return err;
}

static int cl_db_group_vector(void *record,long len,int flag,void *arg) { 
  struct cl_db_vector_queue *queue;
  struct cl_db_group *group;
  struct cl_db_group buffer = {0};
  int err = -1;
  char *data = NULL;
  if(!record || !arg) {
    output("Invalid arg...");
    goto out;
  }
  queue = ((struct cl_db_vector_args *)arg)->queue;
  group = (struct cl_db_group *)record;
  /*first fmt the record*/
  if((err = cl_db_group_fmt(group,(unsigned char *)&buffer)) < 0 ) {
    output("Error in formatting group...");
    goto out;
  }
  if((err = cl_db_vector_alloc(&queue->ctrl_queue,group->offset,sizeof(*group),flag,&data)) < 0 ) {
    output("Error in group vector alloc...");
    goto out;
  }
  memcpy(data,&buffer,sizeof(buffer));
  err = 0;
 out:
  return err;
}

struct cl_db_operations cl_db_group_operations = { 
  .ctor = cl_db_group_ctor,
  .dtor = cl_db_group_dtor,
  .display=cl_db_group_display,
  .find = cl_db_find,
  .hash = cl_db_hash_routine,
  .sync = cl_db_group_sync,
  .cmp = cl_db_cmp,
  .commit = cl_db_group_commit,
  .vector = cl_db_group_vector,
};

static int cl_db_fmt_group_array(void *field,int type) { 
  struct cl_db_field_type *f = (struct cl_db_field_type *)field;
  register int i;
  register char *s,*d;
  int err = -1;
  int size = CLDB_SIZEOF(struct cl_db_group,array);
  if(!f->s || !f->d) {
    output("Error in field arg");
    goto out;
  }
  for(d = f->d,s=f->s,i = 0; i < size;s+=sizeof(short),d+=sizeof(short),i+=sizeof(short)) { 
    switch(type) { 
    case CLDB_PACK:
      {
	short v = *(short*)d;
	CLDB_PACK_16(v,s);
      }
      break;
    case CLDB_UNPACK:
      {
	short v;
	CLDB_UNPACK_16(v,s);
	*(short *)d = v;
      }
      break;
    default:
      output("Invalid type");
      goto out;
    }
  }
  err = 0;
 out:
  return err;
}

static int cl_db_fmt(void *s,int type) { 
  struct cl_db_field_type *f = (struct cl_db_field_type*)s;
  int err = -1;
  if(!f) 
    goto out;
  switch(f->type) { 
  case CLDB_CHAR_TYPE:
    switch(type) { 
    case CLDB_PACK: 
      CLDB_PACK_8(f->CLDB_CHAR_FIELD,f->s);
      break;
    case CLDB_UNPACK:
      CLDB_UNPACK_8(f->CLDB_CHAR_FIELD,f->s);
      break;
    default:
      output("Error in type:%d",type);
      goto out;
    }
    break;
  case CLDB_SHORT_TYPE:
    switch(type) { 
    case CLDB_PACK:
      CLDB_PACK_16(f->CLDB_SHORT_FIELD,f->s);
      break;
    case CLDB_UNPACK:
      CLDB_UNPACK_16(f->CLDB_SHORT_FIELD,f->s);
      break;
    default:
      output("Error in type:%d",type);
      goto out;
    }
    break;
  case CLDB_INT_TYPE:
    switch(type) { 
    case CLDB_PACK:
      CLDB_PACK_32(f->CLDB_INT_FIELD,f->s);
      break;
    case CLDB_UNPACK:
      CLDB_UNPACK_32(f->CLDB_INT_FIELD,f->s);
      break;
    default:
      output("Error in type:%d",type);
      goto out;
    }
    break;
  case CLDB_LONG_TYPE:
    switch(type) { 
    case CLDB_PACK:
      CLDB_PACK_LONG(f->CLDB_LONG_FIELD,f->s);
      break;
    case CLDB_UNPACK:
      CLDB_UNPACK_LONG(f->CLDB_LONG_FIELD,f->s);
      break;
    default:
      output("Error in type:%d",type);
      goto out;
    }
    break;
  case CLDB_OFFSET_TYPE:
    switch(type) { 
    case CLDB_PACK:
      CLDB_PACK_64(f->CLDB_OFFSET_FIELD,f->s);
      break;
    case CLDB_UNPACK:
      CLDB_UNPACK_64(f->CLDB_OFFSET_FIELD,f->s);
      break;
    default:
      output("Error in type:%d",type);
      goto out;
    }
    break;
  case CLDB_STR_TYPE:
    break;
  default:
    output("Error in cldb field type:%d",f->type);
    goto out;
  }
  err = 0;
 out:
  return err;
}

static int do_cl_db_fmt(struct cl_db_fields *fields,void *ptr,unsigned char *s) { 
  int err = -1;
  register int i;
  register unsigned char *d;
  unsigned char *base;
  base = (unsigned char *)ptr;
  d = base;
  if(!d || !s) {
    output("Error in fmt");
    goto out;
  }
  for(i = 0; fields[i].name && ( d = base+fields[i].offset ) ; ++i) { 
    struct cl_db_field_type f ;
    f.type = fields[i].type;
    f.s = s + fields[i].offset;
    f.d = d;

    switch(fields[i].type) {
    case CLDB_CHAR_TYPE:
      f.CLDB_CHAR_FIELD = *(unsigned char *)d;
      break;
    case CLDB_SHORT_TYPE:
      f.CLDB_SHORT_FIELD = *(unsigned short *)d;
      break;
    case CLDB_INT_TYPE:
      f.CLDB_INT_FIELD = *(unsigned int *)d;
      break;
    case CLDB_LONG_TYPE:
      f.CLDB_LONG_FIELD = *(unsigned long *)d;
      break;
    case CLDB_OFFSET_TYPE:
      f.CLDB_OFFSET_FIELD = *(cldb_offset_t *)d;
      break;
    case CLDB_STR_TYPE:
      f.CLDB_STR_FIELD = d;
      break;
    case CLDB_VARCHAR_TYPE:
      /*Let the formatting handler handle it*/
      break;
    default:
      output("Unrecognised field type:%d",fields[i].type);
      goto out;
    }

    if(fields[i].fmt && fields[i].fmt((void *)&f,CLDB_PACK)) {
      output("Error in packing for field:%s",fields[i].name);
      goto out;
    }
  }
  err = 0;
 out:
  return err;
}

/*Unpack to a cl_db record*/
static int do_cl_db_unfmt(struct cl_db_fields *fields,void *ptr,unsigned char *s) { 
  int err = -1;
  register int i;
  unsigned char *base = (unsigned char *)ptr;
  register unsigned char *d;
  d = base;
  if(!d || !s) {
    output("Error in fmt");
    goto out;
  }

  for(i = 0; fields[i].name && ( d = base + fields[i].offset ) ; ++i) { 
    struct cl_db_field_type f ;
    f.type = fields[i].type;
    f.s = s + fields[i].offset;
    f.d = d;
    if(fields[i].fmt && (err = fields[i].fmt((void*)&f,CLDB_UNPACK))) { 
      output("Error in unpacking db");
      goto out;
    }
    if(!fields[i].fmt) 
      continue;

    switch(fields[i].type) {
    case CLDB_CHAR_TYPE:
      *(unsigned char *)d = f.CLDB_CHAR_FIELD;
      break;
    case CLDB_SHORT_TYPE:
      *(unsigned short *)d = f.CLDB_SHORT_FIELD ;
      break;
    case CLDB_INT_TYPE:
      *(unsigned int *)d = f.CLDB_INT_FIELD;
      break;
    case CLDB_LONG_TYPE:
      *(unsigned long *)d = f.CLDB_LONG_FIELD;
      break;
    case CLDB_OFFSET_TYPE:
      *(cldb_offset_t *)d = f.CLDB_OFFSET_FIELD;
      break;
    case CLDB_STR_TYPE:
    case CLDB_VARCHAR_TYPE:
      break;
      
    default:
      output("Unrecognised field type:%d",fields[i].type);
      goto out;
    }
  }
  err = 0;
 out:
  return err;
}

/*Let the above guy RIP through formatting and unformatting.
  Thats a feature isnt it:-)
*/

int cl_db_record_fmt(struct cl_db *ptr,unsigned char *s) { 
  ptr->cksum = cldb_cksum((unsigned char *)ptr,(unsigned int)CLDB_OFFSETOF(struct cl_db,cksum));
  return do_cl_db_fmt(cl_db_record_fields,(void*)ptr,s);
}

int cl_db_header_fmt(struct cl_db_header *ptr,unsigned char *s) { 
  ptr->cksum = cldb_cksum((unsigned char *)ptr,(unsigned int)CLDB_OFFSETOF(struct cl_db_header,cksum));
  return do_cl_db_fmt(cl_db_header_fields,(void*)ptr,s);
}

int cl_db_group_fmt(struct cl_db_group *ptr,unsigned char *s) { 
  ptr->cksum = cldb_cksum((unsigned char *)ptr,(unsigned int)CLDB_OFFSETOF(struct cl_db_group,cksum));
  return do_cl_db_fmt(cl_db_group_fields,(void *)ptr,s);
}

int cl_db_group_unfmt(struct cl_db_group *ptr,unsigned char *s) { 
  int err = -1;
  unsigned short csum;
  if((err = do_cl_db_unfmt(cl_db_group_fields,(void*)ptr,s)) < 0) { 
    output("Error in unformatting cl_db_group");
    goto out;
  }
  err = -1;
  csum = cldb_cksum((unsigned char *)ptr,(unsigned int)CLDB_OFFSETOF(struct cl_db_group,cksum));
  if(csum != ptr->cksum) { 
    output("CKSUM INTEGRITY failure on cl_db_group");
    goto out;
  }
  err = 0;
 out:
  return err;
}

int cl_db_record_unfmt(struct cl_db *ptr,unsigned char *s) { 
  int err = -1;
  unsigned short csum;
  if((err = do_cl_db_unfmt(cl_db_record_fields,(void*)ptr,s)) < 0 ) {
    output("Error in cl_db_unfmt");
    goto out;
  }
  err = -1;
  csum = cldb_cksum((unsigned char *)ptr,(unsigned int)CLDB_OFFSETOF(struct cl_db,cksum));
  if(csum != ptr->cksum) { 
    output("CKSUM integrity check failure for cl_db record");
    goto out;
  } 
  err = 0;
 out:
  return err;
}

int cl_db_header_unfmt(struct cl_db_header *ptr,unsigned char *s) { 
  int err = -1;
  unsigned short csum;
  if((err = do_cl_db_unfmt(cl_db_header_fields,(void*)ptr,s)) < 0 ) {
    output("Errror in unformatting data");
    goto out;
  }
  err = -1;
  csum = cldb_cksum((unsigned char *)ptr,(unsigned int)CLDB_OFFSETOF(struct cl_db_header,cksum));

  if(csum != ptr->cksum) {
    output("CKSUM integrity error for header");
    goto out;
  }
  if(CLDB_MAGIC != ptr->magic) { 
    output("Header magic corrupted");
    goto out;
  }
  err = 0;
 out:
  return err;
}
	
/*Reads a record from the database and unformats it */
int cl_db_record_read_unfmt(int handle,struct cl_db *ptr,cldb_offset_t offset,int flag) { 
  int err = -1;
  unsigned char *buf;
  int bytes;
  int fd;
  int update = 0;
  struct cl_db record = {0};
  struct cl_db *cache;
  struct cl_db node = { .offset = offset };
  if(!ptr) { 
    output("Error in read_unfmt");
    goto out;
  }
  fd = CL_DB_FD(handle);

  if((cache = cl_db_cache_find(handle,(void*)&node,&cl_db_operations))) { 

    if((flag & (CLDB_KEY_DATA | CLDB_DATA))) {
      if(!cache->key || !cache->data) { 
        /*
          We have to read the data and sync the cache
        */
        update = 1;
        goto start_read;
      }
    }

    if((flag & CLDB_CTRL_DATA)) {
      memcpy(ptr,cache,CLDB_CTRL_BYTES);
    }

    if((flag & CLDB_KEY_DATA)) { 
      if(!(ptr->key = cl_dup(cache->key,cache->keylen))) { 
        output("Error duplicating key");
        goto out;
      }
    }

    if((flag & CLDB_DATA)) {
      if(!(ptr->data = cl_dup(cache->data,cache->datalen))) {
        output("Error duplicating data");
        if(ptr->key) {
          free((void*)ptr->key);
          ptr->key = NULL;
        }
        goto out;
      }
    }
    err = 0;
    goto out;
  }
    
  /*First check the cache*/
start_read:
  if((flag & CLDB_CTRL_DATA)) {
    buf = (unsigned char *)&record;
    /*Read the control data*/
    bytes = cl_db_read(fd,(char *)buf,CLDB_CTRL_BYTES,offset);

    if(bytes <= 0) {
      output("Error in cl_db_read:");
      goto out;
    }
    
    if((err = cl_db_record_unfmt(ptr,buf)) < 0) { 
      output("Error in unfmt\n");
      goto out;
    }
    offset = ptr->data_offset;
    if(!(flag & CLDB_KEY_DATA) && (flag & CLDB_DATA)) 
      offset += ptr->keylen;
  }

  ptr->key = NULL;
  ptr->data = NULL;

  if((flag & CLDB_KEY_DATA)) {
    if(!ptr->keylen) { 
      output("Keylen zero");
      goto out;
    }
    ptr->key = (unsigned char*)malloc(ptr->keylen);
    if(!ptr->key) {
      output("Error mallocing key of len:%d",ptr->keylen);
      goto out;
    }
    if((err = cl_db_read(fd,ptr->key,ptr->keylen,offset)) < 0 ) { 
      output("Error reading key");
      goto out_free;
    }
    /*Data follows us. So start the data read without seeking*/
    offset = -1;
  }
  if((flag & CLDB_DATA)) {
    ptr->data = (unsigned char *)malloc(ptr->datalen);
    if(!ptr->data) { 
      output("Error mallocing data of len:%d",ptr->datalen);
      goto out_free;
    }

    if((err = cl_db_read(fd,ptr->data,ptr->datalen,offset)) < 0) { 
      output("Error reading data");
      goto out_free;
    }
    offset = -1;
  }

  /*Now add/sync the cache*/
  if(!update) {
    cl_db_cache_add(handle,(void*)ptr,flag,NULL,&cl_db_operations);
  } else {
    memcpy(ptr,cache,CLDB_CTRL_BYTES);
    cl_db_cache_sync(handle,(void *)ptr,CLDB_KEY_DATA | CLDB_DATA,0,NULL,&cl_db_operations);
  }
  goto out;
out_free:
  if(ptr->key) 
    free((void*)ptr->key);
  if(ptr->data)
    free((void*)ptr->data);
out:
  return err;
}

int cl_db_header_read_unfmt(int handle,struct cl_db_header *ptr,cldb_offset_t offset,int flag) { 
  int err = -1;
  unsigned char *buf;
  int bytes;
  struct cl_db_header header = {0};
  struct cl_db_header cache = { .offset = offset };
  struct cl_db_header *cache_ptr;
  int fd;
  if(!ptr) { 
    output("Error in read_unfmt");
    goto out;
  }

  if((cache_ptr = cl_db_cache_find(handle,(void*)&cache,&cl_db_header_operations))) { 
    /*Cache hit for the header*/
    memcpy(ptr,cache_ptr,sizeof(*ptr));
    err = sizeof(*ptr);
    goto out;
  }

  fd = CL_DB_FD(handle);
  buf = (unsigned char *)&header;
  bytes = cl_db_read(fd,(char *)buf,sizeof(struct cl_db_header),offset);

  if(bytes <= 0 ) {
    output("Error in cl_db_read:");
    goto out;
  }
    
  if(cl_db_header_unfmt(ptr,buf) < 0) { 
    output("Error in unfmt");
    goto out;
  }
  /*Now add this to the cache*/
  cl_db_cache_add(handle,(void*)ptr,flag,NULL,&cl_db_header_operations);
  err = 0;
 out:
  return err;
}

int cl_db_record_write_fmt(int handle,struct cl_db *ptr,cldb_offset_t offset,int flag) { 
  struct cl_db record = {0};
  unsigned char *buf;
  int err = -1;
  int fd;
  if(!ptr) { 
    output("Error in record write\n");
    goto out;
  }
  fd = CL_DB_FD(handle);
  
  /*First try syncing the cache and add if it fails*/
  if(!(flag & CLDB_SYNC) && cl_db_cache_sync(handle,(void*)ptr,flag,0,NULL,&cl_db_operations)) { 
    cl_db_cache_add(handle,(void*)ptr,flag,NULL,&cl_db_operations);
  }

#if 0
  if((flag & CLDB_SYNC)) { 
    struct cl_db *cache;
    cache = cl_db_cache_find(handle,(void*)ptr,&cl_db_operations);
    assert(cache != NULL);
    assert(memcmp(cache,ptr,CLDB_CTRL_BYTES) == 0 );
  }
#endif

  if((flag & CLDB_CTRL_DATA)) {
    buf = (unsigned char *)&record;

    if(cl_db_record_fmt(ptr,buf) < 0 ) {
      output("Error in formatting record");
      goto out;
    }

    /*First write the control data */
    if((err = cl_db_write(fd,(char*)buf,CLDB_CTRL_BYTES,offset) < 0 )) { 
      output("Error in write:");
      goto out;
    }
    offset = ptr->data_offset;
    if(!(flag & CLDB_KEY_DATA) && (flag & CLDB_DATA)) 
      offset += ptr->keylen;
  }

  if((flag & CLDB_KEY_DATA)) {
    /*Now write the data: KEY + DATA
     */
    if(ptr->key && (err = cl_db_write(fd,ptr->key,ptr->keylen,offset) ) < 0 ) { 
      output("Error in cl_db_write");
      goto out;
    }
    offset = -1;
  }

  if((flag & CLDB_DATA)) {
    if(ptr->data && (err = cl_db_write(fd,ptr->data,ptr->datalen,offset)) < 0 ) { 
      output("Error in cl_db_write");
      goto out;
    }
  }
 out:
  return err;
}

int cl_db_header_write_fmt(int handle,struct cl_db_header *ptr,cldb_offset_t offset,int flag) { 
  struct cl_db_header header = {0};
  unsigned char *buf;
  int err = -1;
  int fd ;
  if(!ptr) { 
    output("Error in header write");
    goto out;
  }
  fd = CL_DB_FD(handle);

  /*Try to sync up the cache or add if that fails*/
  if(!(flag & CLDB_SYNC) && cl_db_cache_sync(handle,(void*)ptr,flag,0,NULL,&cl_db_header_operations)) { 
    cl_db_cache_add(handle,(void*)ptr,flag,NULL,&cl_db_header_operations);
  }

  buf = (unsigned char *)&header;
  if(cl_db_header_fmt(ptr,buf) < 0 ) { 
    output("Error in formatting header:");
    goto out;
  }
  if(cl_db_write(fd,(char *)buf,sizeof(struct cl_db_header),offset) < 0 ) {
    output("Error in db write");
    goto out;
  }
  err = 0;
 out:
  return err;
}

int cl_db_group_read_unfmt(int handle,struct cl_db_group *ptr,cldb_offset_t offset,int flag) { 
  unsigned char *buf;
  struct cl_db_group group = {0};
  struct cl_db_group cache = { .offset = offset };
  struct cl_db_group *cache_ptr;
  int err = -1;
  int fd; 

  if(!ptr) { 
    output("Error in group arg");
    goto out;
  }

  if((cache_ptr = cl_db_cache_find(handle,(void *)&cache,&cl_db_group_operations))) { 
    memcpy(ptr,cache_ptr,sizeof(*ptr));
    err = sizeof(*ptr);
    goto out;
  }

  fd = CL_DB_FD(handle);

  buf = (unsigned char *)&group;

  if((err = cl_db_read(fd,buf,sizeof(group),offset)) < 0 ) { 
    output("Error in read of group data");
    goto out;
  }

  if((err = cl_db_group_unfmt(ptr,buf)) < 0 ) { 
    output("Error in group unfmt");
    goto out;
  }

  /*Add this to the cache*/
  cl_db_cache_add(handle,(void*)ptr,flag,NULL,&cl_db_group_operations);
  err = 0;
 out:
  return err;
}
  
int cl_db_group_write_fmt(int handle,struct cl_db_group *ptr,cldb_offset_t offset,int flag) { 
  struct cl_db_group group = {0};
  unsigned char *buf;
  int err = -1;
  int fd;

  if(!ptr) { 
    output("Error in group write arg");
    goto out;
  }

  /*Try to sync the cache*/
  if(!(flag & CLDB_SYNC) && cl_db_cache_sync(handle,(void*)ptr,flag,0,NULL,&cl_db_group_operations)) {
    cl_db_cache_add(handle,(void*)ptr,flag,NULL,&cl_db_group_operations);
  }

  fd = CL_DB_FD(handle);
  buf = (unsigned char *)&group;
  
  if((err = cl_db_group_fmt(ptr,buf)) < 0 ) {
    output("Error in group fmt");
    goto out;
  }
  
  if((err = cl_db_write(fd,buf,sizeof(group),offset)) < 0 ) { 
    output("Error in group write");
    goto out;
  }
  err = 0;
 out:
  return err;
}
