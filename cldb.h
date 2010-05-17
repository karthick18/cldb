#ifndef _CLDB_H
#define _CLDB_H

#include "hash.h"
#include "queue.h"
#include "cldb_cache.h"

typedef long long cldb_offset_t;

#ifdef DEBUG
#define output(fmt,arg...) do { \
  fprintf(stderr,fmt ",File:%s,Line:%d,Function:%s\n",##arg,__FILE__,__LINE__,__FUNCTION__);\
}while(0)
#else
#define output(fmt,arg...)
#endif

#define CLDB_MAGIC ( ('C' << 24) | ('L' < 16) | ('D' << 8 ) | ( 'B' ) )

#define CLDB_VERSION 10

#define CL_DB_QUEUE_FULL  (-2)
#define MAX_CL_DB_INSTANCES (0x10)
#define CL_DB_HANDLE_VALID(handle) ( (handle) >= 0 && (handle) < MAX_CL_DB_INSTANCES )

#define CL_DB_TOUCH(handle,record,flag) do { \
  if(cl_db_touch_record(handle,record,flag)) { \
   output("Error in touching records");\
   goto out;\
 }\
}while(0)

#define STR(a) #a
#define CLDB_SIZEOF(cast,field)  ((int)sizeof(((cast*)0)->field))
#define CLDB_OFFSETOF(cast,field) ( (unsigned long)&((cast *)0)->field )

#define CL_DB_COMMIT_QUEUE(handle) (cl_db_instances[(handle)].cl_db_records)
#define CL_DB_STATS(handle) (cl_db_instances[(handle)].cl_db_stats)
#define CL_DB_FD(handle)    (cl_db_instances[(handle)].fd)

#define CL_DB_HEADER(handle)  (cl_db_instances[(handle)].cl_db_header)
#define CL_DB_ROOT(handle)  (CL_DB_HEADER(handle).root)
#define CL_DB_CUR_OFFSET(handle) (CL_DB_HEADER(handle).cur_offset)
#define CL_DB_FREE_OFFSET(handle)  (CL_DB_HEADER(handle).free_offset)
#define CL_DB_LAST_OFFSET(handle)  (CL_DB_HEADER(handle).last_offset)
#define CL_DB_HANDLE_VALID(handle) ( (handle) >= 0 && (handle) < MAX_CL_DB_INSTANCES )
#define CL_DB_CACHE(handle)  ( cl_db_instances[(handle)].cl_db_cache)
#define CL_DB_GROUP_CACHE(handle)  (cl_db_instances[handle].cl_db_group_cache_records)
#define CL_DB_GROUP_CACHE_ROOT(handle) (CL_DB_GROUP_CACHE(handle).cl_db_group_cache)

#define CL_DB_STATS_UPDATE(stats,count)  do { \
  (stats)+=(count);\
  if((stats) < 0 ) { \
    (stats) = 0;\
  }\
}while(0)

#define CL_DB_SEARCH_HITS(handle) CL_DB_STATS_UPDATE(CL_DB_STATS(handle).search_hits,1)
#define CL_DB_SEARCH_MISSES(handle) CL_DB_STATS_UPDATE(CL_DB_STATS(handle).search_misses,1)

#define CLDB_DATA (0x1)
#define CLDB_CTRL_DATA (0x2)
#define CLDB_KEY_DATA (0x4)
#define CLDB_DATA_ALL (CLDB_CTRL_DATA | CLDB_KEY_DATA | CLDB_DATA)
#define CLDB_SYNC (0x8)

#define CLDB_DATA_BYTES  (sizeof(struct cl_db) - CLDB_CTRL_BYTES )
#define CLDB_CTRL_BYTES  (CLDB_OFFSETOF(struct cl_db,key))

#define CLDB_HEADER_OFFSET (0)
#define CLDB_OFFSET_FMT "%lld"

struct cl_db_stats { 
  int max_record;
  int max_key;
  int search_hits;
  int search_misses;
};

/*Queue for touching records*/

struct cl_db_commit_records { 
#define MAX_CL_DB_RECORDS (0x1000)
  struct queue_struct *head;
  struct queue_struct **tail;
  struct rbtree_root root;
  int count;
};

struct cl_db_group_cache_stats { 
  int entries;
  int hits;
  int misses;
  /*Stats for slow group fills when both cache lookup and free group
    lookup fails.
    Try to have this as less as possible.
  */
  int slow_fills;
};

struct cl_db_group_cache_records { 
  struct rbtree_root cl_db_group_cache;
  struct cl_db_group_cache_stats stats;
};

struct cl_db_header {
  cldb_offset_t offset;
  unsigned int magic;
  unsigned int version;
  unsigned long records;
  cldb_offset_t last_offset;/*start of the last offset allocated*/
  cldb_offset_t  root; /*root offset of the rbtree*/
  cldb_offset_t  first;/*starting offset of the linear list*/
  cldb_offset_t  group; /*starting offset of the group*/
  cldb_offset_t  last_group; /*last group*/
  cldb_offset_t next_free_group;/*offset of the next free group to allocate*/
  cldb_offset_t free_group;/*offset of the group thats free with the max freelen below*/
  long maxlen;
  unsigned long groups; /*number of groups*/
  unsigned short cksum;
} __attribute__((packed));

struct cl_db_instances { 
  struct cl_db_header cl_db_header;
  /*Should be used for touching and commits once thats through:-)
  */
  struct cl_db_commit_records cl_db_records;
  /*
    These are stored in-memory to maintain the free record list + stats.
  */
  struct cl_db_cache cl_db_cache;
  /*Incore group cache*/
  struct cl_db_group_cache_records cl_db_group_cache_records;
  struct cl_db_stats cl_db_stats;
  int fd;
  char name[0xff+1];
  int inuse;
};

struct cl_db { 
  /*Offset of the record*/
  cldb_offset_t offset; 

  /*For the tree*/
  cldb_offset_t left;
  cldb_offset_t right;
  cldb_offset_t parent;
  unsigned char colour;

  /*For the queue*/
  cldb_offset_t next;
  cldb_offset_t prev;
  cldb_offset_t group; /*the group to which this guy belongs*/
  /*The data portion*/
  unsigned int keylen;
  unsigned int datalen;
  /*This is used whenever the record is added first.
    The first addition of the record is considered as the
    max. record capacity with potential of reuse.
  */
  long maxlen; 
  cldb_offset_t data_offset;
  unsigned short cksum;
  /*The below arent stored on-disk*/
  unsigned char *key;
  unsigned char *data;

} __attribute__((packed));

struct cl_db_operations {
  void * (*ctor) (void *,int,long *);
  int    (*dtor) (void *);
  int    (*find) (void *,void *);
  int    (*sync) (void *,void *,int,int,long *);
  void   (*display)(void *);
  int    (*cmp) ( void *,void * );
  unsigned long    (*hash) (void *);
  int  (*commit)(int,void *,int);
  int  (*vector)(void *,long,int,void *);
};
extern struct cl_db_operations cl_db_operations;
extern struct cl_db_operations cl_db_header_operations;
extern struct cl_db_operations cl_db_group_operations;
extern struct cl_db_instances cl_db_instances[MAX_CL_DB_INSTANCES];
extern void cl_db_display(void *);
extern int cl_db_record_fmt(struct cl_db *,unsigned char *);
extern int cl_db_record_unfmt(struct cl_db *,unsigned char *);
extern int cl_db_record_read_unfmt(int handle,struct cl_db *,cldb_offset_t offset,int flag);
extern int cl_db_record_write_fmt(int handle,struct cl_db *,cldb_offset_t offset,int flag);
extern int cl_db_header_fmt(struct cl_db_header *,unsigned char *);
extern int cl_db_header_unfmt(struct cl_db_header *,unsigned char *);
extern int cl_db_header_read_unfmt(int handle, struct cl_db_header *,cldb_offset_t offset,int flag);
extern int cl_db_header_write_fmt(int handle,struct cl_db_header *,cldb_offset_t offset,int flag);
extern int cl_db_get_free_offset(int handle,struct cl_db *);
extern int cl_db_free_offset(int handle,struct cl_db*);
extern int cl_db_touch(int handle,void *rec,long rec_size,int flag,struct cl_db_operations *);
extern int cl_db_touch_record(int handle,struct cl_db *,int flag);
extern int cl_db_touch_header(int handle);
extern int cl_db_commit_start(int handle);
extern int cl_db_commit_end(int handle);
extern int cl_db_commit_rollback(int handle);

#endif
