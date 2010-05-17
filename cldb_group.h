#ifndef _CLDB_GROUP_H
#define _CLDB_GROUP_H

#include "cldb_io.h"

#define CLDB_GROUP_FREE_LIST  ( (CLDB_BLOCK_SIZE - (3*sizeof(cldb_offset_t) + 5*sizeof(short) + sizeof(long)))/sizeof(short) )
#define CLDB_GROUP_SIZE (CLDB_BLOCK_SIZE /*sizeof(struct cl_db_group) */)
/*Have 16k records in the group cache tree*/
#define MAX_GROUP_CACHE ( 16*1024L) 

#define CL_DB_GROUP_CACHE_STATS(handle,f)  (CL_DB_GROUP_CACHE(handle).stats.f)
#define CL_DB_GROUP_CACHE_COUNT(handle) (CL_DB_GROUP_CACHE_STATS(handle,entries))
#define CL_DB_GROUP_CACHE_HITS(handle)  (CL_DB_GROUP_CACHE_STATS(handle,hits))
#define CL_DB_GROUP_CACHE_MISSES(handle) (CL_DB_GROUP_CACHE_STATS(handle,misses))
#define CL_DB_GROUP_CACHE_SLOW_FILLS(handle) (CL_DB_GROUP_CACHE_STATS(handle,slow_fills))
#define CL_DB_GROUP_CACHE_LIMIT(handle) (CL_DB_GROUP_CACHE_COUNT(handle) >= MAX_GROUP_CACHE )
#define CL_DB_GROUP_CACHE_MOD(handle,f,v) do { \
  CL_DB_GROUP_CACHE_STATS(handle,f) +=(v);\
}while(0)

#define CL_DB_GROUP_CACHE_HITS_INCR(handle) CL_DB_GROUP_CACHE_MOD(handle,hits,1)
#define CL_DB_GROUP_CACHE_MISSES_INCR(handle) CL_DB_GROUP_CACHE_MOD(handle,misses,1)
#define CL_DB_GROUP_CACHE_SLOW_FILLS_INCR(handle) CL_DB_GROUP_CACHE_MOD(handle,slow_fills,1)
#define CL_DB_GROUP_CACHE_INCR(handle) CL_DB_GROUP_CACHE_MOD(handle,entries,1)
#define CL_DB_GROUP_CACHE_DECR(handle) CL_DB_GROUP_CACHE_MOD(handle,entries,-1)

struct cl_db_group {
  cldb_offset_t offset;
  cldb_offset_t next;
  short free;/*index of the first free element*/
  short last_free;/*index of the last free to use incase of invalid groups*/
  /*tail points to the slot that was freed when we werent valid.
   Some dirty games here just to pull more ctrl data within a group:-)
  */
  short tail; 
  /*you can reuse this group if all the slots were occupied in this group
   The short is just to make the free short list aligned 
  */
  short valid;
  /*Data offset to be granted*/
  cldb_offset_t next_data_offset;
  /*Max len of the guy that can be reused within this group
    for faster reuse.
  */
  long maxlen; 
  short array[CLDB_GROUP_FREE_LIST];
  unsigned short cksum;
} __attribute__((packed));


struct cl_db_group_cache { 
  cldb_offset_t offset;
  cldb_offset_t group;
  cldb_offset_t data;
  long datalen;
  long maxlen;
  struct rbtree tree; /*tree linkage*/
};

static __inline__ void cl_db_group_cache_unlink(int handle,struct cl_db_group_cache *cache) { 
  __rbtree_erase(&CL_DB_GROUP_CACHE_ROOT(handle),&cache->tree);
  CL_DB_GROUP_CACHE_DECR(handle);
}

extern int cl_db_group_init(int handle);
extern int cl_db_group_start(int handle);
extern int cl_db_group_read_unfmt(int handle,struct cl_db_group *,cldb_offset_t,int flag);
extern int cl_db_group_write_fmt(int handle,struct cl_db_group *,cldb_offset_t,int flag);
extern int cl_db_group_fmt(struct cl_db_group *,unsigned char *);
extern int cl_db_group_unfmt(struct cl_db_group *,unsigned char *);
extern int cl_db_touch_group(int handle,struct cl_db_group *,int flag);
extern cldb_offset_t cl_db_group_get_offset(int handle,long datalen,cldb_offset_t *data,cldb_offset_t *group,long *max);
extern int cl_db_group_release_offset(int handle,struct cl_db *);

extern int cl_db_group_cache_add(int handle,cldb_offset_t offset,cldb_offset_t group,cldb_offset_t data_offset,long datalen,long maxlen);
extern int cl_db_group_cache_del(int handle,cldb_offset_t offset,long datalen);
extern struct cl_db_group_cache *cl_db_group_cache_find(int handle,long datalen);
extern void cl_db_group_cache_display(int handle);

#endif
