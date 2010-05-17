#ifndef _CLDB_CACHE_H
#define _CLDB_CACHE_H

#include "rbtree.h"

#define CLDB_CACHE_STATS_UPDATE(handle,s,v) do {\
  CL_DB_CACHE(handle).cache_stats.s += (v);\
}while(0)

#define CLDB_CACHE_HITS_INCR(handle) do { \
  CLDB_CACHE_STATS_UPDATE(handle,cache_hits,1);\
}while(0)

#define CLDB_CACHE_MISSES_INCR(handle) do {\
  CLDB_CACHE_STATS_UPDATE(handle,cache_misses,1);\
}while(0)

#define CLDB_CACHE_FILLS_INCR(handle) do {\
  CLDB_CACHE_STATS_UPDATE(handle,cache_fills,1);\
}while(0)

#define CLDB_CACHE_SIZE_INCR(handle,v) do { \
 CLDB_CACHE_STATS_UPDATE(handle,cache_size,v);\
}while(0)

#define CLDB_CACHE_SIZE_DECR(handle,v) do { \
  CLDB_CACHE_STATS_UPDATE(handle,cache_size,-(v));\
}while(0)

#define CLDB_CACHE_COMMIT_INCR(handle,v) do { \
  CLDB_CACHE_STATS_UPDATE(handle,cache_commit_size,v);\
}while(0)

#define CLDB_CACHE_COMMIT_DECR(handle,v) do { \
  CLDB_CACHE_STATS_UPDATE(handle,cache_commit_size,-(v));\
}while(0)

#define CLDB_CACHE_ENTRIES_INCR(handle) do { \
  CLDB_CACHE_STATS_UPDATE(handle,cache_entries,1);\
}while(0)

#define CLDB_CACHE_ENTRIES_DECR(handle) do { \
  CLDB_CACHE_STATS_UPDATE(handle,cache_entries,-1);\
}while(0)

#define CLDB_CACHE_ENTRIES(handle)  (CL_DB_CACHE(handle).cache_stats.cache_entries)
#define CLDB_CACHE_SIZE(handle)  ( CL_DB_CACHE(handle).cache_stats.cache_size)
#define CLDB_CACHE_COMMIT_SIZE(handle) (CL_DB_CACHE(handle).cache_stats.cache_commit_size)
#define CLDB_CACHE_SIZE_LIMIT(handle) (CL_DB_CACHE(handle).max_cachesize)
#define CLDB_CACHE_HITS(handle)  ( CL_DB_CACHE(handle).cache_stats.cache_hits )
#define CLDB_CACHE_MISSES(handle) ( CL_DB_CACHE(handle).cache_stats.cache_misses)
#define CLDB_CACHE_FILLS(handle) ( CL_DB_CACHE(handle).cache_stats.cache_fills)

struct cl_db_cache_stats {
  int cache_hits;
  int cache_misses;
  int cache_fills;
  long cache_size;
  long cache_commit_size;
  long cache_entries;/*number of entries in the cache*/
};

struct cl_db_cache { 
  struct hash_struct *hash_table[HASH_TABLE_SIZE];
  struct rbtree_root lru_root;/*rbtree root of the LRU cache*/
  long max_cachesize; /*max cachesize thats user specified*/
  int commit_in_progress;/*Mark the commit flag for the cache*/
  struct cl_db_cache_stats cache_stats;
};

struct cl_db_operations;
extern int cl_db_cache_init(int handle);
extern int cl_db_cache_add(int handle,void *,int flag,void **,struct cl_db_operations *);
extern int cl_db_cache_add_commit(int handle,void *,int flag,void **,struct cl_db_operations *);
extern int cl_db_cache_del(int handle,void *,struct cl_db_operations *);
extern int cl_db_cache_sync(int handle,void *,int flag,int update_data,void **ret,struct cl_db_operations *);
extern int cl_db_cache_sync_commit(int handle,void *,int flag,int update_data,void **ret,struct cl_db_operations *);
extern int cl_db_cache_set(int handle,long amount);
extern void *cl_db_cache_find(int handle,void *node,struct cl_db_operations *);
extern int cl_db_cache_destroy(int handle);
extern int cl_db_cache_commit_start(int handle);
extern int cl_db_cache_vector_callback(struct rbtree *,void *);
extern int cl_db_cache_commit_end(int handle);
extern int cl_db_cache_commit_rollback(int handle);
extern void cl_db_cache_stats(int handle);

#endif
