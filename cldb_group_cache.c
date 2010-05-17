/*CLDB group cache for managing free offsets within the group.
  It maintains a rbtree sorted by datalen,offsets to be given out.
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "cldb.h"
#include "rbtree.h"
#include "cldb_group.h"


static int cl_db_group_cache_link(int handle,struct cl_db_group_cache *ptr) { 
  int err = -1;
  struct rbtree **link = &CL_DB_GROUP_CACHE_ROOT(handle).root;
  struct rbtree *parent = NULL;
  while(*link) { 
    int cmp;
    struct cl_db_group_cache *cache;
    parent = *link;
    cache = rbtree_entry(parent,struct cl_db_group_cache,tree);
    /*Now cmp with datalen*/
    cmp = ptr->datalen - cache->datalen;
    if(cmp <= 0 ) { 
      link = &parent->left;
    } else {
      link = &parent->right;
    }
  }
  /*Now add to the rbtree*/
  __rbtree_insert(&ptr->tree,parent,link);
  rbtree_insert_colour(&CL_DB_GROUP_CACHE_ROOT(handle),&ptr->tree);
  err = 0;
  return err;
}

int cl_db_group_cache_add(int handle,cldb_offset_t offset,cldb_offset_t group,cldb_offset_t data_offset,long datalen,long maxlen) { 
  struct cl_db_group_cache *ptr ;
  int err = -1;

  if(CL_DB_GROUP_CACHE_LIMIT(handle)) { 
    output("Cache limit:%ld reached...",MAX_GROUP_CACHE);
    goto out;
  }
  ptr = malloc(sizeof(*ptr));
  if(!ptr) { 
    output("Error in malloc");
    goto out;
  }
  memset(ptr,0x0,sizeof(*ptr));
  ptr->offset = offset;
  ptr->group = group;
  ptr->data = data_offset;
  ptr->datalen = datalen;
  ptr->maxlen = maxlen;
  err = cl_db_group_cache_link(handle,ptr);
  if(err < 0 ) {
    output("Error in linking group cache...");
    goto out;
  }
  CL_DB_GROUP_CACHE_INCR(handle);
  output("Group cache add - offset = "CLDB_OFFSET_FMT",group="CLDB_OFFSET_FMT",data="CLDB_OFFSET_FMT",datalen=%ld,maxlen=%ld\n",offset,group,data_offset,datalen,maxlen);
 out:
  return err;
}

/*
  Find an entry from the group cache thats >= datalen.(min match as the tree is sorted by datalen
*/

static struct cl_db_group_cache *do_cl_db_group_cache_find(int handle,cldb_offset_t offset,long datalen) {
  struct rbtree *root ;
  struct cl_db_group_cache *cache = NULL;
  root = CL_DB_GROUP_CACHE_ROOT(handle).root;
  output("Group cache find -datalen=%ld,offset="CLDB_OFFSET_FMT"\n",datalen,offset);
  while(root) { 
    int cmp;
    struct cl_db_group_cache *ptr;
    ptr = rbtree_entry(root,struct cl_db_group_cache,tree);
    cmp = datalen - ptr->datalen;
    if(cmp < 0 ) { 
      root = root->left;
      if(!offset)
        cache = ptr;
    } else if(cmp > 0) { 
      root = root->right;
    } else { 
      if(!offset || ptr->offset == offset) {
        cache = ptr;
        break;
      }
      root = root->left;
    }
  }
  if(cache) {
    CL_DB_GROUP_CACHE_HITS_INCR(handle);
  } else {
    CL_DB_GROUP_CACHE_MISSES_INCR(handle);
  }
  return cache;
}

struct cl_db_group_cache *cl_db_group_cache_find(int handle,long datalen) {
  return do_cl_db_group_cache_find(handle,0,datalen);
}

int cl_db_group_cache_del(int handle,cldb_offset_t offset,long datalen) { 
  struct cl_db_group_cache *cache;
  int err = -1;
  cache = do_cl_db_group_cache_find(handle,offset,datalen);
  if(!cache) { 
    output("group cache find error.Offset "CLDB_OFFSET_FMT,offset);
    goto out;
  }
  cl_db_group_cache_unlink(handle,cache);
  free((void*)cache);
  err = 0;
 out:
  return err;
}

static void *cl_db_group_cache_print(void *arg) { 
  struct rbtree *tree = (struct rbtree *)arg;
  struct cl_db_group_cache *ptr  = rbtree_entry(tree,struct cl_db_group_cache,tree);
  fprintf(stderr,"\nOffset: "CLDB_OFFSET_FMT ",group :"CLDB_OFFSET_FMT ",data offset :"CLDB_OFFSET_FMT ",datalen:%ld,maxlen:%ld\n",ptr->offset,ptr->group,ptr->data,ptr->datalen,ptr->maxlen);
  return NULL;
}

static void cl_db_group_cache_walk(struct rbtree *root,void *(*fun)(void *arg)) {
  if(!root)
    return;
  cl_db_group_cache_walk(root->left,fun);
  (void)(*fun)((void*)root);
  cl_db_group_cache_walk(root->right,fun);
}

static void cl_db_group_cache_stats_print(int handle) { 
  fprintf(stderr,"\nGroup cache entries - %d,hits:%d,misses:%d,slow fills:%d...\n",CL_DB_GROUP_CACHE_COUNT(handle),CL_DB_GROUP_CACHE_HITS(handle),CL_DB_GROUP_CACHE_MISSES(handle),CL_DB_GROUP_CACHE_SLOW_FILLS(handle));
}

void cl_db_group_cache_display(int handle) { 
  cl_db_group_cache_walk(CL_DB_GROUP_CACHE_ROOT(handle).root,cl_db_group_cache_print);
  fprintf(stderr,"\nEnd of group cache entries...\n");
  fprintf(stderr,"\nDisplaying group cache stats...\n");
  cl_db_group_cache_stats_print(handle);
  fprintf(stderr,"\nEnd group cache stats...\n");
}
