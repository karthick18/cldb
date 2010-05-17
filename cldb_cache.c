/*DB cache with a lru cache.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "rbtree.h"
#include "hash.h"
#include "cldb.h"
#include "utils.h"
#include "cldb_cache.h"
#include "cldb_vector.h"

#define CLDB_FUN_CHECK(fun) do { \
 if(!(fun) || !(fun)->ctor || !(fun)->dtor || !(fun)->find || !(fun)->sync || !(fun)->hash ) { \
  output("Error in cl db function...");\
  goto out;\
}\
}while(0)

/*start with a default cache size of 2MB
*/

#define CLDB_CACHE_SIZE_DEFAULT (2<<20L)

/*Min is 4K*/
#define CLDB_CACHE_SIZE_MIN     (4096L)

/*Max cache size is 1/4th of the RAM
*/
#define CLDB_CACHE_SIZE_FACTOR     (4)
#define CLDB_CACHE_THRESHOLD(s,handle)  ((s+CLDB_CACHE_SIZE(handle)) > CLDB_CACHE_SIZE_LIMIT(handle))

#define CLDB_CACHE_INCR(handle,v) do { \
    CLDB_CACHE_SIZE_INCR(handle,v);\
} while(0)

#define CLDB_CACHE_DECR(handle,v) do { \
    CLDB_CACHE_SIZE_DECR(handle,v);\
}while(0)
  
struct cl_db_cache_lru;

/*The cache record*/
struct cl_db_cache_element { 
#define CACHE_UPDATE_FREQUENCY (0x5)
  void *record;
  long rec_size;
  int touch; /*the record is marked for a commit*/
  int flag;/*the flag for the object thats touched*/
  struct cl_db_operations *fun;
  long hits; /*Increment the cache hits for this record*/
  short frequency; /*frequency of updates for the hit counter*/
  struct cl_db_cache_lru *lru_node;/*pointer to the lru list*/
  struct hash_struct hash;
  struct queue_struct commit_queue; /*cache commit queue*/
  struct rbtree commit_tree; /*record sorted by offsets in the commit queue*/
};

/*Here we maintain the LRU list as a RBTREE
  thats ordered by cache hits for a DB record.
*/

struct cl_db_cache_lru { 
#define CLDB_CACHE_LRU_ROOT(handle) (CL_DB_CACHE(handle).lru_root)
  struct rbtree tree; /*index to the rbtree node*/
  struct cl_db_cache_element *cache; 
  struct cl_db_cache_lru *next; /*for the LRU stack used during a walk*/
};

struct lru_walk_args { 
#define CLDB_CACHE_LRU_STOPWALK (0x2)
  int handle;
  long amount;
  long scan;
  /*stack of lru nodes that can be marked for deletion through a walk*/
  struct cl_db_cache_lru *stack;
};

static __inline__ int cl_db_cache_remove(int handle,struct cl_db_cache_element *cache) { 
  int err = 0;
  del_hash(&cache->hash);
  if(cache->fun && cache->fun->dtor) { 
    err = cache->fun->dtor(cache->record);
  }
  CLDB_CACHE_DECR(handle,cache->rec_size);
  free((void*)cache);
  return err;
}

int cl_db_cache_init(int handle) { 
  memset(&CL_DB_CACHE(handle),0x0,sizeof(struct cl_db_cache));
  memset(CL_DB_CACHE(handle).hash_table,0x0,sizeof(CL_DB_CACHE(handle).hash_table));
  CLDB_CACHE_LRU_ROOT(handle).root = NULL;
  CLDB_CACHE_SIZE_LIMIT(handle) = cldb_setmem(CLDB_CACHE_SIZE_DEFAULT,CLDB_CACHE_SIZE_FACTOR);
  return 0;
}

static int lru_cache_add(int handle,struct cl_db_cache_element *ptr,struct cl_db_cache_lru *lru_node) { 
  struct rbtree **link;
  struct rbtree *parent = NULL;
  int err = -1;

  if(!lru_node && !(lru_node = malloc(sizeof(*lru_node)))) {
    output("Error mallocing lru node");
    goto out;
  }

  link = &CLDB_CACHE_LRU_ROOT(handle).root;
  
  while(*link) { 
    struct cl_db_cache_lru *node ;
    parent = *link;
    node = rbtree_entry(parent,struct cl_db_cache_lru,tree);
    /*Order based on cache hits*/
    if(ptr->hits <= node->cache->hits) 
      link = &parent->left;
    else
      link = &parent->right;
  }
  lru_node->cache = ptr;
  ptr->lru_node = lru_node;
  /*Now insert into the lru tree*/
  __rbtree_insert(&lru_node->tree,parent,link);
  rbtree_insert_colour(&CLDB_CACHE_LRU_ROOT(handle),&lru_node->tree);
  CLDB_CACHE_ENTRIES_INCR(handle);
  err = 0;
 out:
  return err;
}

static __inline__  int lru_cache_del(int handle,struct cl_db_cache_lru *ptr) { 
  __rbtree_erase(&CLDB_CACHE_LRU_ROOT(handle),&ptr->tree);
  CLDB_CACHE_ENTRIES_DECR(handle);
  return 0;
}

/*Since the hit count has increased, update the lru cache again*/
static int lru_cache_update(int handle,struct cl_db_cache_element *cache) { 
  int err = -1;
  if(lru_cache_del(handle,cache->lru_node)) { 
    output("Error in lru cache delete");
    goto out;
  }
  if(lru_cache_add(handle,cache,cache->lru_node)) { 
    free((void*)cache->lru_node);
    cache->lru_node = NULL;
    output("Error in lru cache add");
    goto out;
  }
  err = 0;
 out:
  return err;
}

/*Update the frequency of the cache for hits*/
static __inline__ int lru_cache_update_age(int handle,struct cl_db_cache_element *cache) { 
  if(cache->frequency++ >= CACHE_UPDATE_FREQUENCY) { 
    cache->frequency = 0;
    ++cache->hits;
    lru_cache_update(handle,cache);
  }
  return 0;
} 

static int lru_cache_remove(struct rbtree *node,void *arg) { 
  struct lru_walk_args *args = (struct lru_walk_args *)arg;
  struct cl_db_cache_lru *lru_node;
  long rec_size;
  int err = -1;
  
  if(args->amount <= 0 && args->scan <= 0) 
    goto out;
  if(CLDB_CACHE_LRU_ROOT(args->handle).root == NULL) 
    goto out;
  lru_node = rbtree_entry(node,struct cl_db_cache_lru,tree);

  /*See if the record is marked.If yes,skip it*/
  if(lru_node->cache->touch) { 
    err = 0;
    goto out;
  }

  rec_size = lru_node->cache->rec_size;

  /*put this guy on the lru stack for deletion
  */
  lru_node->next = args->stack;
  args->stack = lru_node;
  err = 0;
  if(args->amount > 0 ) { 
    args->amount -= rec_size;
    if(args->amount <= 0 ) { 
      err = CLDB_CACHE_LRU_STOPWALK;
    }
  } else { 
    if(--args->scan <= 0 ) { 
      err = CLDB_CACHE_LRU_STOPWALK;
    }
  }
  if(CLDB_CACHE_LRU_ROOT(args->handle).root == NULL) 
    err = CLDB_CACHE_LRU_STOPWALK;
 out:
  return err;
}
    
static int lru_cache_walk(struct rbtree *root,int (*fn)(struct rbtree *,void *),void *arg) { 
  int err;

  if(!root) {
    return 0;
  }
  err = lru_cache_walk(root->left,fn,arg);

  if(err) 
    goto out;

  err = fn(root,arg);
  if(err)
    goto out;

  err = lru_cache_walk(root->right,fn,arg);

 out:
  return err;
  
}


/*We shrink 20% percent of the entries if amount isnt specified*/
static int lru_cache_shrink(int handle,long amount,int force) { 
  long scan = 0;
  int err ;
  struct lru_walk_args args ;
  register struct cl_db_cache_lru *stack;
  struct cl_db_cache_lru *next;
  if(amount <= 0) {
    scan = CLDB_CACHE_ENTRIES(handle);
    scan/=5;
  } else {
    if(!force)
      amount = MIN(amount,CLDB_CACHE_SIZE(handle));
  }

  args.amount = amount;
  args.scan = scan;
  args.handle = handle;
  args.stack = NULL;

  err = lru_cache_walk(CLDB_CACHE_LRU_ROOT(handle).root,lru_cache_remove,(void*)&args);

  if(err < 0 ) { 
    output("Error in cache walk");
    goto out;
  }

  /*Check for the elements in the stack marked for deletion*/

  for(stack = args.stack ; stack; stack = next) { 
    next = stack->next;
    cl_db_cache_remove(handle,stack->cache);
    lru_cache_del(handle,stack);
    free((void*)stack);
  } 
  
  if(err == CLDB_CACHE_LRU_STOPWALK) { 
    output("LRU cache shrunk Success...");
    err = 0;
  }

 out:
  return err;
}

int cl_db_cache_set(int handle,long amount) { 
  int err = -1;

  if(amount < CLDB_CACHE_SIZE_MIN) { 
    output("Invalid cache set with amount:%ld(min is %ld)",amount,CLDB_CACHE_SIZE_MIN);
    goto out;
  }  

  if(amount == CLDB_CACHE_SIZE_LIMIT(handle)) { 
    output("Cache limit is already at the desired value:%ld",amount);
    goto out;
  }

  if(amount > CLDB_CACHE_SIZE_LIMIT(handle)) {
    CLDB_CACHE_SIZE_LIMIT(handle) = cldb_setmem(amount,CLDB_CACHE_SIZE_FACTOR);
    err = 0;
  } else {
    /*
      We have to shrink the cache.Actually we can skip this
     */
    if(lru_cache_shrink(handle,CLDB_CACHE_SIZE_LIMIT(handle)-amount,1) < 0 ) {
      output("Cache shrink failure...");
      goto out;
    }
    CLDB_CACHE_SIZE_LIMIT(handle) = amount;
  }
  err = 0;
 out:
  return err;
}

static int do_cl_db_cache_add(int handle,void *ptr,int flag,void **ret,struct cl_db_operations *fun) {
  unsigned long key;
  struct hash_struct **table;
  struct cl_db_cache_element *cache;
  int err = -1;
  int commit;
  long rec_size = 0;
  CLDB_FUN_CHECK(fun);

  commit = CL_DB_CACHE(handle).commit_in_progress;

  if(!commit && rec_size >= CLDB_CACHE_SIZE_LIMIT(handle)) {
    output("rec size too big to be accomodated in the cache:%ld",rec_size);
    goto out;
  }

  /*Check if the threshold has been hit.
    If yes, shrink the lru cache and check again
    for space to respect the presumption that the blighter is the eldest. We only like young minds in the cache:-)
  */
  
  if(!commit && CLDB_CACHE_THRESHOLD(rec_size,handle)) {
    if(lru_cache_shrink(handle,0,0) < 0 ) {
      output("Error shrinking lru cache");
      goto out;
    }
    if(CLDB_CACHE_THRESHOLD(rec_size,handle)) { 
      output("lru cache shrink not sufficient for rec size:%ld",rec_size);
      goto out;
    }
    CLDB_CACHE_FILLS_INCR(handle);
  }

  if( !(cache = malloc(sizeof(*cache))) ) {
    output("Error in malloc");
    goto out;
  }

  memset(cache,0x0,sizeof(*cache));

  if(!(cache->record = fun->ctor(ptr,flag,&rec_size))) {
    output("Error adding cache record..");
    goto out;
  }

  key = fun->hash(ptr);
  table = CL_DB_CACHE(handle).hash_table;
  cache->fun = fun;
  cache->rec_size = rec_size;

  if((err = add_hash(table,key,&cache->hash))) { 
    output("Error in add_hash");
    goto out_free;
  }

  /*Now add into the lru cache*/
  if((err = lru_cache_add(handle,cache,NULL))) { 
    output("Error in lru cache addition for rec size:%ld",rec_size);
    del_hash(&cache->hash);
    goto out_free;
  }

  CLDB_CACHE_INCR(handle,rec_size);
  if(ret)
    *ret = (void*)cache;
  err = 0;
  goto out;

 out_free:
  fun->dtor(cache->record);
  free((void*)cache);

 out:
  return err;
}

int cl_db_cache_add(int handle,void *ptr,int flag,void **ret,struct cl_db_operations *fun) { 
  struct cl_db_cache_element *cache = NULL;
  int err;
  err = do_cl_db_cache_add(handle,ptr,flag,(void**)&cache,fun);
  if(err < 0 ) { 
    output("Error in cache addition");
    goto out;
  }
  if(ret) {
    *ret = cache->record;
  }
 out:
  return err;
}

static int cl_db_cache_commit_queue(int handle,struct cl_db_cache_element *cache) { 
  struct cl_db_commit_records *queue = &CL_DB_COMMIT_QUEUE(handle);
  struct rbtree **link = &queue->root.root;
  struct rbtree *parent = NULL;
  struct queue_struct *rec = &cache->commit_queue;
  int err = -1;

  QUEUE_ADD_TAIL(rec,queue);
  ++queue->count;

  CLDB_FUN_CHECK(cache->fun);
  
  while(*link) { 
    struct cl_db_cache_element *ptr;
    int cmp;
    parent = *link;
    ptr = rbtree_entry(parent,struct cl_db_cache_element,commit_tree);
    cmp = cache->fun->cmp(cache->record,ptr->record);
    if(cmp < 0 ) { 
      link = &parent->left;
    } else if(cmp > 0 ) { 
      link = &parent->right;
    } else { 
      fprintf(stderr,"Duplicate cache commit add..\n");
      goto out;
    }
  }
  __rbtree_insert(&cache->commit_tree,parent,link);
  rbtree_insert_colour(&queue->root,&cache->commit_tree);
  err = 0;
 out:
  return err;
}

int cl_db_cache_add_commit(int handle,void *ptr,int flag,void **ret,struct cl_db_operations *fun) { 
  int err = -1;
  struct cl_db_cache_element *cache = NULL;
  if(!CL_DB_CACHE(handle).commit_in_progress) {
    output("cache add commit while not in commit...");
    goto out;
  }
  if(!(err = do_cl_db_cache_add(handle,ptr,flag,(void**)&cache,fun) ) ) { 
    /*Add this to the commit queue*/
    cl_db_cache_commit_queue(handle,cache);
    cache->touch = 1;
    cache->flag |= flag;
    if(ret) { 
      *ret = cache->record;
    }
  }
 out:
  return err;
}

static void *do_cl_db_cache_find(int handle,void *node,void **cache,struct cl_db_operations *fun ) {
  register struct hash_struct *temp;
  void *ret = NULL;
  unsigned long key;

  CLDB_FUN_CHECK(fun);

  key = fun->hash(node);

  for(temp = CL_DB_CACHE(handle).hash_table[key]; temp; temp = temp->next) { 
    struct cl_db_cache_element *ptr = hash_entry(temp,struct cl_db_cache_element,hash);
    if(fun->find(ptr->record,node)) {
      CLDB_CACHE_HITS_INCR(handle);
      lru_cache_update_age(handle,ptr);
      ret = ptr->record;
      if(cache) { 
        *cache = (void*)ptr;
      }
      goto out;
    }
  }
  CLDB_CACHE_MISSES_INCR(handle);
out:
  return ret;
}

void *cl_db_cache_find(int handle,void *ptr,struct cl_db_operations *fun) { 
  return do_cl_db_cache_find(handle,ptr,NULL,fun);
}

/*Sync the cache for the corresponding object.
*/
int cl_db_cache_sync(int handle,void *ptr,int flag,int update_data,void **ret,struct cl_db_operations *fun) { 
  void *node;
  struct cl_db_cache_element *cache = NULL;
  int err = -1;
  long data_size=0;

  CLDB_FUN_CHECK(fun);
  
  if((node = do_cl_db_cache_find(handle,ptr,(void **)&cache,fun))) {
    if(ret) {
      *ret = node;
    }
    err = fun->sync(node,ptr,flag,update_data,&data_size);
    if(data_size) { 
      cache->rec_size += data_size;
      CLDB_CACHE_INCR(handle,data_size);
    }
  }
 out:
  return err;
}      

/*Sync the cache and add to the commit queue*/
int cl_db_cache_sync_commit(int handle,void *ptr,int flag,int update_data,void **ret,struct cl_db_operations *fun) { 
  struct cl_db_cache_element *cache = NULL;
  void *node;
  int err = -1;

  if(!CL_DB_CACHE(handle).commit_in_progress) {
    output("cache sync commit while not in commit...");
    goto out;
  }

  CLDB_FUN_CHECK(fun);
  if( ( node = do_cl_db_cache_find(handle,(void*)ptr,(void **)&cache,fun ) ) ) { 
    long size = 0;
    err = fun->sync(node,(void *)ptr,flag,update_data,&size);
    if(size) {
      cache->rec_size += size;
      CLDB_CACHE_INCR(handle,size);
    }
    if(err) {
      goto out;
    }
    if(ret)
      *ret = node;
    cache->flag |= flag;
    if(!cache->touch) { 
      cl_db_cache_commit_queue(handle,cache);
      cache->touch = 1;
    }
  }
 out:
  return err;
}

int cl_db_cache_del(int handle,void *ptr,struct cl_db_operations *fun) {
  int err = -1;
  struct cl_db_cache_element *cache = NULL;
  
  CLDB_FUN_CHECK(fun);

  if(!do_cl_db_cache_find(handle,ptr,(void **)&cache,fun)) {
    output("Error deleting");
    goto out;
  }

  if(cache->lru_node) {
    lru_cache_del(handle,cache->lru_node);
    free((void*)cache->lru_node);
  }
  cl_db_cache_remove(handle,cache);
  err = 0;
out:
  return err;
}

#ifdef DEBUG_CACHE

static int lru_display(struct rbtree *node,void *args) { 
  struct cl_db_cache_lru *cache ;
  cache = rbtree_entry(node,struct cl_db_cache_lru,tree);
  fprintf(stderr,"\nCache hits:%ld,Frequency:%d\n",cache->cache->hits,cache->cache->frequency);
  if(cache->cache->fun && cache->cache->fun->display) {
    cache->cache->fun->display(cache->cache->record);
  }
  return 0;
}

static void lru_cache_display(int handle) { 
  struct lru_walk_args args = {0};
  args.handle = handle;
  if(lru_cache_walk(CLDB_CACHE_LRU_ROOT(handle).root,lru_display,(void*)&args) < 0 ) {
    fprintf(stderr,"Error in lru cache walk");
  }
}

void cl_db_cache_stats(int handle) { 
  fprintf(stderr,"\nDisplaying the LRU cache entries...\n");
  lru_cache_display(handle);
  fprintf(stderr,"Cache size=%ld,entries:%ld,hits:%d,misses:%d,fills:%d\n",CLDB_CACHE_SIZE(handle),CLDB_CACHE_ENTRIES(handle),CLDB_CACHE_HITS(handle),CLDB_CACHE_MISSES(handle),CLDB_CACHE_FILLS(handle));  
}

#else
void cl_db_cache_stats(int handle) { }
static __inline__ int lru_display(struct rbtree *n,void *p) { return 0; }
static __inline__ void lru_cache_display(int handle) { }
#endif

int cl_db_cache_destroy(int handle) { 
  int err = -1;

  cl_db_cache_stats(handle);

  if(lru_cache_shrink(handle,CLDB_CACHE_SIZE(handle),1) < 0 ) {
    output("Error in shrinking lru cache for cache destroy");
    goto out;
  }
  cl_db_cache_stats(handle);
  assert(CLDB_CACHE_LRU_ROOT(handle).root == NULL);
  assert(CLDB_CACHE_SIZE(handle) == 0);
  assert(CLDB_CACHE_ENTRIES(handle) == 0);
  memset(CL_DB_CACHE(handle).hash_table,0x0,sizeof(CL_DB_CACHE(handle).hash_table));
  err = 0;
 out:
  return err;
}
    
int cl_db_cache_commit_start(int handle) { 
  int err = -1;
  if(CL_DB_CACHE(handle).commit_in_progress) {
    output("Commit is already in progress...");
    goto out;
  }
  CL_DB_COMMIT_QUEUE(handle).count = 0;
  QUEUE_INIT(&CL_DB_COMMIT_QUEUE(handle));
  CL_DB_CACHE(handle).commit_in_progress = 1;
  err = 0;
 out:
  return err;
}

/*Cache vector callback per cache element*/
int cl_db_cache_vector_callback(struct rbtree *node,void *arg) { 
  struct cl_db_cache_element *cache;
  struct cl_db_vector_args *vector ;
  int err = -1 ;
  if(!node || !arg) { 
    output("Invalid arg...");
    goto out;
  }
  vector = (struct cl_db_vector_args *)arg;
  cache = rbtree_entry(node,struct cl_db_cache_element,commit_tree);
  if(!cache->fun || !cache->fun->vector) {
    output("cache fun NULL...");
    goto out;
  }
  vector->len += cache->rec_size;
  cache->touch = 0;
  err = cache->fun->vector(cache->record,cache->rec_size,cache->flag,arg);

 out:
  return err;
}

/*Shrink the cache by the amount of commits.
  The shrink should work but its not guaranteed to free
  the committed entries from the cache since its an lru shrink.
*/

int cl_db_cache_commit_end(int handle) { 
  int err = -1;
  struct cl_db_commit_records *queue;
  long amount = 0;

  if(!CL_DB_CACHE(handle).commit_in_progress) {
    output("commit end called while not in commit..");
    goto out;
  }
  queue = &CL_DB_COMMIT_QUEUE(handle);
  if((err = cl_db_vector_commit(handle,queue,&amount)) < 0 ) {
    output("Error in commit end...");
    goto out;
  }
  /*Reset the commit queue now*/
  QUEUE_INIT(queue);
  queue->count = 0;
  queue->root.root = NULL;

  output("Commit end shrinking the cache by %ld,size:%ld,limit:%ld...\n",amount,CLDB_CACHE_SIZE(handle),CLDB_CACHE_SIZE_LIMIT(handle));

  if((err = lru_cache_shrink(handle,amount,1)) < 0 ) {
    output("Error in lru cache shrink");
  }
  output("Cache size after shrink = %ld,limit:%ld\n",CLDB_CACHE_SIZE(handle),CLDB_CACHE_SIZE_LIMIT(handle));

  CL_DB_CACHE(handle).commit_in_progress = 0;
  err = 0;
  if(CLDB_CACHE_THRESHOLD(0,handle)) {
    err |= lru_cache_shrink(handle,0,0);
  }
 out:
  return err;
}

/*Delete the entries in the commit queue
 as we are rolling backward and want the cache to be in a consistent 
 state
*/

int cl_db_cache_commit_rollback(int handle) {
  struct cl_db_commit_records *queue;
  int err = -1;
  long amount = 0;

  if(!CL_DB_CACHE(handle).commit_in_progress) {
    output("commit rollback while not in commit...");
    goto out;
  }
  queue = &CL_DB_COMMIT_QUEUE(handle);
  while(!QUEUE_EMPTY(queue)) {
    struct queue_struct *head = queue->head;
    struct cl_db_cache_element *cache = queue_entry(head,struct cl_db_cache_element,commit_queue);
    QUEUE_DEL(head,queue);
    --queue->count;
    amount += cache->rec_size;
    /*Remove this guy*/
    if(cache->lru_node) {
      lru_cache_del(handle,cache->lru_node);
      free((void*)cache->lru_node);
    }
    cl_db_cache_remove(handle,cache);
  }
  output("Cache shrunk by %ld\n",amount);
  CL_DB_CACHE(handle).commit_in_progress = 0;
  err = 0;

  if(CLDB_CACHE_THRESHOLD(0,handle)) {
    err |= lru_cache_shrink(handle,0,0);
  }

 out:
  return err;
}
