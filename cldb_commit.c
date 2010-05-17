/*DB commit part.
  This guy closely works with the caching module.
*/

#include <stdio.h>
#include <assert.h>
#include "cldb.h"
#include "cldb_cache.h"

/*Mark the cache with commit in progress*/

int cl_db_commit_start(int handle) { 
  int err = -1;
  if((err = cl_db_cache_commit_start(handle)) < 0 ) {
    output("Error in commit start");
  }
  return err;
}

int cl_db_touch(int handle,void *node,long rec_size,int flag,struct cl_db_operations *fun) { 
  int err = -1;
  struct cl_db *cache = NULL;

  /*Try syncing the cache first*/
  if(cl_db_cache_sync_commit(handle,node,flag,0,(void**)&cache,fun)) { 
    /*It failed.Now add this guy to the cache.
      It should succeed always since we are in commit phase.
    */
    if(!cache) {
      cl_db_cache_add_commit(handle,(void*)node,flag,NULL,fun);
    } else {
      output("Transient error in cache sync...");
      goto out;
    }
  }
  err = 0;
 out:
  return err;
}

/*Now commit the records through a sync write to the disk*/
int cl_db_commit_end(int handle) { 
  int err ;
  if((err = cl_db_cache_commit_end(handle)) < 0 ) {
    output("Error in cache commit end");
  }
  return err;
}


int cl_db_commit_rollback(int handle) { 
  int err ;
  err = cl_db_cache_commit_rollback(handle);
  if(err < 0 ) {
    output("Error in commit end during rollback...");
  }
  return err;
}
