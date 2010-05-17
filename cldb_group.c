/*Database group management for free list
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "cldb.h"
#include "cldb_cache.h"
#include "utils.h"
#include "cldb_group.h"

#ifdef GROUP_SLOW

/*Slow versions of get and release offset*/
#define __cl_db_group_get_offset __cl_db_group_get_offset_slow
#define __cl_db_group_release_offset(h,p) __cl_db_group_release_offset_slow(h,(p)->offset,(p)->group)

#else

/*Fast versions of get and release offset*/
#define __cl_db_group_get_offset __cl_db_group_get_offset_fast
#define __cl_db_group_release_offset __cl_db_group_release_offset_fast

#endif



int cl_db_touch_group(int handle,struct cl_db_group *group,int flag) { 
  int err;
  if( (err = cl_db_touch(handle,(void*)group,sizeof(*group),flag,&cl_db_group_operations)) < 0 ) {
    output("Error in touching group offset "CLDB_OFFSET_FMT,group->offset);
  }
  return err;
}

/*Get the offset from the group
*/
static cldb_offset_t cl_db_group_get(int handle,struct cl_db_group *group,long datalen,cldb_offset_t *data,long *max) {
  cldb_offset_t data_offset=-1;
  cldb_offset_t ret = -1;
  if(group->free == -1) {
    output("Group "CLDB_OFFSET_FMT "is full...",group->offset);
    goto out;
  }
  if(group->valid) { 
    register short i;
    short match = -1,last=-1,last_match = -1;
    cldb_offset_t base_offset;
    long minlen = 0,maxlen = -1;
    if(datalen > group->maxlen) {
      output("Datalen :%ld cannot be accomodated within group :"CLDB_OFFSET_FMT,datalen,group->offset);
      goto out;
    }
    /*Now hunt for a free ctrl slot that would fit the datalen 
      above.We have to be successful coz of the maxlen check above
    */
    base_offset = group->offset + CLDB_GROUP_SIZE;
    for( i = group->free ; i != -1 ; last = i , i = group->array[i] ) { 
      struct cl_db record ;
      cldb_offset_t offset;
      long len;
      offset = base_offset + (i * CLDB_CTRL_BYTES);
      if(cl_db_record_read_unfmt(handle,&record,offset,CLDB_CTRL_DATA) < 0 ) {
        output("Error in reading ctrl data at offset "CLDB_OFFSET_FMT,offset);
        continue;
      }
      /*Max len is the sum of key + data
        that was added for the first time.
      */
      len = record.maxlen;
      /*Find the best matching len
       */
      if(len >= datalen && minlen && len < minlen) { 
        /*Mark this record*/
        minlen = len;
        last_match = last;
        match = i;
        data_offset = record.data_offset;
        maxlen = len;
      } else if(!minlen && len >= datalen) { 
        minlen = len;
        last_match = last;
        match = i;
        data_offset = record.data_offset;
        maxlen = len;
      }
    }
    if(match == -1) { 

      /*We are here when we find that the free slots dont have the
	capacity to fill us.
	Actually we have a record in-use for our request len.
	So this group cannot be reused at this point.
      */
      output("Group free list cannot accomodate datalen:%ld in group "CLDB_OFFSET_FMT,datalen,group->offset);
      goto out;
    }
    ret = base_offset + match*CLDB_CTRL_BYTES;
    if(data) {
      *data = data_offset;
    }
    if(max) 
      *max = maxlen;
    if(last_match == -1) { 
      /*We have taken the first slot.Hence update free index*/
      group->free = group->array[group->free];
    } else { 
      group->array[last_match] = group->array[match];
    }
    goto out;
  }

  /*We would be here for groups that have not been taken fully.
   */
  ret = group->last_free * CLDB_CTRL_BYTES;
  ret += group->offset + CLDB_GROUP_SIZE;

  /*Update last free to the next free slot sequentially*/
  group->last_free = group->array[group->last_free];
  /*
    Check if we had a free tail to update,
    else move the free ahead till we encounter a free for a invalid group,
    in which case in initialize the tail for the group
  */
  if(group->tail >= 0 ) {
    group->array[group->tail] = group->last_free;
  } else { 
    group->free = group->last_free;
  }

  /*This is the first addition for the record.
    Set maxlen for this record*/
  if(max)
    *max = datalen;

  if(datalen > group->maxlen) 
    group->maxlen = datalen;

  /*Check if we have hit the ceiling for the group to
    make this guy into a valid group 
    with slots that can be read
  */
  if(group->last_free == -1) { 
    group->valid = 1;
  }
  /*Initialise the next data offset if required*/
  if(group->next_data_offset <= 0 ) {
    group->next_data_offset = group->offset + CLDB_GROUP_SIZE + CLDB_GROUP_FREE_LIST*CLDB_CTRL_BYTES;
    group->next_data_offset = ALIGN_VAL(group->next_data_offset,CLDB_BLOCK_SIZE);
  }
  data_offset = group->next_data_offset;
  /*Next data offset to be used*/
  group->next_data_offset += datalen;

  /*This group actually has now entered recycle phase*/
  if(group->last_free == -1) { 
    /*Update the next free group*/
    cldb_offset_t next_free_group;
    next_free_group = ALIGN_VAL(group->next_data_offset,CLDB_BLOCK_SIZE);
    if(next_free_group > CL_DB_HEADER(handle).next_free_group) 
      CL_DB_HEADER(handle).next_free_group = next_free_group;
  }
  if(data) {
    *data = data_offset;
  }
 out:
  return ret;
}

/*Link the group*/
static int cl_db_group_link(int handle,struct cl_db_group *group) { 
  int err = -1;
  cldb_offset_t last;
  last = CL_DB_HEADER(handle).last_group;

  if(last <= 0 ) {
    /*First group*/
    CL_DB_HEADER(handle).group = group->offset;
  } else { 
    /*Read the last group and link that*/
    struct cl_db_group record;
    if(cl_db_group_read_unfmt(handle,&record,last,0) < 0 ) {
      output("Error reading the last group...");
      goto out;
    }
    record.next = group->offset;
    if(cl_db_touch_group(handle,&record,0) < 0 ) {
      output("Error touching the last group...");
      goto out;
    }
  }
  CL_DB_HEADER(handle).last_group = group->offset;
  err = 0;
 out:
  return err;
}

/*Contruct a new group and make it available*/
cldb_offset_t cl_db_group_make(int handle,struct cl_db_group *group,long datalen,cldb_offset_t *data,cldb_offset_t *group_offset,long *max) { 
  register int i;
  cldb_offset_t ret = -1;
  cldb_offset_t offset;
  if(!group) { 
    output("Invalid param...");
    goto out;
  }

  memset(group,0x0,sizeof(*group));
  /*It isnt valid now*/
  group->valid = 0;
  for(i = 0; i < CLDB_GROUP_FREE_LIST;++i) { 
    group->array[i] = i+1;
  }

  /*Make the next of the last guy as full*/
  group->array[i-1] = -1;
  group->tail = -1;

  if(CL_DB_HEADER(handle).group == -1) {
    /*First group being made*/
    offset = CL_DB_HEADER(handle).offset + sizeof(struct cl_db_header);
    offset = ALIGN_VAL(offset,CLDB_BLOCK_SIZE);
  } else { 
    offset = CL_DB_HEADER(handle).next_free_group;
  }

  group->offset = offset;
  if(group_offset) 
    *group_offset = offset;

  group->next = -1;
  
  ret = cl_db_group_get(handle,group,datalen,data,max);
  if(ret < 0 ) {
    output("Error in making the group...");
    goto out;
  }
  if(cl_db_group_link(handle,group) < 0 ) {
    output("Error in linking the group...");
    ret = -1;
  }
  ++CL_DB_HEADER(handle).groups;
 out:
  return ret;
}

/*  
  Get a free offset by looping through the groups for the datalen.
  *Use this if you dont wanna the group cache:-)
  *Slow versions actually of get and release offset
*/

static cldb_offset_t do_cl_db_group_get_offset_slow(int handle,struct cl_db_group *group,long datalen,cldb_offset_t *data,cldb_offset_t *group_offset,long *max) { 
  cldb_offset_t i;
  cldb_offset_t ret = -1;

  if(!group) { 
    output("Invalid param...");
    goto out;
  }

  for(i = CL_DB_HEADER(handle).group; i != -1; i = group->next ) { 
    /*Now read the contents of the group*/
    if(cl_db_group_read_unfmt(handle,group,i,0) < 0 ) {
      output("Error in reading group at offset "CLDB_OFFSET_FMT,i);
      continue;
    }
    if(group->free == -1) {
      output("Group "CLDB_OFFSET_FMT " is full...",i);
      continue;
    }
    /*Now try getting the offset from this group*/
    if(( ret = cl_db_group_get(handle,group,datalen,data,max)) < 0 ) {
      output("Error in getting offset from group "CLDB_OFFSET_FMT,i);
      continue;
    }
    /*Found the offset*/
    break;
  }
  if(ret > 0 && group_offset) {
      *group_offset = i;
  }
 out:
  return ret;
}

#ifdef GROUP_SLOW

static cldb_offset_t __cl_db_group_get_offset_slow(int handle,long datalen,cldb_offset_t *data,cldb_offset_t *group_offset,long *max) { 
  cldb_offset_t ret = -1;
  struct cl_db_group group = {0};
  if((ret = do_cl_db_group_get_offset_slow(handle,&group,datalen,data,group_offset,max)) < 0 ) {
    /*We havent found any offset.So contruct a new group now*/
    if((ret = cl_db_group_make(handle,&group,datalen,data,group_offset,max)) < 0 ) {
      output("Error making group...");
      goto out;
    }
  } 
  /*Touch the group for commit*/
  if(cl_db_touch_group(handle,&group,0) < 0 ) { 
    ret = -1;
    output("Error touching group "CLDB_OFFSET_FMT,group.offset);
  }
 out:
  return ret;
}
 
/*Release the offset back to the group from which it was taken
*/
static int __cl_db_group_release_offset_slow(int handle,cldb_offset_t offset,cldb_offset_t group_offset) { 
  int err = -1;
  struct cl_db_group group = {0};
  cldb_offset_t temp;
  short index;
  if(offset <= 0 || group_offset <= 0 ) {
    output("Invalid param...");
    goto out;
  }

  if(cl_db_group_read_unfmt(handle,&group,group_offset,0) < 0 ) {
    output("Error reading group "CLDB_OFFSET_FMT,group_offset);
    goto out;
  }

  temp = group.offset + CLDB_GROUP_SIZE;
  index = (offset - temp)/CLDB_CTRL_BYTES;

  /*Release this index back to the group*/
  group.array[index] = group.free;
  group.free = index;
  /*Check if the group that we are freeing is valid or not.
   If not, update the tail if required
  */
  if(!group.valid && group.tail == -1) {
    group.tail = group.free;
  }
  if(cl_db_touch_group(handle,&group,0) < 0 ) {
    output("Error in touching group "CLDB_OFFSET_FMT,group.offset);
    goto out;
  }
  err = 0;
out:
  return err;
}

#else

/*
 *Fast versions of get and release offset with the group cache
 */
static cldb_offset_t __cl_db_group_get_offset_fast(int handle,long datalen,cldb_offset_t *data,cldb_offset_t *group_offset,long *max) { 
  cldb_offset_t ret = -1;
  struct cl_db_group group = {0};
  struct cl_db_group_cache *cache;
  /*Try finding a match with the free group that we have
    for invalid/half-full groups.   
    If we fail, then try looking up our sorted cache of datalens.
    If we fail that too, than try looking for a max free group 
    incase of valid groups.
    Else we make the group.
  */

  /*Try 1: Checking the half-full or invalid free_group if any
  */
  if(CL_DB_HEADER(handle).free_group > 0 ) { 
    if(cl_db_group_read_unfmt(handle,&group,CL_DB_HEADER(handle).free_group,0) < 0 ) { 
      output("Error in reading group at offset "CLDB_OFFSET_FMT,CL_DB_HEADER(handle).free_group);
      group.valid = 0;
      goto fetch_group;
    }
    if(!group.valid) {
      if((ret = cl_db_group_get(handle,&group,datalen,data,max))<0) {
        output("Error in getting offset from group even though max is pointing to group "CLDB_OFFSET_FMT,group.offset);
        goto fetch_group;
      } 
      *group_offset = group.offset;
      /*
        This is the ugliest part. But I cant help it right now
        Maybe a free group stack would help in future
      */
      if(group.last_free == -1) { 
        CL_DB_HEADER(handle).free_group = -1;
        CL_DB_HEADER(handle).maxlen = -1;
      }
      goto touch;
    }
  }
  /*Try 2:
   *Check the sorted group cache for a datalen hit:
   */
fetch_group:

  if((cache = cl_db_group_cache_find(handle,datalen))) { 
    ret = cache->offset;
    *group_offset = cache->group;
    *data = cache->data;
    *max = cache->maxlen;
    cl_db_group_cache_unlink(handle,cache);
    free((void*)cache);
    goto out;
  }

  /*Try 3:
   *Check the max free group incase we have one valid guy.
  */
  if(group.valid && CL_DB_HEADER(handle).maxlen >= datalen) { 
    if((ret = cl_db_group_get(handle,&group,datalen,data,max))<0) {
      output("Error in getting group info for group :"CLDB_OFFSET_FMT,group.offset);
      goto group_slow;
    }
    *group_offset = group.offset;
    CL_DB_HEADER(handle).free_group = -1;
    CL_DB_HEADER(handle).maxlen = -1;
    goto touch;
  }
  /*
   *Slow search to fetch offsets by scanning all the groups.
  */
group_slow:

#if 1
  /*Skip the group slowness
   */
  goto make_group;
#endif

  /*    
     The below bugger with fast mode shows up some inconsistencies.
   */
  CL_DB_GROUP_CACHE_SLOW_FILLS_INCR(handle);
  if((ret = do_cl_db_group_get_offset_slow(handle,&group,datalen,data,group_offset,max)) < 0) {
    output("group get slow failed...");
    goto make_group;
  }
  goto touch;

make_group:
  /*Fallback case:
   *We havent found any offset.So contruct a new group now:
   *Maybe we can do a slow lookup of all the groups for the free list 
   *as a last resort.
  */
  if((ret = cl_db_group_make(handle,&group,datalen,data,group_offset,max)) < 0  ) {
    output("Error making group...");
    goto out;
  }
  CL_DB_HEADER(handle).free_group = *group_offset;
touch:
  /*Touch the group for commit*/
  if(cl_db_touch_group(handle,&group,0) < 0 ) { 
    ret = -1;
    output("Error touching group "CLDB_OFFSET_FMT,group.offset);
  }

out:
  return ret;
}

/*Release the offset back to the group from which it was taken
*/
static int __cl_db_group_release_offset_fast(int handle,struct cl_db *ptr) {
  int err = -1;
  struct cl_db_group group = {0};
  cldb_offset_t temp;
  short index;
  long datalen;

  if(ptr->offset <= 0 || ptr->group <= 0) {
    output("Invalid param...");
    goto out;
  }
  datalen = ptr->keylen + ptr->datalen;
  /*
    First add this guy to the group cache
  */
  cl_db_group_cache_add(handle,ptr->offset,ptr->group,ptr->data_offset,datalen,ptr->maxlen);

  /*Now release to the group
   */
  if(cl_db_group_read_unfmt(handle,&group,ptr->group,0) < 0 ) {
    output("Error reading group "CLDB_OFFSET_FMT,ptr->group);
    goto out;
  }

  temp = group.offset + CLDB_GROUP_SIZE;
  index = (ptr->offset - temp)/CLDB_CTRL_BYTES;

  /*Release this index back to the group*/
  group.array[index] = group.free;
  group.free = index;

  /*Check if the group that we are freeing is valid or not.
    If not, update the tail if required
  */
  if(!group.valid && group.tail == -1) {
    group.tail = group.free;
  }
  
  if(CL_DB_HEADER(handle).free_group < 0 ) {
    CL_DB_HEADER(handle).free_group = ptr->group;
    CL_DB_HEADER(handle).maxlen = ptr->maxlen;
  } else {
    struct cl_db_group temp;
    if(cl_db_group_read_unfmt(handle,&temp,CL_DB_HEADER(handle).free_group,0) < 0 ) { 
      output("Error reading free group at offset : "CLDB_OFFSET_FMT,CL_DB_HEADER(handle).free_group);
      CL_DB_HEADER(handle).free_group = ptr->group;
      CL_DB_HEADER(handle).maxlen = ptr->maxlen;
      goto touch;
    }
    /*Update if we have a valid free group at our disposal*/
    if(temp.valid && (ptr->maxlen > CL_DB_HEADER(handle).maxlen)) {
      if(ptr->group != CL_DB_HEADER(handle).free_group)
        CL_DB_HEADER(handle).free_group = ptr->group;
      CL_DB_HEADER(handle).maxlen = ptr->maxlen;
    }
  }
touch:
  if(cl_db_touch_group(handle,&group,0) < 0 ) {
    output("Error in touching group "CLDB_OFFSET_FMT,group.offset);
    goto out;
  }
  err = 0;
out:
  return err;
}
#endif

/*
 *Get and release offsets
*/
cldb_offset_t cl_db_group_get_offset(int handle,long datalen,cldb_offset_t *data_offset,cldb_offset_t *group_offset,long *max) { 
  return __cl_db_group_get_offset(handle,datalen,data_offset,group_offset,max);
}

int cl_db_group_release_offset(int handle,struct cl_db *entry) { 
  return __cl_db_group_release_offset(handle,entry);
}

/*Initialise the group by setting up the first group
*/
int cl_db_group_init(int handle) { 
  struct cl_db_group group = {0};
  int err = -1;
  register int i;
  cldb_offset_t temp;
  memset(&CL_DB_GROUP_CACHE(handle),0x0,sizeof(struct cl_db_group_cache_records));
  for(i = 0; i < CLDB_GROUP_FREE_LIST;++i) 
    group.array[i] = i+1;
  group.array[i-1] = -1;
  group.tail = -1;
  temp = CL_DB_HEADER(handle).offset + sizeof(struct cl_db_header);
  temp = ALIGN_VAL(temp,CLDB_BLOCK_SIZE);
  /*Update the group offset*/
  group.offset = temp;
  group.next = -1;
  if(cl_db_touch_group(handle,&group,0) < 0 ) {
    output("Error in touching group "CLDB_OFFSET_FMT,group.offset);
    goto out;
  }
  /*Initialise the header of the DB
   */
  CL_DB_HEADER(handle).group = temp;
  CL_DB_HEADER(handle).last_group = temp;
  CL_DB_HEADER(handle).free_group = temp;
  CL_DB_HEADER(handle).groups = 1;
  err = 0;
out:
  return err;
}

#if 0
/*Start the group by writing the dummy group blocks following the db header
  including the blocks for the ctrl data so that we are set for
  db inserts/updates.
  We dont need this as we can write to the file with holes
*/
int cl_db_group_start(int handle) { 
  cldb_offset_t temp;
  char *pad;
  int err=-1;
  long size = CLDB_GROUP_SIZE;
  return 0;
  temp = CL_DB_HEADER(handle).offset + sizeof(struct cl_db_header);
  temp = ALIGN_VAL(temp,CLDB_BLOCK_SIZE);
  size += CLDB_GROUP_FREE_LIST*CLDB_CTRL_BYTES;
  if(!(pad = malloc(size))) {
    output("Error in mallocing during group start...");
    goto out;
  }
  memset(pad,0x0,size);
  output("Group start writing %ld bytes (%d ctrl recs of size:%ld)\n",size,CLDB_GROUP_FREE_LIST,CLDB_CTRL_BYTES);
  if((err = cl_db_write(CL_DB_FD(handle),pad,size,temp)) < 0 ) {
    output("Error in cl_db_write during group start...");
    goto out_free;
  }
  err = 0;
 out_free:
  free((void*)pad);
 out:
  return err;
}
#endif
