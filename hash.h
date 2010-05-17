/*Karthick: A generic hash*/
#ifndef _HASH_H
#define _HASH_H

#define HASH_TABLE_BITS (10)

#define HASH_TABLE_SIZE (1 << HASH_TABLE_BITS)

struct hash_struct { 
    struct hash_struct *next;
    struct hash_struct **pprev;
};


static __inline__ int add_hash(struct hash_struct **hash_table,unsigned int key,struct hash_struct *ptr) { 
  struct hash_struct **base;
  int err = -1;
  if(!hash_table || key >= HASH_TABLE_SIZE) 
	goto out;
  base = hash_table + key;
  if( (ptr->next = *base) ) { 
	ptr->next->pprev = &ptr->next;
  }
  *(ptr->pprev = base) = ptr;
  err = 0;
out:
  return err;
}

static __inline__ void del_hash(struct hash_struct *ptr) { 
    if(ptr && ptr->pprev) {
	if(ptr->next)
	    ptr->next->pprev = ptr->pprev;
	*ptr->pprev = ptr->next;
	ptr->next = NULL,ptr->pprev = NULL;
    }
}

#define hash_entry(element,cast,field) \
(cast*) ((unsigned char *)(element) - (unsigned long)&(((cast*)0)->field) )

#define hash_for_each(iter,bucket) \
 for( iter = (bucket); iter; iter = (bucket)->next )

#define hash_for_each_safe(iter,temp,bucket) \
 for(iter=bucket,temp=iter?iter->next:NULL;iter;iter=temp,temp=temp?temp->next:NULL)

#endif
	
