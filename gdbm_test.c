/*Karthick:Test code for testing GDBM
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <gdbm.h>
#include "queue.h"

#ifdef GDBM_BLOCK_SIZE
#undef GDBM_BLOCK_SIZE
#endif

#define GDBM_BLOCK_SIZE (4096)
#define FPRINTF(f,fmt,arg...)

#ifdef DEBUG
#define output(fmt,arg...) do { \
  fprintf(stderr,fmt ",File:%s,Line:%d,Function:%s\n",##arg,__FILE__,__LINE__,__FUNCTION__);\
}while(0)
#else
#define output(fmt,arg...)
#endif

#define DB_FILENAME "/home/karthick/db/gdbm2.db"

#define MAX_RECORDS (0xffff)
#define DEFAULT_RECORDS (0x10)

static struct sample_queue { 
  struct queue_struct *head;
  struct queue_struct **tail;
  int count;
} sample_queue = { 
  .head = NULL,
  .tail = &sample_queue.head,
};

struct sample { 
  unsigned int keylen;
  unsigned int datalen;
  unsigned char *key;
  unsigned char *data;
  struct queue_struct queue;
};

static __inline__ void link_record(struct sample *ptr) {
  struct sample_queue *head = &sample_queue;
  struct queue_struct *element = &ptr->queue;
  QUEUE_ADD_TAIL(element,head);
  ++head->count;
}

static __inline__ void unlink_record(struct sample *ptr) { 
  struct sample_queue *head = &sample_queue;
  struct queue_struct *element = &ptr->queue;
  QUEUE_DEL(element,head);
  assert(--head->count >= 0);
}

static struct sample *add_record(unsigned int keylen,unsigned char *key,unsigned int datalen,unsigned char *data) { 
  struct sample *ptr;
  ptr = malloc(sizeof(struct sample));
  if(!ptr) { 
    output("Error in malloc:");
    return NULL;
  }
  memset(ptr,0x0,sizeof(*ptr));
  ptr->key = strdup(key);
  ptr->data = strdup(data);
  ptr->keylen = keylen;
  ptr->datalen = datalen;
  link_record(ptr);
  return ptr;
}

static __inline__ void display_queue(void) { 
  struct queue_struct *ptr;
  struct sample_queue *head = &sample_queue;
  queue_for_each(ptr,head) { 
    struct sample *sample =queue_entry(ptr,struct sample,queue);
    fprintf(stderr,"k=%s,d=%s,kl=%d,dl=%d\n",sample->key,sample->data,sample->keylen,sample->datalen);
  }
}

int main(int argc,char **argv) { 
  register int i;
  register int x,y,count = 0;
  int flag = 1;
  int err = -1;
  datum data,key;
  int records = DEFAULT_RECORDS;
  GDBM_FILE handle;
  if(argc > 1 ) {
    records = atoi(argv[1]);
    if(records > MAX_RECORDS) 
      records = MAX_RECORDS;
  }

  handle = gdbm_open(DB_FILENAME,GDBM_BLOCK_SIZE,GDBM_NEWDB | GDBM_WRITER | GDBM_NOLOCK,0644,NULL);

  if(!handle) {
    fprintf(stderr,"Error in creating db\n");
    return -1;
  }
  x = 1,y=2;
  for(i = 0; i < records ; ++i) {
    char k[0xff+1],d[0xff+1];
    int n;
    if(flag) {
      n = x;
      x += 2;
    } else {
      n = y;
      y += 2;
    }
    if(++count >= 2 ) {
      count = 0;
      flag ^= 1;
    }
    snprintf(k,sizeof(k),"K_%d",n);
    snprintf(d,sizeof(d),"ABCD_%d",n);
    key.dsize = strlen(k);
    key.dptr = k;
    data.dsize = strlen(d);
    data.dptr = d;
    assert(add_record(strlen(k),k,strlen(d),d) != NULL);
    if((err = gdbm_store(handle,key,data,GDBM_INSERT))) { 
      fprintf(stderr,"Error adding key:%.*s,data:%.*s\n",key.dsize,key.dptr,data.dsize,data.dptr);
    } else { 
#ifdef DEBUG_PRINT
      output("Added key:%s,data:%s\n",k,d);
#endif
    }
  }

#ifdef DEBUG_QUEUE
  display_queue();
#endif

  FPRINTF(stderr,"Finding the records now...\n");
  {
    struct sample_queue *queue = &sample_queue;
    register struct queue_struct *head;
    struct sample *ptr;
    queue_for_each(head,queue) { 
      ptr = queue_entry(head,struct sample,queue);
      key.dptr = ptr->key;
      key.dsize =ptr->keylen;

      data = gdbm_fetch(handle,key);
      if(!data.dptr) {
        fprintf(stderr,"Error in finding key:%.*s\n",key.dsize,key.dptr);
      } else {
        FPRINTF(stderr,"Data=%.*s:%d for key=%.*s\n",data.dsize,data.dptr,key.dsize,key.dptr);
        free((void*)data.dptr);
      }
    }
  }
  FPRINTF(stderr,"\nFound done...\n");
  gdbm_close(handle);
  return 0;
}
