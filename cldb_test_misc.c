/*
  Cldb misc test that does add/find and db dump.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "cldb_api.h"
#include "queue.h"
#define FPRINTF(f,fmt,arg...)

#ifdef DEBUG
#define output(fmt,arg...) do { \
  fprintf(stderr,fmt ",File:%s,Line:%d,Function:%s\n",##arg,__FILE__,__LINE__,__FUNCTION__);\
}while(0)
#else
#define output(fmt,arg...)
#endif

#define MAX_RECORDS (0xfffff)
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

#define DB_FILENAME "kar4.db"
#define DB_BASEPATH "/home/karthick/db"

static char *get_filename(char *filename,const char *db_filename,int size) {
  char *str;
  if(!(str = strrchr(db_filename,'/'))) { 
    snprintf(filename,size,"%s/%s",DB_BASEPATH,db_filename);
  } else {
    strncpy(filename,db_filename,size);
  }
  return filename;
}

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
    fprintf(stderr,"k=%.*s,d=%.*s,kl=%d,dl=%d\n",sample->keylen,sample->key,sample->datalen,sample->data,sample->keylen,sample->datalen);
  }
}

int main(int argc,char **argv) { 
  register int i;
  register int x,y,count = 0;
  int flag = 1;
  int handle;
  struct cldb_data data;
  char filename[0xff+1];
  char db_filename[0xff+1] = DB_FILENAME;
  int records = DEFAULT_RECORDS;

  if(argc > 1 ) {
    strncpy(db_filename,argv[1],sizeof(db_filename)-1);
  }

  if(argc > 2 ) {
    records = atoi(argv[2]);
    if(records > MAX_RECORDS) 
      records = MAX_RECORDS;
  }

  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),CLDB_RDWR | CLDB_CREAT | CLDB_TRUNC);

  if(handle < 0 ) {
    fprintf(stderr,"Error in creating db:%s\n",filename);
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
    data.keylen = strlen(k);
    data.key = k;
    data.data = d;
    data.datalen = strlen(d);
    assert(add_record(strlen(k),k,strlen(d),d) != NULL);

    if(cl_db_add(handle,&data)) {
      output("Error adding key:%s,data:%s",k,d);
    } else { 
#ifdef DEBUG_PRINT
      output("Added key:%s,data:%s\n",k,d);
#endif
    }
  }
  FPRINTF(stderr,"\nDisplaying the Database on-disk TREE...\n");
  cl_db_dump_tree(handle,NULL);
  FPRINTF(stderr,"\nDone...\n");

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
      data.key = ptr->key;
      data.keylen=ptr->keylen;
      data.data = NULL;
      data.datalen = 0;
      if(cl_db_find(handle,data.keylen,data.key,&data) < 0 ) {
        fprintf(stderr,"Error in finding key:%s\n",data.key);
      } else {
        FPRINTF(stderr,"Data=%.*s:%d for key=%.*s\n",data.datalen,data.data,data.datalen,data.keylen,data.key);
        free((void*)data.data);
      }
    }
  }
  FPRINTF(stderr,"\nFound done...\n");

  fprintf(stderr,"\nDisplaying the Database list...\n");
  cl_db_dump_list(handle);

  cl_db_close(handle);
  return 0;
}
