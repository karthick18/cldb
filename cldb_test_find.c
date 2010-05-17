/*Find all the records in the DB that were created
  using the cldb_test code.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "cldb_api.h"

#define FPRINTF(f,fmt,arg...)

#ifdef DEBUG
#define output(fmt,arg...) do { \
  fprintf(stderr,fmt ",File:%s,Line:%d,Function:%s\n",##arg,__FILE__,__LINE__,__FUNCTION__);\
}while(0)
#else
#define output(fmt,arg...)
#endif

#define MAX_RECORDS (0xfffffff0)
#define DEFAULT_RECORDS (0x10)

#define DB_FILENAME "kar3.db"
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

  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),CLDB_READ);

  if(handle < 0 ) {
    fprintf(stderr,"Error in opening db:%s",filename);
    return -1;
  }

  x = 1,y=2;
  FPRINTF(stderr,"Finding the records now...\n");
  for(i = 0; i < records ; ++i) {
    char k[0xff+1],d[0xff+1];
    int n;
    data.datalen = 0;
    data.data = NULL;
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
    if(cl_db_find(handle,data.keylen,data.key,&data) < 0) {
      fprintf(stderr,"Error Finding key:%.*s\n",data.keylen,data.key);
    } else { 
#ifdef DEBUG_FIND
      fprintf(stderr,"Found key:%.*s,data:%.*s\n",data.keylen,data.key,data.datalen,data.data);
      assert(memcmp(data.data,d,data.datalen) == 0);
#endif
      free((void*)data.data);
    }
  }
  FPRINTF(stderr,"\nFound done...\n");
  cl_db_close(handle);
  return 0;
}
