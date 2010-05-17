/*Test database additions*/

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

#define MAX_RECORDS (0xfffffff0)
#define DEFAULT_RECORDS (0x10)

int main(int argc,char **argv) { 
  register int i;
  register int x,y,count = 0;
  int flag = 1;
  int handle;
  int start = 0;
  struct cldb_data data;
  char filename[0xff+1];
  char db_filename[0xff+1] = DB_FILENAME;
  int records = DEFAULT_RECORDS;
  int db_flags = CLDB_RDWR | CLDB_CREAT | CLDB_TRUNC;
  if(argc > 1 ) {
    strncpy(db_filename,argv[1],sizeof(db_filename)-1);
  }
  if(argc > 2 ) {
    records = atoi(argv[2]);
    if(records > MAX_RECORDS) 
      records = MAX_RECORDS;
  }
  if(argc > 3 ) {
    db_flags &= ~(CLDB_CREAT | CLDB_TRUNC);
  }
  if(argc > 4 ) { 
    start = atoi(argv[4]);
  }

  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),db_flags);

  if(handle < 0 ) {
    fprintf(stderr,"Error in creating db:%s\n",filename);
    return -1;
  }

  if(!start) start = 1;
  if(!(start % 4) || (start % 4) == 1) { 
    if((start & 1)) { 
      x = start;
      y = x+1;
    } else { 
      flag = 0; /*start with even or y*/
      count = 1;
      y = start;
      x = y+1;
    }
  } else { 
    if((start & 1)) {
      flag = 0;
      y = start-1;
      x = y + 3;
    } else { 
      x = start+1;
      y = x-1;
      count = 1;
    }
  }

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
    if(cl_db_add(handle,&data)) {
      fprintf(stderr,"Error adding key:%.*s,data:%.*s\n",data.keylen,data.key,data.datalen,data.data);
    } else { 
#ifdef DEBUG_PRINT
      fprintf(stderr,"Added key:%.*s,data:%.*s\n",data.keylen,data.key,data.datalen,data.data);
#endif
    }
  }
  cl_db_close(handle);
  return 0;
}
