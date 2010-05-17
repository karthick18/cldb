/*Test database Deletions
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

  if(argc > 3 ) { 
    start = atoi(argv[3]);
  }

  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),CLDB_RDWR);

  if(handle < 0 ) {
    fprintf(stderr,"Error in opening db:%s\n",filename);
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
    char k[0xff+1];
    int klen;
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
    klen = strlen(k);
    if(cl_db_del(handle,klen,k)) {
      fprintf(stderr,"Error Deleting key:%.*s\n",klen,k);
    } else { 
#ifdef DEBUG_PRINT
      fprintf(stderr,"Deleted key:%.*s\n",klen,k);
#endif
    }
  }
  cl_db_close(handle);
  return 0;
}
