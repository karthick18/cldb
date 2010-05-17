/*Test database updates
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
  struct cldb_data data;
  char filename[0xff+1];
  char db_filename[0xff+1] = DB_FILENAME;
  char suffix[0xff+1] = "XYZ";
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
    strncpy(suffix,argv[3],sizeof(suffix)-1);
  }

  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),CLDB_RDWR );

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
    snprintf(d,sizeof(d),"ABCD_%d_%s",n,suffix);
    data.keylen = strlen(k);
    data.key = k;
    data.data = d;
    data.datalen = strlen(d);
    if(cl_db_update(handle,&data)) {
      fprintf(stderr,"Error updating key:%.*s,data:%.*s\n",data.keylen,data.key,data.datalen,data.data);
    } else { 
#ifdef DEBUG_PRINT
      fprintf(stderr,"Updated key:%.*s,data:%.*s\n",data.keylen,data.key,data.datalen,data.data);
#endif
    }
  }
  cl_db_close(handle);
  return 0;
}
