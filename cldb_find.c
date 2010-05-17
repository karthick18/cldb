/*Find a record from the DB given a Key
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "cldb_api.h"

#ifdef DEBUG
#define output(fmt,arg...) do { \
  fprintf(stderr,fmt ",File:%s,Line:%d,Function:%s\n",##arg,__FILE__,__LINE__,__FUNCTION__);\
}while(0)
#else
#define output(fmt,arg...)
#endif


#define MAX_RECORDS (0x1000)
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
  int handle;
  char filename[0xff+1];
  char db_filename[0xff+1] = DB_FILENAME;
  struct cldb_data data;
  char k[0xff+1] = "K_1";
  int klen = strlen(k);
  if(argc > 1 ) {
    strncpy(db_filename,argv[1],sizeof(db_filename)-1);
  }

  if(argc > 2 ) {
    strncpy(k,argv[2],sizeof(k)-1);
    klen = strlen(k);
  }

  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),CLDB_READ);

  if(handle < 0 ) {
    fprintf(stderr,"Error in opening db:%s\n",filename);
    return -1;
  }
  if(cl_db_find(handle,klen,k,&data) < 0 ) { 
    fprintf(stderr,"Error in finding key:%.*s\n",klen,k);
  }else {
    fprintf(stderr,"Found data:%.*s for key:%.*s\n",data.datalen,data.data,klen,k);
    free((void*)data.data);
  }
  cl_db_close(handle);
  return 0;
}
