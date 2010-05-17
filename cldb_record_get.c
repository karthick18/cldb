/*Get all the records in the DB
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

static __inline__ void do_display(struct cldb_data *data) { 
  fprintf(stderr,"Key = %.*s,Data=%.*s\n",data->keylen,data->key,data->datalen,data->data);
}

int main(int argc,char **argv) { 
  int handle;
  struct cldb_data data = {0};
  char filename[0xff+1];
  char db_filename[0xff+1] = DB_FILENAME;
  int err =-1;
  unsigned char *last_key = NULL;

  if(argc > 1 ) {
    strncpy(db_filename,argv[1],sizeof(db_filename)-1);
  }

  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),CLDB_READ);

  if(handle < 0 ) {
    fprintf(stderr,"Error in opening db:%s",filename);
    goto out;
  }

  err = cl_db_first_record(handle,&data);

  if(err) { 
    if(err == CLDB_END) { 
      fprintf(stderr,"Database %s empty\n",filename);
    } else {
      fprintf(stderr,"Error in first record get\n");
    }
    goto out_close;
  }

  do_display(&data);
  last_key = data.key;
  
  while ( (err = cl_db_next_record(handle,&data)) == 0 ) { 
    do_display(&data);
    free((void*)last_key);
    free((void*)data.data);
    last_key = data.key;
  }
  free((void*)last_key);

  if(err != CLDB_END) {
    fprintf(stderr,"Error in next record get...\n");
    goto out_close;
  }

  err = 0;
out_close:
  if(cl_db_close(handle)){
    fprintf(stderr,"Error in db close\n");
  }

out:
  return err;
}
