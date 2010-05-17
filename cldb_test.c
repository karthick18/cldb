/*Test database additions,deletions
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
#define MAX_RECORDS (0xfffffff0)
#define DEFAULT_RECORDS (0x10)

static char *prog;
static char *get_filename(char *filename,const char *db_filename,int size) {
  char *str;
  if(!(str = strrchr(db_filename,'/'))) { 
    snprintf(filename,size,"%s/%s",DB_BASEPATH,db_filename);
  } else {
    strncpy(filename,db_filename,size);
  }
  return filename;
}

static int get_index(int start,int *x_p,int *y_p,int *flag_p,int *count_p) {
  int x,y,flag,count;
  if(!x_p || !y_p || !flag_p || !count_p) {
    fprintf(stderr,"Invalid arg to get_index (%d:%d:%d:%d)\n",x_p ? 1:0,y_p ? 1:0,flag_p ? 1:0, count_p ? 1 : 0);
    return -1;
  }
  x = *x_p, y=*y_p, flag = *flag_p, count = *count_p;
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
  *x_p = x , *y_p = y, *flag_p = flag , *count_p = count;
  return 0;
}

static int do_del(int handle,int records,int start) { 
  register int i;
  int x,y,count = 0;
  int flag = 1;
  if(get_index(start,&x,&y,&flag,&count)) { 
    fprintf(stderr,"do_del: Error in get_index\n");
    return -1;
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
  return 0;
}

static int do_add(int handle,int records,int start) {
  register int i;
  int x,y,count = 0,flag = 1;
  struct cldb_data data = {0};

  if(get_index(start,&x,&y,&flag,&count)) { 
    fprintf(stderr,"do_add: get_index error\n");
    return -1;
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
  return 0;
}

static __inline__ volatile void usage(void) { 
  fprintf(stderr,"%s DBname [ no. of records ] [ start record ] [ start delete record ] [ no. of delete records ] [ no. of test iterations ]\n",prog ? : "UNKNOWN");
  exit(127);
}
int main(int argc,char **argv) { 
  int handle;
  int start = 1,delete_start = 1;
  char filename[0xff+1];
  char db_filename[0xff+1] = DB_FILENAME;
  int records = DEFAULT_RECORDS,delete_records = DEFAULT_RECORDS;
  int db_flags = CLDB_RDWR | CLDB_CREAT | CLDB_TRUNC;
  int iteration = 1;
  register int i;
  char *str;
  extern void cl_db_group_cache_display(int handle);

  prog = argv[0];
  if((str = strrchr(prog,'/'))) {
    prog = ++str;
  }

  if(argc == 1) { 
    usage();
  }
  if(argc > 1 ) {
    strncpy(db_filename,argv[1],sizeof(db_filename)-1);
  }
  if(argc > 2 ) {
    records = atoi(argv[2]);
    if(records > MAX_RECORDS) 
      records = MAX_RECORDS;
  }

  if(argc > 3 ) {
    /*db_flags &= ~(CLDB_CREAT | CLDB_TRUNC);*/
    start = atoi(argv[3]);
  }
  
  if(argc > 4 ) { 
    delete_start = atoi(argv[4]);
  }

  if(argc > 5 ) {
    delete_records = atoi(argv[5]);
    if(delete_records > MAX_RECORDS)
      delete_records = MAX_RECORDS;
  }
  if(argc > 6 ) {
    iteration = atoi(argv[6]);
  }
  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),db_flags);

  if(handle < 0 ) {
    fprintf(stderr,"Error in creating db:%s\n",filename);
    return -1;
  }
  if(!iteration) iteration = 1;
  for(i = 0; i < iteration; ++i) {
    fprintf(stderr,"\nIteration %d:Phase 1: Adding %d records starting at %d\n",i,records,start);
    assert(do_add(handle,records,start) == 0);
    fprintf(stderr,"\nIteration %d:Phase 2: Deleting %d records starting at %d\n",i,delete_records,delete_start);
    assert(do_del(handle,delete_records,delete_start) == 0 );
    if(!i) {
      records = delete_records;
      start = delete_start;
    }
  }
#ifdef DEBUG_PRINT
  fprintf(stderr,"Dumping the DB group cache...\n");
  cl_db_group_cache_display(handle);
  fprintf(stderr,"Dumping the DB tree...\n");
  cl_db_dump_tree(handle,NULL);
  fprintf(stderr,"Dumping the DB list...\n");
  cl_db_dump_list(handle);
#endif
  cl_db_close(handle);
  return 0;
}
