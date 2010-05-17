/*Display the on-disk tree or linked list of the DB
  To display the tree: run just with the dbname.
  To display the linked list: Run with the second arg. as greater than 0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cldb_api.h"

#define DB_FILENAME "kar2.db"
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
  char db_filename[0xff+1] = DB_FILENAME;
  char filename[0xff+1];
  int display = 0; /*display tree*/

  if(argc > 1 ) {
    strncpy(db_filename,argv[1],sizeof(db_filename)-1);
  } 

  if(argc > 2 ) { 
    display = atoi(argv[2]);
  }
  
  handle = cl_db_open(get_filename(filename,db_filename,sizeof(filename)),CLDB_READ);

  if(handle < 0 ) {
    fprintf(stderr,"Error in opening db:%s\n",filename);
    return -1;
  }
  
  if(!display) {
    fprintf(stderr,"\nDisplaying the Database on-disk TREE...\n");
    cl_db_dump_tree(handle,NULL);
    fprintf(stderr,"\nDone...\n");
  } else { 
    fprintf(stderr,"\nDisplaying the Database list...\n");
    cl_db_dump_list(handle);
  }
  cl_db_close(handle);
  return 0;
}
