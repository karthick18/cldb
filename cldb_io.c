/*Just the basic I/O routines for this guy*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include "cldb.h"
#include "cldb_pack.h"
#include "cldb_io.h"

int cl_db_read(int fd,char *buffer,int bytes,cldb_offset_t offset) { 
  register char *s = buffer;
  int err = -1;
  int n = 0,rem = bytes;
  if(!buffer || !bytes) { 
	output("Read error\n");
	goto out;
  }

  if(offset >= 0 ) {
	if(lseek(fd,offset,SEEK_SET) < 0) { 
      output("Error in lseek\n");
      goto out;
	}
  }

  while ( rem && (n = read(fd,s,rem)) > 0 ) {
	s += n;
	rem -= n;
  }
  if(n < 0 ) { 
	output("Error in read:%s\n",strerror(errno));
	goto out;
  }
  err = bytes - rem;
out:
  return err;
}

int cl_db_write(int fd,char *buffer,int bytes,cldb_offset_t offset) { 
  int n,rem = bytes;
  register char *s = buffer;
  int err = -1;

  if(!s || !bytes) { 
    output("Error in write.(invalid param)\n");
    goto out;
  }

  if(offset >= 0 ) { 
    if(lseek(fd,offset,SEEK_SET) < 0 ) { 
      output("Error in lseek:%s\n",strerror(errno));
      goto out;
    }
  }

  while(rem) { 
    n = write(fd,s,rem);
    if(n <= 0 ) { 
      output("Error in write\n");
      goto out;
    }
    s += n;
    rem -= n;
  }
  err = bytes;
 out:
  return err;
}

