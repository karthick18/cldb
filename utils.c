#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include "cldb.h"
#include "utils.h"

/*Add up all the 16 bit words and truncate the result to 16 bits*/

unsigned short cldb_cksum(unsigned char *data,unsigned int len) { 
  register unsigned short *ptr = (unsigned short *)data;
  int sum = 0;
  unsigned short result;

  while(len > 1) {
    sum += (int)*ptr++;
    len-=2;
  }

  if(len) { 
    sum += *((unsigned char *)ptr);
  }
  
  /*Add carry*/
  sum = (sum >> 16) + (sum&0xffff);
  sum += (sum >> 16);
  
  result = ~sum;
  return result;
}

unsigned char *cl_dup(const unsigned char *data,unsigned int len) { 
  unsigned char *str;
  str = (unsigned char *)malloc(len);
  if(!str) {
	output("Error in malloc");
	return NULL;
  }
  memcpy(str,data,len);
  return str;
}

long cldb_setmem(long val,int f) { 
  struct sysinfo info;
  long ram;
  if(sysinfo(&info) < 0 ) {
    output("Error in getting sysinfo");
    return val;
  }
  ram = info.totalram;
  if(!f) 
    f = 4;
  ram/= f;
  if(!ram) 
    ram = 1;
  ram *=info.mem_unit;
  val = MIN(ram,val);
  output("RAM = %ld,%ldkb,mem unit=%d",ram,(ram*info.mem_unit) >> 10,info.mem_unit);
  output("setmem returned: %ld kb",val >> 10);
  return val;
}

char *get_process(char *str) { 
  char buffer[0xff+1];
  FILE *fptr;
  char *ptr = str;
  if(!str) return str;
  str[0] = 0;
  snprintf(buffer,sizeof(buffer),"ps -p %d -o comm=",getpid());
  fptr = popen(buffer,"r");
  if(fptr) { 
    if(fgets(str,0xff,fptr)) { 
      char *s;
      int nbytes = strlen(str);
      if(str[nbytes-1] == '\n') str[nbytes-1] = 0;
      if((s = strrchr(str,'/'))) ptr == ++s;
      
    }
    pclose(fptr);
  }
  return ptr;
}
