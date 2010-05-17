#ifndef _UTILS_H
#define _UTILS_H

#define CL_DB_HASHSTART (5381UL)
#ifdef MIN
#undef MIN
#endif
#define MIN(a,b) ( (a) < (b) ? (a) : (b) )

#define GOLDEN_RATIO_PRIME (0x9e370001UL)
#define ALIGN_VAL(v,a)  (((v) + (a)-1) & ~((a)-1))

extern unsigned short cldb_cksum(unsigned char *data,unsigned int len);
extern unsigned char *cl_dup(const unsigned char *data,unsigned int len);
char *get_process(char *);
extern long cldb_setmem(long val,int factor);

static __inline__ unsigned long cl_db_hashadd(unsigned long hash,const unsigned char c)
{
  hash += (hash << 5);
  return hash ^ c;
}

static __inline__ unsigned long cl_db_hash(const char *buf,unsigned int len)
{
  unsigned long hash = CL_DB_HASHSTART;
  while (len--)
	hash = cl_db_hashadd(hash,*buf++);

  return hash;
}

/*Taken from linux/hash.h for efficient hash for longs*/
static __inline__ cldb_offset_t cl_db_hash_offset(cldb_offset_t val, unsigned int bits)
{
	unsigned long long hash = (unsigned long long)val;
	/*  Sigh, gcc can't optimise this alone like it does for 32 bits. */
	unsigned long long n = hash;
	n <<= 18;
	hash -= n;
	n <<= 33;
	hash -= n;
	n <<= 3;
	hash += n;
	n <<= 3;
	hash -= n;
	n <<= 4;
	hash += n;
	n <<= 2;
	hash += n;
	/* High bits are more random, so use them. */
	return (cldb_offset_t)(hash >> (64 - bits));
}

static __inline__ unsigned long cl_db_hash_long(unsigned long val,unsigned int bits) { 
  unsigned long hash;
  if(sizeof(val) == 8 ) {
    return (unsigned long) cl_db_hash_offset((cldb_offset_t)val,bits);
  }
  hash *= GOLDEN_RATIO_PRIME;
  return  hash >> (32 - bits) ;
}

#endif
