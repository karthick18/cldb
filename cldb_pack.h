/*Database pack/unpack routines for formatting
*/
#ifndef _CLDB_PACK_H
#define _CLDB_PACK_H

#define CLDB_PACK_LONG(v,s) do { \
 if(sizeof(long) == 4 ) { \
   CLDB_PACK_32(v,s);\
 } else { \
   CLDB_PACK_64(v,s);\
 }\
}while(0)

#define CLDB_UNPACK_LONG(v,s) do { \
  if(sizeof(long) == 4 ) { \
   CLDB_UNPACK_32(v,s) ;\
  } else { \
   CLDB_UNPACK_64(v,s) ;\
 }\
}while(0)
  
#define CLDB_PACK_BYTE(v,s,n,shift)  \
 ( (unsigned char *)(s) )[n] = (((v) >> (shift)) & 0xff)

#define CLDB_PACK_8(v,s)  do { \
  CLDB_PACK_BYTE(v,s,0,0);\
}while(0)

#define CLDB_PACK_16(v,s) do { \
  CLDB_PACK_BYTE(v,s,0,8);\
  CLDB_PACK_BYTE(v,s,1,0);\
}while(0)

#define CLDB_PACK_32(v,s) do { \
 CLDB_PACK_BYTE(v,s,0,24);\
 CLDB_PACK_BYTE(v,s,1,16);\
 CLDB_PACK_BYTE(v,s,2,8) ;\
 CLDB_PACK_BYTE(v,s,3,0) ;\
}while(0)

#define CLDB_PACK_64(v,s) do { \
 CLDB_PACK_BYTE((unsigned long long)(v),s,0,56);\
 CLDB_PACK_BYTE((unsigned long long)(v),s,1,48);\
 CLDB_PACK_BYTE((unsigned long long)(v),s,2,40);\
 CLDB_PACK_BYTE((unsigned long long)(v),s,3,32);\
 CLDB_PACK_BYTE((unsigned long long)(v),s,4,24);\
 CLDB_PACK_BYTE((unsigned long long)(v),s,5,16);\
 CLDB_PACK_BYTE((unsigned long long)(v),s,6,8);\
 CLDB_PACK_BYTE((unsigned long long)(v),s,7,0);\
}while(0)

#define CLDB_UNPACK_8(v,s) do { \
  ( (v) = ((unsigned char *)(s))[0] );\
}while(0)

#define CLDB_UNPACK_16(v,s) do { \
  (v) = ( ( (unsigned short) ((unsigned char *)(s))[0] << 8 ) | \
          ((unsigned short)((unsigned char *)(s))[1]) ) ;\
}while(0)

#define CLDB_UNPACK_32(v,s) do { \
 (v) = ( ( (unsigned long) ((unsigned char *)(s))[0] << 24 ) | \
          ( (unsigned long) ((unsigned char *)(s))[1] << 16 ) | \
          ( (unsigned long) ((unsigned char *)(s))[2] << 8  ) | \
          ( (unsigned long) ((unsigned char *)(s))[3] ) );\
}while(0)

#define CLDB_UNPACK_64(v,s) do { \
 (v) = ((unsigned long long) \
	( (unsigned long long)((unsigned char *)(s))[0] << 56 ) | \
	( (unsigned long long)((unsigned char *)(s))[1] << 48 ) | \
	( (unsigned long long)((unsigned char *)(s))[2] << 40 ) | \
	( (unsigned long long)((unsigned char *)(s))[3] << 32 ) | \
	( (unsigned long long)((unsigned char *)(s))[4] << 24 ) | \
	( (unsigned long long)((unsigned char *)(s))[5] << 16 ) | \
	( (unsigned long long)((unsigned char *)(s))[6] << 8 ) | \
	( (unsigned long long)((unsigned char *)(s))[7]) \
	);\
}while(0)

#endif

 
