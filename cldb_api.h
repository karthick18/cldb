/*Database API for users.
 */
#ifndef _CLDB_API_H
#define _CLDB_API_H

#ifdef __cplusplus
extern "C" {
#endif

#define CLDB_READ   (0x1)
#define CLDB_WRITE  (0x2)
#define CLDB_CREAT  (0x8)
#define CLDB_TRUNC  (0x10)
#define CLDB_APPEND (0x20)
#define CLDB_RDWR   (CLDB_READ | CLDB_WRITE)
#define CLDB_END    (-2)
#define CLDB_NO_MEM (-3)
#define CLDB_DUPLICATE (1)

  struct cldb_data { 
    unsigned int keylen;
    unsigned int datalen;
    unsigned char *key;
    unsigned char *data;
  };

  extern int cl_db_open(const char *name,int flags);
  extern int cl_db_close(int handle);
  extern int cl_db_add(int handle,struct cldb_data *data);
  extern int cl_db_del(int handle,unsigned int keylen,unsigned char *key);
  extern int cl_db_update(int handle,struct cldb_data *data);
  extern int cl_db_find(int handle,unsigned int keylen,unsigned char *key,struct cldb_data *data);
  extern int cl_db_dump_tree(int handle,void (*display)(void *data));
  extern int cl_db_dump_list(int handle);
  extern int cl_db_first_record(int handle,struct cldb_data *data);
  extern int cl_db_next_record(int handle,struct cldb_data *data);
  extern int cl_db_free_record(int handle,void *record);
  extern int cl_db_free_key(int handle,void *key);

#ifdef __cplusplus
}
#endif

#endif

