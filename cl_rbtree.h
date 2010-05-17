/*A.R.Karthick
  A rewrite of Rbtree implementation:
  Sent a patch to Andrea Arcangeli (AA) in this context
  which has a redundant check in the linux kernel rbtree.c.
  Note: The person using this utility has to code his
  own rbtree_search and rbtree_insert routines: 
  Search should be sorted:Simple binary tree search
  and same is the case with inserts.
*/

#ifndef _CL_RBTREE_H
#define _CL_RBTREE_H

#define RED   0
#define BLACK 1

extern int cl_rbtree_insert(int handle,struct cl_db *node,cldb_offset_t parent,int left);
extern int cl_rbtree_erase(int handle,struct cl_db *node);

#endif
