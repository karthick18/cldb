/*Author: A.R.Karthick
  A rewrite of Rbtree implementation:
  Sent a patch to Andrea Arcangeli (AA) in this context
  which has a redundant check in the linux kernel rbtree.c.
  Note: The person using this utility has to code his
  own rbtree_search and rbtree_insert routines: 
  Search should be sorted:Simple binary tree search
  and same is the case with inserts.
*/

#ifndef _RBTREE_H
#define _RBTREE_H

#define RED   0
#define BLACK 1

struct rbtree{
  struct rbtree *left;
  struct rbtree *right;
  struct rbtree *parent;
  unsigned int colour:1;
};

struct rbtree_root{
  struct rbtree *root;
};

extern void rbtree_insert_colour(struct rbtree_root *,struct rbtree *);
extern void rbtree_erase(struct rbtree_root *,struct rbtree *);

static __inline__ void __rbtree_insert(struct rbtree *node,struct rbtree *parent,struct rbtree **link){
  node->left = node->right = NULL;
  node->parent = parent;
  node->colour = RED;
  *link = node;
}

static __inline__ void __rbtree_erase(struct rbtree_root *root,struct rbtree *node){
  rbtree_erase(root,node);
}

#define rbtree_entry(node,cast,field)   \
(cast*) ((unsigned char*)node - (unsigned long)(&((cast*)0)->field) )

#endif
