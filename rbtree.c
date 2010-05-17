/*A.R.Karthick: 
  An implementation of RBtrees:
  A patch was sent to AA(Andrea Arcangeli) to fix some redundancy
  in linux rbtree implementation.
*/

#include <stdio.h>
#include <stdlib.h>
#include "rbtree.h"


typedef void (rbtree_function) (struct rbtree_root *,struct rbtree *);

static rbtree_function rbtree_rotate_left,rbtree_rotate_right;
static void rbtree_erase_colour(struct rbtree_root *,struct rbtree *,struct rbtree *);

static void rbtree_rotate_left(struct rbtree_root *root,struct rbtree *node) {
  struct rbtree *right = node->right;
  if((node->right = right->left) ) 
    right->left->parent = node;
  if((right->parent = node->parent) ) {
    if(node == node->parent->left)
      node->parent->left = right;  
    else
      node->parent->right = right;
  }
  else
    root->root = right;
  node->parent = right;
  right->left = node;
}
static void rbtree_rotate_right(struct rbtree_root *root,struct rbtree *node){
  struct rbtree *left = node->left;
  if((node->left = left->right) )
    left->right->parent = node;
  if((left->parent = node->parent) ) {
    if(node == node->parent->left)
      node->parent->left = left;
    else 
      node->parent->right = left;
  }
  else 
    root->root = left;
  left->right = node;
  node->parent = left;
}

/*Balance a RBTREE*/

void rbtree_insert_colour(struct rbtree_root *root,struct rbtree *node)
{
  while(node != root->root && node->parent->colour == RED)
    {
      register struct rbtree *parent  = node->parent;
      register struct rbtree *gparent = parent->parent;
      if(parent == gparent->left){
        /*Case 1:
          Nodes uncle is RED.change colours of uncle ,parent and gparent
          and proceed with gparent as the next node.    
        */
        if(gparent->right && gparent->right->colour == RED) {
          parent->colour = gparent->right->colour = BLACK;
          gparent->colour = RED;
          node = gparent;
          continue; 
        }
        /*Case 2:
          uncle is BLACK.
          Now check if node is to the right or left of the parent.
          If the node is to the right of the parent,left rotate the parent,
          which will make the parent onto the left of the node.
          Interchange the positions of parent and node,
          change the colour of the parent to BLACK,gparent to RED,
          and rightrotate the gparent.
        */
        if(parent->right == node) {
          struct rbtree *tmp = NULL;
          rbtree_rotate_left(root,parent);
          tmp = node;
          node = parent;
          parent = tmp; 
        }
        parent->colour = BLACK;
        gparent->colour = RED;
        rbtree_rotate_right(root,gparent);
      }
      /*Interchange the left and the right*/
      else {
        if(gparent->left && gparent->left->colour == RED) {
          parent->colour = gparent->left->colour = BLACK;
          gparent->colour = RED;
          node = gparent;
          continue; 
        }
        if(parent->left == node) {
          struct rbtree *tmp = NULL;
          rbtree_rotate_right(root,parent);
          tmp = node;
          node = parent;
          parent = tmp; 
        }
        parent->colour = BLACK;
        gparent->colour = RED;
        rbtree_rotate_left(root,gparent);
      }
    }
  root->root->colour = BLACK;
}

/*Balance an RBTREE after an erase operation*/
static void rbtree_erase_colour(struct rbtree_root *root,struct rbtree *node,struct rbtree *parent){
  while((!node || node->colour == BLACK) && node != root->root)
    {
      if(node == parent->left){
        register struct rbtree *brother = parent->right;
        if(brother->colour == RED) {
          brother->colour = BLACK;
          parent->colour =  RED;
          rbtree_rotate_left(root,parent);
          brother = parent->right;
        }
        if((!brother->left  || brother->left->colour ==  BLACK) && 
           (!brother->right || brother->right->colour == BLACK)
           )
          {
            brother->colour = RED;
            node = parent;
            parent = node->parent;
          }
        else {
          if(!brother->right || brother->right->colour == BLACK) {
            /*Here,brothers left has to exist because we are going for a 
              right rotation with the brother:Andrea!! Are you listening?
            */
            brother->left->colour = BLACK;
            brother->colour = RED;
            rbtree_rotate_right(root,brother);
            brother = parent->right;
          } 
          brother->colour = parent->colour;
          parent->colour  = BLACK;
          brother->right->colour = BLACK;
          rbtree_rotate_left(root,parent);
          node = root->root;
          break;
        }
      }
      else {
        register struct rbtree *brother = parent->left;
        if(brother->colour == RED) {
          brother->colour = BLACK;
          parent->colour =  RED;
          rbtree_rotate_right(root,parent);
          brother = parent->left;
        }
        if((!brother->left  || brother->left->colour ==  BLACK) && 
           (!brother->right || brother->right->colour == BLACK)
           )
          {
            brother->colour = RED;
            node = parent;
            parent = node->parent;
          }
        else {
          if(!brother->left || brother->left->colour == BLACK) {
            /*Here,brothers right has to exist because we are going for a 
              left rotation with the brother:Andrea!! Are you listening?
            */
            brother->right->colour = BLACK;
            brother->colour = RED;
            rbtree_rotate_left(root,brother);
            brother = parent->left;
          } 
          brother->colour = parent->colour;
          parent->colour  = BLACK;
          brother->left->colour = BLACK;
          rbtree_rotate_right(root,parent);
          node = root->root;
          break;
        }
      }
    }
  if(node)
    node->colour = BLACK;
}

/*Erase a node in a RBTREE: 
  Perform the rebalancing act if the node to be deleted is BLACK.
  If the node to be deleted is RED,then the tree is already a RBTREE
  as deletions can be performed only in a balance tree.
*/
void rbtree_erase(struct rbtree_root *root,struct rbtree *node) {
  struct rbtree *parent = NULL,*child = NULL;
  unsigned int colour;
  if(!node->left)
    child = node->right;
  else if(!node->right)
    child = node->left;
  else {
    struct rbtree *old = node;
    struct rbtree *left = NULL;
    node = old->right;
    while((left = node->left) ) 
      node = left;
    child = node->right;
    parent = node->parent;
    colour = node->colour & 1;
    if(child)
      child->parent = parent;
    if(parent) {
      if(node == parent->left) 
	parent->left = child;
      else
        parent->right = child;
    }
    else{
     root->root = child;
    }
    if(node->parent == old) 
      parent = node;
    node->colour = old->colour;
    node->left =   old->left;
    node->right = old->right;
    if((node->parent = old->parent) ) {
      if(old == old->parent->left) 
	old->parent->left = node;
      else
        old->parent->right = node;
    }
    else{
      root->root = node; 
    }
    old->left->parent = node;
    if(old->right) 
      old->right->parent = node;
    goto rebalance;
  }
  parent = node->parent;
  colour = node->colour & 1;
  if(child)
    child->parent = parent;
  if(parent) {
    if(parent->left == node) 
      parent->left= child;
    else
      parent->right = child;
  }
  else{ 
    root->root = child;
  }
 rebalance:
  if(colour == BLACK) 
    rbtree_erase_colour(root,child,parent);
}

