/*A.R.Karthick: 
  An implementation of RBtrees for ON-DISK DB:
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "cldb.h"
#include "cldb_pack.h"
#include "cl_rbtree.h"

static int cl_rbtree_rotate_left(int handle,struct cl_db *node,struct cl_db *right) {
  struct cl_db right_left;
  struct cl_db node_parent;
  int err = -1;
  if(right->left != -1) { 
    if(cl_db_record_read_unfmt(handle,&right_left,right->left,CLDB_CTRL_DATA) < 0 ) { 
      output("Error in rotate left");
      goto out;
    }
  }

  if(node->parent != -1) { 
    if(cl_db_record_read_unfmt(handle,&node_parent,node->parent,CLDB_CTRL_DATA) < 0 ) { 
      output("Error in rotate left");
      goto out;
    }
  }

  if((node->right = right->left) != -1 ) {
    right_left.parent = node->offset;
    CL_DB_TOUCH(handle,&right_left,CLDB_CTRL_DATA);
  }

  if((right->parent = node->parent) != -1 ) {
    if(node->offset == node_parent.left)
      node_parent.left = right->offset;  
    else
      node_parent.right = right->offset;
    CL_DB_TOUCH(handle,&node_parent,CLDB_CTRL_DATA);
  }
  else { 
    CL_DB_ROOT(handle) = right->offset;
  }
  node->parent = right->offset;
  right->left = node->offset;
  CL_DB_TOUCH(handle,node,CLDB_CTRL_DATA);
  CL_DB_TOUCH(handle,right,CLDB_CTRL_DATA);
  err = 0;
 out:
  return err;
}

static int cl_rbtree_rotate_right(int handle,struct cl_db *node,struct cl_db *left) {
  struct cl_db left_right;
  struct cl_db node_parent;
  int err = -1;

  if(left->right != -1) { 
    if(cl_db_record_read_unfmt(handle,&left_right,left->right,CLDB_CTRL_DATA) < 0 ) { 
      output("Error in rotate right");
      goto out;
    }
  }

  if(node->parent != -1) { 
    if(cl_db_record_read_unfmt(handle,&node_parent,node->parent,CLDB_CTRL_DATA) < 0 ) { 
      output("Error in rotate right");
      goto out;
    }
  }

  if((node->left = left->right) != -1 ) {
    left_right.parent = node->offset;
    CL_DB_TOUCH(handle,&left_right,CLDB_CTRL_DATA);
  }

  if((left->parent = node->parent) != -1 ) {
    if(node->offset == node_parent.left)
      node_parent.left = left->offset;
    else 
      node_parent.right = left->offset;
    CL_DB_TOUCH(handle,&node_parent,CLDB_CTRL_DATA);
  } else {
    CL_DB_ROOT(handle) = left->offset;
  }

  left->right = node->offset;
  node->parent = left->offset;
  CL_DB_TOUCH(handle,node,CLDB_CTRL_DATA);
  CL_DB_TOUCH(handle,left,CLDB_CTRL_DATA);

  err = 0;
 out:
  return err;
}

static int cl_rbtree_touch_root(int handle) { 
  cldb_offset_t root = CL_DB_ROOT(handle);
  struct cl_db record;
  int err = -1;
  if(cl_db_record_read_unfmt(handle,&record,root,CLDB_CTRL_DATA) < 0 ) {
    output("Error in reading root");
    goto out;
  }
  if(record.colour != BLACK) {
    record.colour = BLACK;
    CL_DB_TOUCH(handle,&record,CLDB_CTRL_DATA);
  }
  err = 0;
 out:
  return err;
}

/*Balance a RBTREE*/

static int cl_rbtree_insert_colour(int handle,struct cl_db *node,struct cl_db *parent)
{
  cldb_offset_t  node_offset,cur_node_offset;
  cldb_offset_t root_offset = CL_DB_ROOT(handle);
  int err = -1;
  
  if(root_offset == -1 || !parent) {
    node->colour = BLACK;
    CL_DB_TOUCH(handle,node,CLDB_CTRL_DATA);
    CL_DB_ROOT(handle) = node->offset;
    return 0;
  }
  cur_node_offset = node_offset = node->offset;

  while(cur_node_offset != CL_DB_ROOT(handle) )
    {
      struct cl_db node_parent,*p_parent = &node_parent;
      struct cl_db node_gparent,*p_gparent = &node_gparent;
      struct cl_db temp_node,*p_node = &temp_node;
      cldb_offset_t gparent_offset;
      cldb_offset_t cur_parent_offset;
      if(cl_db_record_read_unfmt(handle,p_node,cur_node_offset,CLDB_CTRL_DATA) < 0 ) {
        output("Error in reading node");
        goto out;
      }
      cur_parent_offset = p_node->parent;
      assert(cur_parent_offset > 0);

      if(cl_db_record_read_unfmt(handle,p_parent,cur_parent_offset,CLDB_CTRL_DATA) < 0 ) { 
        output("Error in reading parent in insert colour");
        goto out;
      }
      if(p_parent->colour != RED) 
        break;
      gparent_offset = p_parent->parent;
      assert(gparent_offset > 0 );
      if(cl_db_record_read_unfmt(handle,p_gparent,gparent_offset,CLDB_CTRL_DATA) < 0 ) { 
        output("Error in reading gparent in insert colour");
        goto out;
      }
      if(p_parent->offset == p_gparent->left) {
        /*Case 1:
          Nodes uncle is RED.change colours of uncle ,parent and gparent
          and proceed with gparent as the next node.    
        */
        if(p_gparent->right != -1) {
          struct cl_db record;
          if(cl_db_record_read_unfmt(handle,&record,p_gparent->right,CLDB_CTRL_DATA) < 0 ) { 
            output("Error in reading gparent right");
            goto out;
          }
          if(record.colour == RED) {
            p_parent->colour = record.colour = BLACK;
            p_gparent->colour = RED;
            CL_DB_TOUCH(handle,p_gparent,CLDB_CTRL_DATA);
            CL_DB_TOUCH(handle,p_parent,CLDB_CTRL_DATA);
            CL_DB_TOUCH(handle,&record,CLDB_CTRL_DATA);
            cur_node_offset = p_gparent->offset;
            continue; 
          }
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
        if(p_parent->right == p_node->offset) {
          struct cl_db *tmp;
          cl_rbtree_rotate_left(handle,p_parent,p_node);
          tmp = p_node;
          p_node = p_parent;
          p_parent = tmp;
        }
        p_parent->colour = BLACK;
        p_gparent->colour = RED;
        cl_rbtree_rotate_right(handle,p_gparent,p_parent);
        cur_node_offset = p_node->offset;
      }
      /*Interchange the left and the right*/
      else {
        if(p_gparent->left != -1) {
          struct cl_db record;
          if(cl_db_record_read_unfmt(handle,&record,p_gparent->left,CLDB_CTRL_DATA) < 0 ) { 
            output("Error in reading gparent left");
            goto out;
          }
          if(record.colour == RED) {
            p_parent->colour = record.colour = BLACK;
            p_gparent->colour = RED;
            CL_DB_TOUCH(handle,p_gparent,CLDB_CTRL_DATA);
            CL_DB_TOUCH(handle,p_parent,CLDB_CTRL_DATA);
            CL_DB_TOUCH(handle,&record,CLDB_CTRL_DATA);
            cur_node_offset = p_gparent->offset;
            continue; 
          }
        }
        if(p_parent->left == p_node->offset) {
          struct cl_db *tmp;
          cl_rbtree_rotate_right(handle,p_parent,p_node);
          tmp = p_node;
          p_node = p_parent;
          p_parent = tmp;
        }
        p_parent->colour = BLACK;
        p_gparent->colour = RED;
        cl_rbtree_rotate_left(handle,p_gparent,p_parent);
        cur_node_offset = p_node->offset;
      }
    }
  err = cl_rbtree_touch_root(handle);
 out:
  return err;
}

int cl_rbtree_insert(int handle,struct cl_db *ptr,cldb_offset_t parent,int left) {
  struct cl_db parent_record;
  struct cl_db *p_parent = &parent_record;
  int err = -1;
  ptr->left = -1;
  ptr->right = -1;
  ptr->parent = parent;
  ptr->colour = RED;

  if(parent != -1) { 

    if(left == -1) { 
      output("Left invalid");
      goto out;
    }

    if(cl_db_record_read_unfmt(handle,p_parent,parent,CLDB_CTRL_DATA) < 0 ) { 
      output("Error in reading record");
      goto out;
    }
    if(left) {
      p_parent->left = ptr->offset;
    }
    else {
      p_parent->right = ptr->offset;
    }
    CL_DB_TOUCH(handle,p_parent,CLDB_CTRL_DATA);
  } else { 
    p_parent = NULL;
  }

  CL_DB_TOUCH(handle,ptr,CLDB_CTRL_DATA);

  if(cl_rbtree_insert_colour(handle,ptr,p_parent) < 0 ) {
    output("Error in rbtree insert colour");
    goto out;
  }

  err = 0;
 out:
  return err;
}

/*Balance an RBTREE after an erase operation*/
static int cl_rbtree_erase_colour(int handle,struct cl_db *child,struct cl_db *parent) {
  int err = -1;
  struct cl_db *node,child_node = {0};
  node = child;

  while((!node || node->colour == BLACK) && parent)
    {
      if( (node && node->offset == CL_DB_ROOT(handle)) || 
          (CL_DB_ROOT(handle) == -1)
          ) {
        break;
      }
      if( (node && parent->left == node->offset) || parent->left == -1) {
        struct cl_db brother_node ={0},*brother = &brother_node;
        struct cl_db brother_left_node={0},brother_right_node={0},*brother_left = &brother_left_node,*brother_right = &brother_right_node;
        if(cl_db_record_read_unfmt(handle,brother,parent->right,CLDB_CTRL_DATA) < 0 ) {
          output("Error reading brother :"CLDB_OFFSET_FMT,parent->right);
          goto out;
        }
        if(brother->colour == RED) {
          brother->colour = BLACK;
          parent->colour =  RED;
          if(cl_rbtree_rotate_left(handle,parent,brother) < 0 ) {
            output("Error in rotating the parent to the left...");
            goto out;
          }
          if(cl_db_record_read_unfmt(handle,brother,parent->right,CLDB_CTRL_DATA) < 0 ) {
            output("Error reading brother at offset :"CLDB_OFFSET_FMT,parent->right);
            goto out;
          }
        }
        if(brother->left != -1 ) {
          if(cl_db_record_read_unfmt(handle,brother_left,brother->left,CLDB_CTRL_DATA) < 0 ) {
            output("Error reading brother left at :"CLDB_OFFSET_FMT,brother->left);
            goto out;
          }
        } else {
          brother_left = NULL;
        }
        if(brother->right != -1) {
          if(cl_db_record_read_unfmt(handle,brother_right,brother->right,CLDB_CTRL_DATA) < 0 ) {
            output("Error reading brother right at :"CLDB_OFFSET_FMT,brother->right);
            goto out;
          }
        } else {
          brother_right = NULL;
        }
        if((!brother_left  || brother_left->colour ==  BLACK) && 
           (!brother_right || brother_right->colour == BLACK)
           )
          {
            brother->colour = RED;
            CL_DB_TOUCH(handle,brother,CLDB_CTRL_DATA);
            if(!node) { 
              node = &child_node;
            }
            memcpy(node,parent,CLDB_CTRL_BYTES);

            if(node->parent != -1) {
              if(cl_db_record_read_unfmt(handle,parent,node->parent,CLDB_CTRL_DATA) < 0 ) { 
                output("Error reading parent at offset :"CLDB_OFFSET_FMT,node->parent);
                goto out;
              }
            } else {
              parent = NULL;
            }
          } else {
	    struct cl_db *temp;
            if(!brother_right || brother_right->colour == BLACK) {
              /*Here,brothers left has to exist because we are going for a 
                right rotation with the brother:Andrea!! Are you listening?
              */
              brother_left->colour = BLACK;
              brother->colour = RED;
              if(cl_rbtree_rotate_right(handle,brother,brother_left) < 0 ) {
                output("Error in rotate right for brother at :"CLDB_OFFSET_FMT,brother->offset);
                goto out;
              }
	       /*After the above right rotation of the brother,
		brother_left would become the new brother,
		and brother would become brothers right
	      */
	      temp = brother;
	      brother = brother_left;
	      brother_right = temp;
	      /*Sync up the parents right offset with the new brother*/
	      parent->right = brother->offset;
            }
            brother->colour = parent->colour;
            parent->colour  = BLACK;
            brother_right->colour = BLACK;
            CL_DB_TOUCH(handle,brother_right,CLDB_CTRL_DATA);
            if(cl_rbtree_rotate_left(handle,parent,brother) < 0 ) { 
              output("Error in rotating parent to the left at :"CLDB_OFFSET_FMT,parent->offset);
              goto out;
            }
            if(CL_DB_ROOT(handle) != -1 && cl_rbtree_touch_root(handle) < 0 ) {
              output("Error touching root at :"CLDB_OFFSET_FMT,CL_DB_ROOT(handle));
              goto out;
            }
            if(node)
              node = NULL;
            break;
          }
      } else {
        /*Interchange the left and the right now*/
        struct cl_db brother_node ={0},*brother = &brother_node;
        struct cl_db brother_left_node={0},brother_right_node={0},*brother_left = &brother_left_node,*brother_right = &brother_right_node;
        if(cl_db_record_read_unfmt(handle,brother,parent->left,CLDB_CTRL_DATA) < 0 ) {
          output("Error reading brother :"CLDB_OFFSET_FMT,parent->left);
          goto out;
        }
        if(brother->colour == RED) {
          brother->colour = BLACK;
          parent->colour =  RED;
          if(cl_rbtree_rotate_right(handle,parent,brother) < 0 ) {
            output("Error in rotating the parent to the right...");
            goto out;
          }
          if(cl_db_record_read_unfmt(handle,brother,parent->left,CLDB_CTRL_DATA) < 0 ) {
            output("Error reading brother at offset :"CLDB_OFFSET_FMT,parent->left);
            goto out;
          }
        }
        if(brother->left != -1 ) {
          if(cl_db_record_read_unfmt(handle,brother_left,brother->left,CLDB_CTRL_DATA) < 0 ) {
            output("Error reading brother left at :"CLDB_OFFSET_FMT,brother->left);
            goto out;
          }
        } else {
          brother_left = NULL;
        }
        if(brother->right != -1) {
          if(cl_db_record_read_unfmt(handle,brother_right,brother->right,CLDB_CTRL_DATA) < 0 ) {
            output("Error reading brother right at :"CLDB_OFFSET_FMT,brother->right);
            goto out;
          }
        } else {
          brother_right = NULL;
        }
        if((!brother_left  || brother_left->colour ==  BLACK) && 
           (!brother_right || brother_right->colour == BLACK)
           )
          {
            brother->colour = RED;
            CL_DB_TOUCH(handle,brother,CLDB_CTRL_DATA);
            if(!node) {
              node = &child_node;
            }
            memcpy(node,parent,CLDB_CTRL_BYTES);

            if(node->parent != -1) {
              if(cl_db_record_read_unfmt(handle,parent,node->parent,CLDB_CTRL_DATA) < 0 ) { 
                output("Error reading parent at offset :"CLDB_OFFSET_FMT,node->parent);
                goto out;
              }
            } else {
              parent = NULL;
            }
          } else {
	    struct cl_db *temp;
            if(!brother_left || brother_left->colour == BLACK) {
              /*Here,brothers right has to exist because we are going for a 
                left rotation with the brother:Andrea!! Are you listening?
              */
              brother_right->colour = BLACK;
              brother->colour = RED;
              if(cl_rbtree_rotate_left(handle,brother,brother_right) < 0 ) {
                output("Error in rotate right for brother at :"CLDB_OFFSET_FMT,brother->offset);
                goto out;
              }
	      /*After the above left rotation of the brother,
		brother_right would become the new brother,
		and brother would become brothers left.
	      */
	      temp = brother;
	      brother = brother_right;
	      brother_left = temp;
	      /*Sync up the parents left offset with the new brother*/
	      parent->left = brother->offset;
            }
            brother->colour = parent->colour;
            parent->colour  = BLACK;
            brother_left->colour = BLACK;
            CL_DB_TOUCH(handle,brother_left,CLDB_CTRL_DATA);
            if(cl_rbtree_rotate_right(handle,parent,brother) < 0 ) { 
              output("Error in rotating parent to the right at :"CLDB_OFFSET_FMT,parent->offset);
              goto out;
            }
            if(CL_DB_ROOT(handle) != -1 && cl_rbtree_touch_root(handle) < 0 ) {
              output("Error touching root at :"CLDB_OFFSET_FMT,CL_DB_ROOT(handle));
              goto out;
            }
            if(node)
              node = NULL;
            break;
          }
      }
    }
  if(node) {
    node->colour = BLACK;
    CL_DB_TOUCH(handle,node,CLDB_CTRL_DATA);
  }
  err = 0;
out:
  return err;
}

/*Erase a node in a RBTREE: 
  Perform the rebalancing act if the node to be deleted is BLACK.
  If the node to be deleted is RED,then the tree is already a RBTREE
  as deletions can be performed only in a balance tree.
*/
int cl_rbtree_erase(int handle,struct cl_db *node) {
  cldb_offset_t parent,child;
  struct cl_db child_node = {0},*child_ptr = &child_node;
  struct cl_db parent_node = {0},*parent_ptr = &parent_node;
  struct cl_db temp,*ptr = &temp;
  unsigned int colour;
  int err = -1;

  if(node->left == -1)
    child = node->right;
  else if(node->right == -1)
    child = node->left;
  else {
    cldb_offset_t old = node->offset;
    struct cl_db n = {0};
    if(cl_db_record_read_unfmt(handle,ptr,node->right,CLDB_CTRL_DATA) < 0 ) {
      output("Error reading nodes right...");
      goto out;
    }
    while(ptr->left != -1) {
      if(cl_db_record_read_unfmt(handle,ptr,ptr->left,CLDB_CTRL_DATA) < 0 ) {
        output("Error reading left record...");
        goto out;
      }
    }
    child = ptr->right;
    parent = ptr->parent;
    colour = ptr->colour & 1;
    if(child != -1) {
      if(parent != old) {
        if(cl_db_record_read_unfmt(handle,child_ptr,child,CLDB_CTRL_DATA) < 0 ) {
          output("Error in reading child node...");
          goto out;
        }
        child_ptr->parent = parent;
        CL_DB_TOUCH(handle,child_ptr,CLDB_CTRL_DATA);
      }
    } else { 
      child_ptr = NULL;
    }
    if(parent != -1) {

      if(parent != old) {
        if(cl_db_record_read_unfmt(handle,parent_ptr,parent,CLDB_CTRL_DATA) < 0 ) {
          output("Error reading parent node :"CLDB_OFFSET_FMT,parent);
          goto out;
        }
        if(ptr->offset == parent_ptr->left) 
          parent_ptr->left = child;
        else
          parent_ptr->right = child;

        CL_DB_TOUCH(handle,parent_ptr,CLDB_CTRL_DATA);
      } 
    } else {
      parent_ptr = NULL;
      CL_DB_ROOT(handle) = child;
    }

    ptr->colour = node->colour;
    ptr->left =   node->left;
    if(ptr->parent == old) {
      ptr->right = child;
    } else {
      ptr->right =  node->right;
    }

    if( (ptr->parent = node->parent) != -1 ) { 
      struct cl_db p;
      if( cl_db_record_read_unfmt(handle,&p,node->parent,CLDB_CTRL_DATA) < 0 ) {
        output("Error reading node parent: "CLDB_OFFSET_FMT,node->parent);
        goto out;
      }
      if(node->offset == p.left) {
        p.left = ptr->offset;
      }	else {
        p.right = ptr->offset;
      }
      CL_DB_TOUCH(handle,&p,CLDB_CTRL_DATA);
    } else {
      CL_DB_ROOT(handle) = ptr->offset;
    }

    CL_DB_TOUCH(handle,ptr,CLDB_CTRL_DATA);
    if(cl_db_record_read_unfmt(handle,&n,ptr->left,CLDB_CTRL_DATA) < 0) {
      output("Error reading node left: "CLDB_OFFSET_FMT,node->left);
      goto out;
    }

    n.parent = ptr->offset;
    CL_DB_TOUCH(handle,&n,CLDB_CTRL_DATA);
    if(ptr->right != -1) {
      if(ptr->right != parent) {
        if(cl_db_record_read_unfmt(handle,&n,ptr->right,CLDB_CTRL_DATA) < 0) {
          output("Error reading node right :"CLDB_OFFSET_FMT,ptr->right);
          goto out;
        }
      } else {
        memcpy(&n,parent_ptr,CLDB_CTRL_BYTES);
        parent_ptr->parent = ptr->offset;
      }
      n.parent = ptr->offset;
      if(n.offset == child) {
        memcpy(child_ptr,&n,CLDB_CTRL_BYTES);
      }
      CL_DB_TOUCH(handle,&n,CLDB_CTRL_DATA);
    }
    if(parent_ptr && old == parent) {
      memcpy(parent_ptr,ptr,CLDB_CTRL_BYTES);
    }
    goto rebalance;
  }
  parent = node->parent;
  colour = node->colour & 1;
  if(child != -1) { 
    if(cl_db_record_read_unfmt(handle,child_ptr,child,CLDB_CTRL_DATA) < 0 ) {
      output("Error reading child offset :"CLDB_OFFSET_FMT,child);
      goto out;
    }
    child_ptr->parent = parent;
    CL_DB_TOUCH(handle,child_ptr,CLDB_CTRL_DATA);
  } else {
    child_ptr = NULL;
  }
  if(parent != -1) {
    if(cl_db_record_read_unfmt(handle,parent_ptr,parent,CLDB_CTRL_DATA)<0) {
      output("error reading parent offset :"CLDB_OFFSET_FMT,parent);
      goto out;
    }
    if(parent_ptr->left == node->offset) 
      parent_ptr->left= child;
    else
      parent_ptr->right = child;
    CL_DB_TOUCH(handle,parent_ptr,CLDB_CTRL_DATA);
  } else{ 
    CL_DB_ROOT(handle) = child;
    parent_ptr = NULL;
  }
rebalance:
  err = 0;
  if(colour == BLACK) 
    err = cl_rbtree_erase_colour(handle,child_ptr,parent_ptr);
out:
  return err;
}

