#ifndef _DLIST_H
#define _DLIST_H

struct list_head {
  struct list_head *next,*prev;
};

#define LIST_EMPTY(q) ((q)->next == (q))

#define DECLARE_LIST_HEAD(head) \
struct list_head head = { .next = &(head), .prev = &(head) }

#define INIT_LIST_HEAD(head) do { \
 (head)->next = (head)->prev = (head);\
}while(0)

static __inline__ void init_list_head(struct list_head *list) { 
  list->next = list;
  list->prev = list;
}

#define __list_add(e,n,p) do { \
  (e)->next = n;\
  (e)->prev = p;\
  (p)->next = e;\
  (n)->prev = e;\
}while(0)

#define __list_del(n,p) do { \
  (n)->prev = p;\
  (p)->next = n;\
}while(0)

/*add in a LIFO way*/
static __inline__ void list_add(struct list_head *e,struct list_head *l) { 
  struct list_head *n = l->next;
  __list_add(e,n,l);
}

static __inline__ void list_add_tail(struct list_head *e,struct list_head *l) {
  struct list_head *p = l->prev;
  __list_add(e,l,p);
}

static __inline__ void list_del(struct list_head *e) { 
  struct list_head *n = e->next;
  struct list_head *p = e->prev;
  __list_del(n,p);
}

static __inline__ void list_del_init(struct list_head *e) { 
  list_del(e);
  init_list_head(e);
}

#define list_entry(e,c,f)  \
(c*) ( (unsigned char *)e - (unsigned long)&(((c*)0)->f) )

#define list_for_each(e,l) \
 for(e=(l)->next; e != (l); e = (e)->next)

#define list_for_each_safe(e,t,l)  \
 for( e = (l)->next, t = e->next;e != (l) ; e = t, t = e->next)

#endif


