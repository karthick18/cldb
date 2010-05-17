/*Karthick:A generic queue*/

#ifndef _QUEUE_H
#define _QUEUE_H

struct queue_struct {
  struct queue_struct *next;
  struct queue_struct **pprev;
};

#define QUEUE_INIT(queue)  ( (queue)->head = NULL,(queue)->tail = &(queue)->head )
#define QUEUE_EMPTY(queue) ( (queue)->head == NULL )

#define QUEUE_ADD(element,queue) do { \
 if( (queue) && (element) ) { \
   if(((element)->next = (queue)->head)) {\
        (element)->next->pprev = &(element)->next;\
  } else (queue)->tail = &(element)->next;\
  *((element)->pprev = &(queue)->head) = (element);\
}\
}while(0)

#define QUEUE_ADD_TAIL(element,queue) do { \
 if((queue) && (element)) { \
   (element)->next = NULL;\
   *((element)->pprev = (queue)->tail) = (element);\
    (queue)->tail = &(element)->next;\
 }\
}while(0)

#define QUEUE_DEL(element,queue) do { \
  if((queue) && (element) && (element)->pprev) { \
      if((element)->next) {\
         (element)->next->pprev = (element)->pprev;\
      } else (queue)->tail = (element)->pprev;\
     *((element)->pprev) = (element)->next;\
     (element)->pprev = NULL,(element)->next = NULL;\
 }\
}while(0)

#define queue_entry(queue,cast,field) \
(cast*) ( (unsigned char *)(queue) - (unsigned long)&((cast*)0)->field)

#define queue_for_each(element,queue) \
    for(element = (queue)->head ; element; element = element->next)

#define queue_for_each_safe(element,temp,queue) \
  for(element = (queue)->head ,temp = element?element->next:NULL; element;element=temp,temp=element?element->next:NULL)

#endif
