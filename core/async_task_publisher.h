#ifndef YCSB_C_ASYNC_TASK_PUBLISHER_H_
#define YCSB_C_ASYNC_TASK_PUBLISHER_H_

#include "async_db_interface.h"
#include "utils/rate_limit.h"

namespace ycsbc {

inline int AsyncTaskPublisher(AsyncDBInterface *async_db, 
                              const int num_ops, 
                              const int num_threads, 
                              bool is_loading, 
                              bool init_db, 
                              bool cleanup_db,
                              utils::CountDownLatch *latch, 
                              utils::RateLimiter *rlim) {
  if (init_db) {
    async_db->Init();
  }
  task t;
  if (is_loading) {
    t.type = task::LOAD;
  } else {
    t.type = task::TRANSACTION;
  }
  int ops = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (rlim) {
      rlim->Consume(1);
    }
    t.wait_timer.Start();
    async_db->AddTask(t);
    ops++;
  }
  
  if (cleanup_db) {
    t.type = task::QUIT;
  } else {
    t.type = task::END;
  }
  for (int i = 0; i < num_threads; ++i) {
    async_db->AddTask(t);
  }
  if (cleanup_db) {
    latch->Await();
    async_db->Cleanup();
  }
  return ops;
}
}
#endif // YCSB_C_ASYNC_TASK_PUBLISHER_H_