#ifndef YCSB_C_ASYNC_DB_INTERFACE_H_
#define YCSB_C_ASYNC_DB_INTERFACE_H_

#include "core_workload.h"
#include "db.h"
#include "measurements.h"
#include "utils/countdown_latch.h"
#include "utils/timer.h"
#include "utils/utils.h"

namespace ycsbc {
struct task {
  utils::Timer<uint64_t, std::nano> wait_timer;
  enum task_type {
    LOAD,
    TRANSACTION,
    END,
    QUIT
  }type;
};

class AsyncDBInterface{
 public:
  AsyncDBInterface(DB *db, Measurements *measurements, 
                   utils::Properties *props,
                   ycsbc::CoreWorkload *wl, 
                   utils::CountDownLatch *latch) : db_(db), 
                                                   measurements_(measurements),
                                                   props_(props),
                                                   wl_(wl),
                                                   latch_(latch){}
  virtual ~AsyncDBInterface() {
    delete db_;
  }
  virtual void Init() = 0;
  virtual void Cleanup() = 0;
  virtual void AddTask(const task& t) = 0;
 protected:
  DB *db_;
  Measurements *measurements_;
  utils::Properties *props_;
  ycsbc::CoreWorkload *wl_;
  utils::CountDownLatch *latch_;
};

}

#endif // YCSB_C_ASYNC_DB_INTERFACE_H_