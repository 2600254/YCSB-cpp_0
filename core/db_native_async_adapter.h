#ifndef YCSB_C_DB_NATIVE_ASYNC_ADAPTER_H_
#define YCSB_C_DB_NATIVE_ASYNC_ADAPTER_H_

#ifdef USE_ASYNC_TEST
#include <iostream>
#include <future>
#include <string>
#include <thread>
#include <vector>
#include <folly/concurrency/UnboundedQueue.h>

#include "db.h"
#include "async_db_interface.h"

namespace ycsbc {

class DBNativeASyncAdapter : public AsyncDBInterface{
 public:
  DBNativeASyncAdapter(DB *db, 
                       Measurements *measurements, 
                       utils::Properties *props,
                       ycsbc::CoreWorkload *wl,
                       utils::CountDownLatch *latch) : AsyncDBInterface(db, measurements, props, wl, latch) {}
  ~DBNativeASyncAdapter() = default;
  void Init() {
    db_->Init();
  }
  void Cleanup() {
    db_->Cleanup();
  }

  void AddTask(const task& t) override {
  }

 private:
};

} // ycsbc
#endif // USE_ASYNC_TEST
#endif // YCSB_C_DB_NATIVE_ASYNC_ADAPTER_H_
