#ifndef YCSB_C_DB_SYNC_ASYNC_ADAPTERH_
#define YCSB_C_DB_SYNC_ASYNC_ADAPTERH_
#ifdef SPLINTER_TEST
extern "C"{
#include "splinterdb/splinterdb.h"
#include "splinterdb/default_data_config.h"
}
#endif
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

class DBSyncAsyncAdapter : public AsyncDBInterface{
 public:
  DBSyncAsyncAdapter(DB *db, 
                     Measurements *measurements, 
                     utils::Properties *props,
                     ycsbc::CoreWorkload *wl,
                     utils::CountDownLatch *latch) : AsyncDBInterface(db, measurements, props, wl, latch) {}
  ~DBSyncAsyncAdapter() {};
  void Init() {
    db_->Init();
    const int num_threads = std::stoi(AsyncDBInterface::props_->GetProperty("threadcount", "1"));
    for (int i = 0; i < num_threads; ++i) {
      worker_threads_.emplace_back(std::async(std::launch::async, &DBSyncAsyncAdapter::WorkerThread, this));
    }
  }
  void Cleanup() {
    db_->Cleanup();
  }
  void RefreshThread() override
  {
    const int num_threads = std::stoi(AsyncDBInterface::props_->GetProperty("threadcount", "1"));
    for (int i = 0; i < num_threads; ++i) {
      worker_threads_.emplace_back(std::async(std::launch::async, &DBSyncAsyncAdapter::WorkerThread, this));
    }
  }

  inline void WorkerThread() {
    #ifdef SPLINTER_TEST
      platform_register_thread();
    #endif
    try {
      task t;
      while(true) {
        task_queue_.dequeue(t);
        wait_timer_ = t.wait_timer;
        switch (t.type) {
          case task::LOAD:
            wl_->DoInsert(*this);
            break;
          case task::TRANSACTION:
            wl_->DoTransaction(*this);
            break;
          case task::END:
            latch_->CountDown();
            return;
          case task::QUIT:
            #ifdef SPLINTER_TEST
              platform_deregister_thread();
            #endif
            latch_->CountDown();
            return;
        }
      }
    } catch (const utils::Exception &e) {
      std::cerr << "Caught exception: " << e.what() << std::endl;
      exit(1);
    }
  }

  void AddTask(const task& t) override {
    task_queue_.enqueue(t);
  }

  

 private:
  folly::UnboundedQueue<task, false, false, false> task_queue_;
  
};

} // ycsbc
#endif // USE_ASYNC_TEST
#endif // YCSB_C_DB_SYNC_ASYNC_ADAPTERH_
