#ifndef YCSB_C_DB_SYNC_ASYNC_ADAPTERH_
#define YCSB_C_DB_SYNC_ASYNC_ADAPTERH_

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

class DBSyncAsyncAdapter : public DB, public AsyncDBInterface{
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

  inline void WorkerThread() {
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
            break;
          case task::QUIT:
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

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    uint64_t elapsed = wait_timer_.End();
    measurements_->Report(READ_WAIT, elapsed);
    timer_.Start();
    Status s = db_->Read(table, key, fields, result);
    elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(READ, elapsed);
    } else {
      measurements_->Report(READ_FAILED, elapsed);
    }
    return s;
  }
  Status Scan(const std::string &table, const std::string &key, int record_count,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    uint64_t elapsed = wait_timer_.End();
    measurements_->Report(SCAN_WAIT, elapsed);
    timer_.Start();
    Status s = db_->Scan(table, key, record_count, fields, result);
    elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(SCAN, elapsed);
    } else {
      measurements_->Report(SCAN_FAILED, elapsed);
    }
    return s;
  }
  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    uint64_t elapsed = wait_timer_.End();
    measurements_->Report(UPDATE_WAIT, elapsed);
    timer_.Start();
    Status s = db_->Update(table, key, values);
    elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(UPDATE, elapsed);
    } else {
      measurements_->Report(UPDATE_FAILED, elapsed);
    }
    return s;
  }
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    uint64_t elapsed = wait_timer_.End();
    measurements_->Report(INSERT_WAIT, elapsed);
    timer_.Start();
    Status s = db_->Insert(table, key, values);
    elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(INSERT, elapsed);
    } else {
      measurements_->Report(INSERT_FAILED, elapsed);
    }
    return s;
  }
  Status Delete(const std::string &table, const std::string &key) {
    uint64_t elapsed = wait_timer_.End();
    measurements_->Report(DELETE_WAIT, elapsed);
    timer_.Start();
    Status s = db_->Delete(table, key);
    elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(DELETE, elapsed);
    } else {
      measurements_->Report(DELETE_FAILED, elapsed);
    }
    return s;
  }

 private:
  inline static thread_local utils::Timer<uint64_t, std::nano> wait_timer_;
  inline static thread_local utils::Timer<uint64_t, std::nano> timer_;
  folly::UnboundedQueue<task, false, false, false> task_queue_;
  std::vector<std::future<void>> worker_threads_;
};

} // ycsbc
#endif // USE_ASYNC_TEST
#endif // YCSB_C_DB_SYNC_ASYNC_ADAPTERH_
