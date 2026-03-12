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

class AsyncDBInterface : public DB{
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

 protected:
  DB *db_;
  Measurements *measurements_;
  utils::Properties *props_;
  ycsbc::CoreWorkload *wl_;
  utils::CountDownLatch *latch_;
  inline static thread_local utils::Timer<uint64_t, std::nano> wait_timer_;
  inline static thread_local utils::Timer<uint64_t, std::nano> timer_;
};

}

#endif // YCSB_C_ASYNC_DB_INTERFACE_H_