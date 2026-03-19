#ifndef YCSB_C_F2_ASYNC_ADAPTER_H_
#define YCSB_C_F2_ASYNC_ADAPTER_H_

#include "core/async_db_interface.h"
#include "f2_db.h"
#include <folly/concurrency/UnboundedQueue.h>
#include <future>
#include <memory>
#include <vector>
#include <thread>
#include "unistd.h"
namespace ycsbc {

class F2AsyncAdapter : public AsyncDBInterface {
 public:
  F2AsyncAdapter(DB *db, Measurements *measurements, utils::Properties *props,
                 ycsbc::CoreWorkload *wl, utils::CountDownLatch *latch);
  ~F2AsyncAdapter() = default;

  void Init() override;
  void Cleanup() override;
  void AddTask(const task& t) override;

  DB::Status Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields, std::vector<Field> &result) override;

  DB::Status Scan(const std::string &table, const std::string &key, int record_count,
                  const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) override;

  DB::Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) override;
  DB::Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) override;
  DB::Status Delete(const std::string &table, const std::string &key) override;
  void DeserializeRow(const std::string &data, std::vector<Field> &values);
  void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,const std::vector<std::string> &fields);
  Measurements* GetMeasurements() { return measurements_; }

  friend void ReadCallback(FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result_status);
  friend void UpsertCallback(FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result);
  friend void DeleteCallback(FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result);

 private:
  void WorkerThread();
  void RefreshThread() override
  {
    const int num_threads = std::stoi(AsyncDBInterface::props_->GetProperty("threadcount", "1"));
    for (int i = 0; i < num_threads; ++i) {
      worker_threads_.emplace_back(std::async(std::launch::async, &F2AsyncAdapter::WorkerThread, this));
    }
  }
  void SerializeRow(const std::vector<Field> &values, std::string &data);
  DB::Status WaitFuture(std::future<DB::Status>& future);
  Status MapStatus(FASTER::core::Status s);

  std::future<DB::Status> Read_Sync(const std::string &table, const std::string &key,
                                    const std::vector<std::string> *fields, std::vector<Field> &result,std::shared_ptr<utils::Timer<uint64_t, std::nano>> op_timer);

  F2Db *f2_db_;
  folly::UnboundedQueue<task, false, false, false> task_queue_;
  std::vector<std::future<void>> worker_threads_;
  utils::Timer<uint64_t, std::nano> wait_timer_;
  static thread_local int completepending_flag;
};

} // namespace ycsbc

#endif // YCSB_C_F2_ASYNC_ADAPTER_H_