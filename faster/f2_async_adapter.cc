#include "f2_async_adapter.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <future>
#include "utils/utils.h"


namespace {
  const std::string PROP_NAME = "f2.dbname";
  const std::string PROP_NAME_DEFAULT = "/home/liyixian/FASTER_test";
  const std::string PROP_HOT_LOG_PATH = "f2.hot_log_path";
  const std::string PROP_HOT_LOG_PATH_DEFAULT = "hot_log";
  const std::string PROP_COLD_LOG_PATH = "f2.cold_log_path";
  const std::string PROP_COLD_LOG_PATH_DEFAULT = "cold_log";
  const std::string PROP_HOT_LOG_MEM = "f2.hot_log_mem";
  const std::string PROP_HOT_LOG_MEM_DEFAULT = "2147483648"; // 2GB
  const std::string PROP_COLD_LOG_MEM = "f2.cold_log_mem";
  const std::string PROP_COLD_LOG_MEM_DEFAULT = "2147483648"; // 2GB
  const std::string PROP_HOT_INDEX_SIZE = "f2.hot_index_size";
  const std::string PROP_HOT_INDEX_SIZE_DEFAULT = "134217728"; // 128MB
  const std::string PROP_COLD_INDEX_SIZE = "f2.cold_index_size";
  const std::string PROP_COLD_INDEX_SIZE_DEFAULT = "4194304"; // 4MB
  const std::string PROP_HOT_STORE_BUDGET = "f2.hot_store_budget";
  const std::string PROP_HOT_STORE_BUDGET_DEFAULT = "8589934592"; // 8GB
  const std::string PROP_COLD_STORE_BUDGET = "f2.cold_store_budget";
  const std::string PROP_COLD_STORE_BUDGET_DEFAULT = "8589934592"; // 8GB
  const std::string PROP_HOT_COMPACTION_THREADS = "f2.hot_compaction_threads";
  const std::string PROP_HOT_COMPACTION_THREADS_DEFAULT = "8";
  const std::string PROP_COLD_COMPACTION_THREADS = "f2.cold_compaction_threads";
  const std::string PROP_COLD_COMPACTION_THREADS_DEFAULT = "8";
  const std::string PROP_COLD_INDEX_MEM_SIZE = "f2.cold_index_mem_size";
  const std::string PROP_COLD_INDEX_MEM_SIZE_DEFAULT = "268435456";
  const std::string PROP_COLD_INDEX_MUTABLE_FRACTION = "f2.cold_index_mutable_fraction";
  const std::string PROP_COLD_INDEX_MUTABLE_FRACTION_DEFAULT = "0.6";
  const std::string PROP_COLD_STORE_MUTABLE_FRACTION = "f2.cold_store_mutable_fraction";
  const std::string PROP_COLD_STORE_MUTABLE_FRACTION_DEFAULT = "0";
  const std::string PROP_COMPACTION_THRESHOLD = "f2.compaction_threshold";
  const std::string PROP_COMPACTION_THRESHOLD_DEFAULT = "0.3";
  const std::string PROP_READ_CACHE_ENABLED = "f2.read_cache_enabled";
  const std::string PROP_READ_CACHE_ENABLED_DEFAULT = "false";
  const std::string PROP_F2_FIELD_COUNT = "f2.fieldcount";
  const std::string PROP_F2_FIELD_COUNT_DEFAULT = "0";
} // anonymous
namespace ycsbc {

using namespace FASTER::core;
std::unique_ptr<F2Db::F2KvType> F2Db::store_ = nullptr;
std::mutex F2Db::mu_; 
std::atomic<int> F2Db::ref_cnt_{0}; 
thread_local int F2AsyncAdapter::completepending_flag=0;
void F2Db::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  ref_cnt_++;
  if (store_) {
    return;
  }

  
  const std::string &db_path = props_->GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path.empty()) {
    throw utils::Exception("F2 db path is missing");
  }
  const std::string hot_log_path = props_->GetProperty(PROP_HOT_LOG_PATH, PROP_HOT_LOG_PATH_DEFAULT);
  const std::string cold_log_path = props_->GetProperty(PROP_COLD_LOG_PATH, PROP_COLD_LOG_PATH_DEFAULT);

  
  uint64_t hot_log_mem = std::stoull(props_->GetProperty(PROP_HOT_LOG_MEM, PROP_HOT_LOG_MEM_DEFAULT));
  uint64_t cold_log_mem = std::stoull(props_->GetProperty(PROP_COLD_LOG_MEM, PROP_COLD_LOG_MEM_DEFAULT));
  size_t hot_index_size = static_cast<size_t>(std::stoull(props_->GetProperty(PROP_HOT_INDEX_SIZE, PROP_HOT_INDEX_SIZE_DEFAULT)));
  size_t cold_index_size = static_cast<size_t>(std::stoull(props_->GetProperty(PROP_COLD_INDEX_SIZE, PROP_COLD_INDEX_SIZE_DEFAULT)));

  uint64_t hot_store_budget = std::stoull(props_->GetProperty(PROP_HOT_STORE_BUDGET, PROP_HOT_STORE_BUDGET_DEFAULT));
  uint64_t cold_store_budget = std::stoull(props_->GetProperty(PROP_COLD_STORE_BUDGET, PROP_COLD_STORE_BUDGET_DEFAULT));
  uint32_t hot_compaction_threads = std::stoul(props_->GetProperty(PROP_HOT_COMPACTION_THREADS, PROP_HOT_COMPACTION_THREADS_DEFAULT));
  uint32_t cold_compaction_threads = std::stoul(props_->GetProperty(PROP_COLD_COMPACTION_THREADS, PROP_COLD_COMPACTION_THREADS_DEFAULT));
  double compaction_threshold = std::stod(props_->GetProperty(PROP_COMPACTION_THRESHOLD, PROP_COMPACTION_THRESHOLD_DEFAULT));

  size_t cold_index_mem_size = static_cast<size_t>(std::stoull(props_->GetProperty(PROP_COLD_INDEX_MEM_SIZE, PROP_COLD_INDEX_MEM_SIZE_DEFAULT)));
  double cold_index_mutable_fraction = std::stod(props_->GetProperty(PROP_COLD_INDEX_MUTABLE_FRACTION, PROP_COLD_INDEX_MUTABLE_FRACTION_DEFAULT));
  
  double cold_store_mutable_fraction = std::stod(props_->GetProperty(PROP_COLD_STORE_MUTABLE_FRACTION, PROP_COLD_STORE_MUTABLE_FRACTION_DEFAULT));
  bool read_cache_enabled = (props_->GetProperty(PROP_READ_CACHE_ENABLED, PROP_READ_CACHE_ENABLED_DEFAULT) == "true");
  F2CompactionConfig compaction_config = DEFAULT_F2_COMPACTION_CONFIG;
  compaction_config.hot_store.hlog_size_budget = hot_store_budget;
  compaction_config.cold_store.hlog_size_budget = cold_store_budget;
  compaction_config.hot_store.num_threads = hot_compaction_threads; 
  compaction_config.cold_store.num_threads = cold_compaction_threads; 
  ReadCacheConfig rc_config{ .enabled = read_cache_enabled };
  F2KvType::HotIndexConfig hot_index_config{ hot_index_size };
  F2KvType::ColdIndexConfig cold_index_config{ 
    cold_index_size, 
    cold_index_mem_size,
    cold_index_mutable_fraction
  };
  store_ = std::make_unique<F2KvType>(
      hot_index_config, hot_log_mem, db_path + "/" + hot_log_path,
      cold_index_config, cold_log_mem, db_path + "/" + cold_log_path,
      compaction_threshold, cold_store_mutable_fraction, rc_config, compaction_config);
}

void F2Db::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_ > 0) {
    return;
  }
  store_.reset();
}
DB *NewF2Db() {
  return new F2Db;
}

const bool registered = DBFactory::RegisterDB("f2", NewF2Db);

} // namespace ycsbc




namespace ycsbc {
struct ReadContext : public FASTER::core::IAsyncContext {
  typedef F2Key key_t;
  typedef F2Value value_t;

  F2Key key_;  
  std::string output;

  F2AsyncAdapter* adapter;
  std::shared_ptr<std::promise<DB::Status>> promise;
  std::shared_ptr<utils::Timer<uint64_t, std::nano>> timer;
  std::shared_ptr<std::vector<std::string>> fields;
  std::vector<DB::Field>* result;

  ReadContext(const std::string &k, F2AsyncAdapter* a, std::shared_ptr<std::promise<DB::Status>> p, 
              std::shared_ptr<utils::Timer<uint64_t, std::nano>> t, std::shared_ptr<std::vector<std::string>> f, std::vector<DB::Field>* r)
      : key_(k),output(), adapter(a), promise(p), timer(t), fields(f), result(r) {
        
      }

  ReadContext(ReadContext& other)=default;

  inline const F2Key& key() const {
    return key_;}

  inline void Get(const value_t& val) {
    output.clear();
    output.resize(val.len);
    memcpy(&output[0], val.data, val.len);
  }
  inline void GetAtomic(const value_t& val) {
    Get(val);
  }
  
  FASTER::core::Status DeepCopy_Internal(FASTER::core::IAsyncContext*& context_copy) final {
    return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }
};
struct UpsertContext : public FASTER::core::IAsyncContext {
  typedef F2Key key_t;
  typedef F2Value value_t;

  F2Key key_;
  std::string value;

  F2AsyncAdapter* adapter;
  std::shared_ptr<std::promise<DB::Status>> promise;
  std::shared_ptr<utils::Timer<uint64_t, std::nano>> timer;
  bool is_insert;

  UpsertContext(const std::string &k, const std::string &v, F2AsyncAdapter* a, 
                std::shared_ptr<std::promise<DB::Status>> p, std::shared_ptr<utils::Timer<uint64_t, std::nano>> t, bool insert)
      : key_(k), value(v), adapter(a), promise(p), timer(t), is_insert(insert) {}

  UpsertContext(const UpsertContext& other) = default;

  inline const F2Key& key() const { 
    return key_;}
  inline uint32_t value_size() const { return static_cast<uint32_t>(sizeof(value_t)); }

  inline void Put(value_t& val) {
    val.len = static_cast<uint32_t>(value.size());
    if (!value.empty()) {
      std::memcpy(val.data, value.data(), std::min((size_t)value.size(), kF2MaxValueSize));
    }
  }

  inline bool PutAtomic(value_t& val) {
    Put(val);
    return true;
  }

  FASTER::core::Status DeepCopy_Internal(FASTER::core::IAsyncContext*& context_copy) final {
    return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }
};

struct DeleteContext : public FASTER::core::IAsyncContext {
  typedef F2Key key_t;
  typedef F2Value value_t;

  F2Key key_;

  F2AsyncAdapter* adapter;
  std::shared_ptr<std::promise<DB::Status>> promise;
  std::shared_ptr<utils::Timer<uint64_t, std::nano>> timer;

  DeleteContext(const std::string &k, F2AsyncAdapter* a, std::shared_ptr<std::promise<DB::Status>> p, std::shared_ptr<utils::Timer<uint64_t, std::nano>> t)
      : key_(k), adapter(a), promise(p), timer(t) {}

  DeleteContext(const DeleteContext& other) = default;

  inline const F2Key& key() const {
    return key_;}
  inline uint32_t value_size() const { return 0; }

  FASTER::core::Status DeepCopy_Internal(FASTER::core::IAsyncContext*& context_copy) final {
    return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }
};

void ReadCallback(FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result_status) {

  auto* ctx = static_cast<ReadContext*>(ctxt);
  uint64_t elapsed = ctx->timer->End();
  std::string data=ctx->output;
  std::vector<DB::Field> values;
  if (result_status == FASTER::core::Status::Ok) {
    ctx->adapter->measurements_->Report(READ, elapsed);
    if (ctx->fields != nullptr) {
      ctx->adapter->DeserializeRowFilter(*(ctx->result), ctx->output, *(ctx->fields));
    } else {
      values.clear();
      const char *p = data.data();
      const char *lim = p + data.size();
      while (p < lim) {
        uint32_t len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field_name(p, static_cast<size_t>(len));
        p += len;
        len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field_value(p, static_cast<size_t>(len));
        p += len;
        values.push_back({field_name, field_value});
      }
    }
    ctx->promise->set_value(DB::kOK);
  } else if (result_status == FASTER::core::Status::NotFound) {
    ctx->adapter->measurements_->Report(READ_FAILED, elapsed);
    ctx->promise->set_value(DB::kNotFound);
  } else {
    ctx->adapter->measurements_->Report(READ_FAILED, elapsed);
    ctx->promise->set_value(DB::kError);
  }
}
void ReadCallbackSync(FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result_status) {
  
  auto* ctx = static_cast<ReadContext*>(ctxt);
  std::string data=ctx->output;
  auto values=*ctx->result;
  if (result_status == FASTER::core::Status::Ok) {
    if (ctx->fields != nullptr) {
      ctx->adapter->DeserializeRowFilter(*(ctx->result), ctx->output, *(ctx->fields));
    } else {
      values.clear();
      const char *p = data.data();
      const char *lim = p + data.size();
      while (p < lim) {
        uint32_t len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field_name(p, static_cast<size_t>(len));
        p += len;
        len = *reinterpret_cast<const uint32_t *>(p);
        p += sizeof(uint32_t);
        std::string field_value(p, static_cast<size_t>(len));
        p += len;
        values.push_back({field_name, field_value});
      }
    }
    ctx->promise->set_value(DB::kOK);
  } else if (result_status == FASTER::core::Status::NotFound) {
    ctx->promise->set_value(DB::kNotFound);
  } else {
    ctx->promise->set_value(DB::kError);
  }
}
void UpsertCallback(FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
  
  auto* ctx = static_cast<UpsertContext*>(ctxt);
  uint64_t elapsed = ctx->timer->End();
  if (result == FASTER::core::Status::Ok) {
    ctx->adapter->measurements_->Report(ctx->is_insert ? INSERT : UPDATE, elapsed);
    ctx->promise->set_value(DB::kOK);
  } else {
    ctx->adapter->measurements_->Report(ctx->is_insert ? INSERT_FAILED : UPDATE_FAILED, elapsed);
    ctx->promise->set_value(DB::kError);
  }
}

void DeleteCallback(FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
  auto* ctx = static_cast<DeleteContext*>(ctxt);
  uint64_t elapsed = ctx->timer->End();
  if (result == FASTER::core::Status::Ok) {
    ctx->adapter->measurements_->Report(DELETE, elapsed);
    ctx->promise->set_value(DB::kOK);
  } else {
    ctx->adapter->measurements_->Report(DELETE_FAILED, elapsed);
    ctx->promise->set_value(DB::kError);
  }
}



F2AsyncAdapter::F2AsyncAdapter(DB *db, Measurements *measurements, 
                               utils::Properties *props,
                               ycsbc::CoreWorkload *wl, 
                               utils::CountDownLatch *latch) 
    : AsyncDBInterface(db, measurements, props, wl, latch), f2_db_(static_cast<F2Db*>(db)) {}

void F2AsyncAdapter::Init() {
  f2_db_->Init();
  const int num_threads = std::stoi(AsyncDBInterface::props_->GetProperty("threadcount", "1"));
  for (int i = 0; i < num_threads; ++i) {
    worker_threads_.emplace_back(std::async(std::launch::async, &F2AsyncAdapter::WorkerThread, this));
  }
}

void F2AsyncAdapter::Cleanup() {
  f2_db_->Cleanup();
}

void F2AsyncAdapter::AddTask(const task& t) {
  task_queue_.enqueue(t);
}

void F2AsyncAdapter::WorkerThread() {
  task t;
  f2_db_->store_->StartSession();
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
        while (f2_db_->store_->CompletePending(true) != true);
        f2_db_->store_->StopSession();
        latch_->CountDown();
        return;
      case task::QUIT:
        while (f2_db_->store_->CompletePending(true) != true);
        f2_db_->store_->StopSession();
        latch_->CountDown();
        return; 
    }
  }
}

DB::Status F2AsyncAdapter::Read(const std::string &table, const std::string &key,
                            const std::vector<std::string> *fields, std::vector<Field> &result) {
  uint64_t elapsed = wait_timer_.End();
  measurements_->Report(READ_WAIT, elapsed);
  auto fields_ptr = fields ? std::make_shared<std::vector<std::string>>(*fields) : nullptr;
  auto promise = std::make_shared<std::promise<DB::Status>>();
  auto op_timer = std::make_shared<utils::Timer<uint64_t, std::nano>>();
  op_timer->Start();
  
  
  ReadContext ctx(key,this, promise, op_timer,fields_ptr,&result);
  auto result_status=f2_db_->store_->Read(ctx, ReadCallback, 1);
  if (result_status == FASTER::core::Status::Ok) {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(READ, elapsed);
    if (ctx.fields != nullptr) {
      ctx.adapter->DeserializeRowFilter(*(ctx.result), ctx.output, *(ctx.fields));
    } else {
      ctx.adapter->DeserializeRow(ctx.output, *(ctx.result));
    }
    ctx.promise->set_value(DB::kOK);
  } else if (result_status == FASTER::core::Status::NotFound) {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(READ_FAILED, elapsed);
    ctx.promise->set_value(DB::kNotFound);
  } else if(result_status==FASTER::core::Status::Pending){
    
  }else {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(READ_FAILED, elapsed);
    ctx.promise->set_value(DB::kError);
  }
  return DB::Status::kOK;
}

std::future<DB::Status> F2AsyncAdapter::Read_Sync(const std::string &table, const std::string &key,
                            const std::vector<std::string> *fields, std::vector<Field> &result,std::shared_ptr<utils::Timer<uint64_t, std::nano>> op_timer) {
  
  auto promise = std::make_shared<std::promise<DB::Status>>();
  auto future = promise->get_future();
  auto fields_ptr = fields ? std::make_shared<std::vector<std::string>>(*fields) : nullptr;
  ReadContext ctx(key,this, promise, op_timer,fields_ptr,&result);
  auto result_status=f2_db_->store_->Read(ctx, ReadCallbackSync, 1);
  if (result_status == FASTER::core::Status::Ok) {
    if (ctx.fields != nullptr) {
      ctx.adapter->DeserializeRowFilter(*(ctx.result), ctx.output, *(ctx.fields));
    } else {
      ctx.adapter->DeserializeRow(ctx.output, *(ctx.result));
    }
    ctx.promise->set_value(DB::kOK);
  } else if (result_status == FASTER::core::Status::NotFound) {
    ctx.promise->set_value(DB::kNotFound);
  } else if(result_status==FASTER::core::Status::Pending){
    
  }else {
    ctx.promise->set_value(DB::kError);
  }
  return future;
}

DB::Status F2AsyncAdapter::Scan(const std::string &table, const std::string &key, int record_count,
                            const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
  return kNotImplemented;
}

DB::Status F2AsyncAdapter::Update(const std::string &table, const std::string &key,
                              std::vector<Field> &values) {
  uint64_t elapsed = wait_timer_.End();
  measurements_->Report(UPDATE_WAIT, elapsed);
  
  auto promise = std::make_shared<std::promise<DB::Status>>();
  auto op_timer = std::make_shared<utils::Timer<uint64_t, std::nano>>();
  op_timer->Start();
  
  std::vector<Field> current_values;
  auto future2 = Read_Sync(table, key, nullptr, current_values,op_timer);


  WaitFuture(future2);
  
  for (Field &new_field : values) {
    bool found = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        cur_field.value = new_field.value;
        found = true;
        break;
      }
    }
    if (!found) current_values.push_back(new_field);
  }
  
  std::string data;
  SerializeRow(current_values, data);
  UpsertContext ctx(key, data, this, promise, op_timer, false); 
  
  auto result_=f2_db_->store_->Upsert(ctx, UpsertCallback, 1);
  if (result_ == FASTER::core::Status::Ok) {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(UPDATE, elapsed);
    ctx.promise->set_value(DB::kOK);
  } else if(result_==FASTER::core::Status::Pending){

  }else {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(UPDATE_FAILED, elapsed);
    ctx.promise->set_value(DB::kError);
  }
  return DB::Status::kOK;
}

DB::Status F2AsyncAdapter::Insert(const std::string &table, const std::string &key,
                              std::vector<Field> &values) {
                                
  uint64_t elapsed = wait_timer_.End();
  measurements_->Report(INSERT_WAIT, elapsed);
  auto promise = std::make_shared<std::promise<DB::Status>>();
  auto op_timer = std::make_shared<utils::Timer<uint64_t, std::nano>>();
  op_timer->Start();
  
  std::string data;
  SerializeRow(values, data);
  UpsertContext ctx(key, data, this, promise, op_timer, true); 
  
  auto result=f2_db_->store_->Upsert(ctx, UpsertCallback, 1);
  if (result == FASTER::core::Status::Ok) {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(INSERT, elapsed);
    ctx.promise->set_value(DB::kOK);
  } else if(result==FASTER::core::Status::Pending){
    
  } else{
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(INSERT_FAILED, elapsed);
    ctx.promise->set_value(DB::kError);
  }
  return DB::Status::kOK;
}

DB::Status F2AsyncAdapter::Delete(const std::string &table, const std::string &key) {
  uint64_t elapsed = wait_timer_.End();
  measurements_->Report(DELETE_WAIT, elapsed);
  
  auto promise = std::make_shared<std::promise<DB::Status>>();
  auto op_timer = std::make_shared<utils::Timer<uint64_t, std::nano>>();
  op_timer->Start();
  
  DeleteContext ctx(key, this, promise, op_timer);
  
  auto result=f2_db_->store_->Delete(ctx, DeleteCallback, 1);
  if (result == FASTER::core::Status::Ok) {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(DELETE, elapsed);
    ctx.promise->set_value(DB::kOK);
  }else if(result==FASTER::core::Status::Pending){
  
  } else {
    uint64_t elapsed = ctx.timer->End();
    ctx.adapter->measurements_->Report(DELETE_FAILED, elapsed);
    ctx.promise->set_value(DB::kError);
  }
  return DB::Status::kOK;
}

void F2AsyncAdapter::SerializeRow(const std::vector<Field> &values, std::string &data) {
  data.clear();
  for (const Field &field : values) {
    uint32_t len = static_cast<uint32_t>(field.name.size());
    data.append(reinterpret_cast<const char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = static_cast<uint32_t>(field.value.size());
    data.append(reinterpret_cast<const char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void F2AsyncAdapter::DeserializeRow(const std::string &data, std::vector<Field> &values) {
  values.clear();
  const char *p = data.data();
  const char *lim = p + data.size();
  while (p < lim) {
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field_name(p, static_cast<size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field_value(p, static_cast<size_t>(len));
    p += len;
    values.push_back({field_name, field_value});
  }
}

void F2AsyncAdapter::DeserializeRowFilter(std::vector<Field> &values, const std::string &data, const std::vector<std::string> &fields) {
  std::vector<Field> all_values;
  DeserializeRow(data, all_values);
  for (const std::string &field : fields) {
    for (const Field &f : all_values) {
      if (f.name == field) {
        values.push_back(f);
        break;
      }
    }
  }
}

DB::Status F2AsyncAdapter::MapStatus(FASTER::core::Status s) {
  return s == FASTER::core::Status::Ok ? DB::Status::kOK : DB::Status::kError;
}
DB::Status F2AsyncAdapter::WaitFuture(std::future<DB::Status>& future) {
  while (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
    f2_db_->store_->CompletePending(false);
    std::this_thread::yield(); 
  }
  return future.get();
}
} // namespace ycsbc