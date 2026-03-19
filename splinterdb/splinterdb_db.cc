#include "splinterdb_db.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <stdexcept>

#include "core/core_workload.h"
#include "utils/utils.h"
namespace {

const std::string PROP_NAME = "splinterdb.dbname";
const std::string PROP_NAME_DEFAULT = "splinter.db";

const std::string PROP_DISK_SIZE = "splinterdb.disk_size";
const std::string PROP_DISK_SIZE_DEFAULT = "32212254720"; // 30GB

const std::string PROP_CACHE_SIZE = "splinterdb.cache_size";
const std::string PROP_CACHE_SIZE_DEFAULT = "10737418240"; // 10GB

const std::string PROP_KEY_SIZE = "splinterdb.max_key_size";
const std::string PROP_KEY_SIZE_DEFAULT = "64";

const std::string PROP_DESTROY = "splinterdb.destroy";
const std::string PROP_DESTROY_DEFAULT = "false";

const std::string PROP_FORMAT = "splinterdb.format";
const std::string PROP_FORMAT_DEFAULT = "single";

const std::string PROP_PAGE_SIZE = "splinterdb.page_size";
const std::string PROP_PAGE_SIZE_DEFAULT = "4096"; // 4KB

const std::string PROP_EXTENT_SIZE = "splinterdb.extent_size";
const std::string PROP_EXTENT_SIZE_DEFAULT = "131072"; // 128KB

const std::string PROP_IO_FLAGS = "splinterdb.io_flags";
const std::string PROP_IO_FLAGS_DEFAULT = "0";

const std::string PROP_IO_PERMS = "splinterdb.io_perms";
const std::string PROP_IO_PERMS_DEFAULT = "0644"; 

const std::string PROP_IO_ASYNC_QUEUE_DEPTH = "splinterdb.io_async_queue_depth";
const std::string PROP_IO_ASYNC_QUEUE_DEPTH_DEFAULT = "256";

const std::string PROP_CACHE_USE_STATS = "splinterdb.cache_use_stats";
const std::string PROP_CACHE_USE_STATS_DEFAULT = "false";

const std::string PROP_CACHE_LOGFILE = "splinterdb.cache_logfile";
const std::string PROP_CACHE_LOGFILE_DEFAULT = ""; 

const std::string PROP_NUM_MEMTABLE_BG_THREADS = "splinterdb.num_memtable_bg_threads";
const std::string PROP_NUM_MEMTABLE_BG_THREADS_DEFAULT = "8";

const std::string PROP_NUM_NORMAL_BG_THREADS = "splinterdb.num_normal_bg_threads";
const std::string PROP_NUM_NORMAL_BG_THREADS_DEFAULT = "8";

const std::string PROP_BTREE_ROUGH_COUNT_HEIGHT = "splinterdb.btree_rough_count_height";
const std::string PROP_BTREE_ROUGH_COUNT_HEIGHT_DEFAULT = "0";

const std::string PROP_FILTER_HASH_SIZE = "splinterdb.filter_hash_size";
const std::string PROP_FILTER_HASH_SIZE_DEFAULT = "0";

const std::string PROP_FILTER_LOG_INDEX_SIZE = "splinterdb.filter_log_index_size";
const std::string PROP_FILTER_LOG_INDEX_SIZE_DEFAULT = "0";

const std::string PROP_USE_LOG = "splinterdb.use_log";
const std::string PROP_USE_LOG_DEFAULT = "true";

const std::string PROP_MEMTABLE_CAPACITY = "splinterdb.memtable_capacity";
const std::string PROP_MEMTABLE_CAPACITY_DEFAULT = "671088640"; 

const std::string PROP_FANOUT = "splinterdb.fanout";
const std::string PROP_FANOUT_DEFAULT = "16";

const std::string PROP_USE_STATS = "splinterdb.use_stats";
const std::string PROP_USE_STATS_DEFAULT = "0";

const std::string PROP_RECLAIM_THRESHOLD = "splinterdb.reclaim_threshold";
const std::string PROP_RECLAIM_THRESHOLD_DEFAULT = "0";

const std::string PROP_QUEUE_SCALE_PERCENT = "splinterdb.queue_scale_percent";
const std::string PROP_QUEUE_SCALE_PERCENT_DEFAULT = "0";

const std::string PROP_USE_SHMEM = "splinterdb.use_shmem";
const std::string PROP_USE_SHMEM_DEFAULT = "false";

const std::string PROP_SHMEM_SIZE = "splinterdb.shmem_size";
const std::string PROP_SHMEM_SIZE_DEFAULT = "0";

// Ensure that the current thread is registered with SplinterDB. Each thread
// must register before using the DB and must deregister on thread exit.


bool FileExists(const std::string &path) {
  struct stat st;
  return (stat(path.c_str(), &st) == 0);
}
static mode_t ParseOctalPerms(const std::string& perm_str) {
  mode_t perms = 0;
  try {
    std::string clean_str = perm_str;
    if (!clean_str.empty() && clean_str[0] == '0') {
      clean_str = clean_str.substr(1);
    }
    perms = static_cast<mode_t>(std::stoul(clean_str, nullptr, 8));
  } catch (...) {
    perms = 0644;
  }
  return perms;
}
}  // namespace

namespace ycsbc {

splinterdb *SplinterdbDB::db_ = nullptr;
splinterdb_config SplinterdbDB::cfg_ = {};
data_config SplinterdbDB::data_cfg_ = {};
std::string SplinterdbDB::db_path_;
int SplinterdbDB::ref_cnt_ = 0;
std::mutex SplinterdbDB::mu_;

void SplinterdbDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string format = props.GetProperty("splinterdb.format", "single");
  if (format == "single") {
    format_ = kSingleRow;
    method_read_ = &SplinterdbDB::ReadSingle;
    method_scan_ = &SplinterdbDB::ScanSingle;
    method_update_ = &SplinterdbDB::UpdateSingle;
    method_insert_ = &SplinterdbDB::InsertSingle;
    method_delete_ = &SplinterdbDB::DeleteSingle;
  } else {
    throw utils::Exception("unknown format");
  }

  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;
  if (db_) {
    return;
  }

  db_path_ = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path_.empty()) {
    throw utils::Exception("SplinterDB db path is missing");
  }

  GetOptions(props, &cfg_, &data_cfg_);

  const bool destroy = props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true";

  cfg_.filename=db_path_.c_str();
  if (destroy && FileExists(db_path_)) {
    int rc = unlink(db_path_.c_str());
    if (rc != 0) {
      throw utils::Exception("SplinterDB remove db path failed");
    }
  }

  int rc;
  platform_register_thread();
  if (FileExists(db_path_)) {
    rc = splinterdb_open(&cfg_, &db_);
  } else {
    rc = splinterdb_create(&cfg_, &db_);
  }
  if (rc != 0) {
    throw utils::Exception(std::string("SplinterDB open/create failed: ") + std::to_string(rc));
  }
  
}

void SplinterdbDB::Cleanup() { 
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  if (db_) {
    splinterdb_close(&db_);
    db_ = nullptr;
  }
  // Note: thread_local lookup_result will be deinitialized automatically
  // when threads exit, but we can't force it here.
}

void SplinterdbDB::GetOptions(const utils::Properties &props,
                              splinterdb_config *cfg,
                              data_config *data_cfg) {
  db_path_ = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  uint64_t disk_size = std::stoull(props.GetProperty(PROP_DISK_SIZE, PROP_DISK_SIZE_DEFAULT));
  uint64_t cache_size = std::stoull(props.GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
  uint64_t max_key_size = std::stoull(props.GetProperty(PROP_KEY_SIZE, PROP_KEY_SIZE_DEFAULT));
  cfg->filename = db_path_.c_str();
  cfg->disk_size = disk_size;
  cfg->cache_size = cache_size;
  default_data_config_init(max_key_size, data_cfg);
  cfg->data_cfg = data_cfg;
  cfg->heap_handle = nullptr;
  cfg->heap_id = nullptr;
  cfg->use_shmem = (props.GetProperty(PROP_USE_SHMEM, PROP_USE_SHMEM_DEFAULT) == "true");
  cfg->shmem_size = std::stoull(props.GetProperty(PROP_SHMEM_SIZE, PROP_SHMEM_SIZE_DEFAULT));
  cfg->page_size = std::stoul(props.GetProperty(PROP_PAGE_SIZE, PROP_PAGE_SIZE_DEFAULT));
  cfg->extent_size = std::stoul(props.GetProperty(PROP_EXTENT_SIZE, PROP_EXTENT_SIZE_DEFAULT));
  cfg->io_flags = std::stoul(props.GetProperty(PROP_IO_FLAGS, PROP_IO_FLAGS_DEFAULT));
  cfg->io_perms = ParseOctalPerms(props.GetProperty(PROP_IO_PERMS, PROP_IO_PERMS_DEFAULT));
  cfg->io_async_queue_depth = std::stoul(props.GetProperty(PROP_IO_ASYNC_QUEUE_DEPTH, PROP_IO_ASYNC_QUEUE_DEPTH_DEFAULT));
  cfg->cache_use_stats = (props.GetProperty(PROP_CACHE_USE_STATS, PROP_CACHE_USE_STATS_DEFAULT) == "true");
  std::string cache_logfile = props.GetProperty(PROP_CACHE_LOGFILE, PROP_CACHE_LOGFILE_DEFAULT);
  cfg->cache_logfile = cache_logfile.empty() ? nullptr : cache_logfile.c_str();
  cfg->num_memtable_bg_threads = std::stoul(props.GetProperty(PROP_NUM_MEMTABLE_BG_THREADS, PROP_NUM_MEMTABLE_BG_THREADS_DEFAULT));
  cfg->num_normal_bg_threads = std::stoul(props.GetProperty(PROP_NUM_NORMAL_BG_THREADS, PROP_NUM_NORMAL_BG_THREADS_DEFAULT));
  cfg->btree_rough_count_height = std::stoul(props.GetProperty(PROP_BTREE_ROUGH_COUNT_HEIGHT, PROP_BTREE_ROUGH_COUNT_HEIGHT_DEFAULT));
  cfg->filter_hash_size = std::stoul(props.GetProperty(PROP_FILTER_HASH_SIZE, PROP_FILTER_HASH_SIZE_DEFAULT));
  cfg->filter_log_index_size = std::stoul(props.GetProperty(PROP_FILTER_LOG_INDEX_SIZE, PROP_FILTER_LOG_INDEX_SIZE_DEFAULT));
  cfg->use_log = (props.GetProperty(PROP_USE_LOG, PROP_USE_LOG_DEFAULT) == "true");
  cfg->memtable_capacity = std::stoull(props.GetProperty(PROP_MEMTABLE_CAPACITY, PROP_MEMTABLE_CAPACITY_DEFAULT));
  cfg->fanout = std::stoul(props.GetProperty(PROP_FANOUT, PROP_FANOUT_DEFAULT));
  cfg->use_stats = std::stoul(props.GetProperty(PROP_USE_STATS, PROP_USE_STATS_DEFAULT));
  cfg->reclaim_threshold = std::stoul(props.GetProperty(PROP_RECLAIM_THRESHOLD, PROP_RECLAIM_THRESHOLD_DEFAULT));
  cfg->queue_scale_percent = std::stoul(props.GetProperty(PROP_QUEUE_SCALE_PERCENT, PROP_QUEUE_SCALE_PERCENT_DEFAULT));
}

void SplinterdbDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<const char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<const char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void SplinterdbDB::DeserializeRowFilter(std::vector<Field> &values,
                                       const char *p,
                                       const char *lim,
                                       const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void SplinterdbDB::DeserializeRowFilter(std::vector<Field> &values,
                                       const std::string &data,
                                       const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void SplinterdbDB::DeserializeRow(std::vector<Field> &values,
                                  const char *p,
                                  const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void SplinterdbDB::DeserializeRow(std::vector<Field> &values,
                                  const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

DB::Status SplinterdbDB::ReadSingle(const std::string &table,
                                    const std::string &key,
                                    const std::vector<std::string> *fields,
                                    std::vector<Field> &result) {

  static thread_local splinterdb_lookup_result lookup_result;
  static thread_local bool initialized = false;
  
  if (!initialized) {
    splinterdb_lookup_result_init(db_, &lookup_result, 0, nullptr);
    initialized = true;
  }
  int rc = splinterdb_lookup(db_, slice_create(key.size(), key.data()), &lookup_result);
  if (rc != 0) {
    return kNotFound;
  }

  if (!splinterdb_lookup_found(&lookup_result)) { 
    return kNotFound;
  }

  slice val;
  rc = splinterdb_lookup_result_value(&lookup_result, &val);
  if (rc != 0) { 
    throw utils::Exception("SplinterDB lookup_result_value failed");
  }
  std::string data(reinterpret_cast<const char *>(val.data), val.length);

  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status SplinterdbDB::ScanSingle(const std::string &table,
                                    const std::string &key,
                                    int len,
                                    const std::vector<std::string> *fields,
                                    std::vector<std::vector<Field>> &result) {

  splinterdb_iterator *it;
  int rc = splinterdb_iterator_init(db_, &it, slice_create(key.size(), key.data()));
  if (rc != 0) {
    throw utils::Exception("SplinterDB iterator init failed");
  }

  for (int i = 0; splinterdb_iterator_valid(it) && i < len; i++) {
    slice k;
    slice v;
    splinterdb_iterator_get_current(it, &k, &v);

    std::string data(reinterpret_cast<const char *>(v.data), v.length);
    result.emplace_back();
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, data, *fields);
    } else {
      DeserializeRow(values, data);
      assert(values.size() == static_cast<size_t>(fieldcount_));
    }
    if (splinterdb_iterator_can_next(it)) {
      splinterdb_iterator_next(it);
    } else {
      break;
    }
  }

  int status = splinterdb_iterator_status(it);
  splinterdb_iterator_deinit(it);

  if (status != 0) {
    throw utils::Exception("SplinterDB iterator error");
  }
  
  return kOK;
}

DB::Status SplinterdbDB::UpdateSingle(const std::string &table,  
                                      const std::string &key,  
                                      std::vector<Field> &values) {  
  static thread_local splinterdb_lookup_result update_lookup_result;  
  static thread_local bool update_initialized = false;  
  if (!update_initialized) {  
    splinterdb_lookup_result_init(db_, &update_lookup_result, 0, nullptr);  
    update_initialized = true;  
  }  
  std::vector<Field> current_values;  
    
  int rc = splinterdb_lookup(db_, slice_create(key.size(), key.data()), &update_lookup_result);  
  if (rc != 0 || !splinterdb_lookup_found(&update_lookup_result)) {  
    return kNotFound;  
  }  
  
  slice val;  
  rc = splinterdb_lookup_result_value(&update_lookup_result, &val);  
  if (rc != 0) {  
    throw utils::Exception("SplinterDB lookup_result_value failed");  
  }  
    
  std::string data(reinterpret_cast<const char *>(val.data), val.length);  
  DeserializeRow(current_values, data);  
  
  for (Field &new_field : values) {  
    bool found = false;  
    for (Field &cur_field : current_values) {  
      if (cur_field.name == new_field.name) {  
        found = true;  
        cur_field.value = new_field.value;  
        break;  
      }  
    }  
    if (!found) {  
      current_values.push_back(new_field);  
    }  
  }  
    
  std::string new_data;  
  SerializeRow(current_values, new_data);  
  rc = splinterdb_insert(db_, slice_create(key.size(), key.data()),  
                         slice_create(new_data.size(), new_data.data()));  
  if (rc != 0) {  
    throw utils::Exception("SplinterDB insert failed");  
  }  
  return kOK;  
}

DB::Status SplinterdbDB::InsertSingle(const std::string &table,
                                      const std::string &key,
                                      std::vector<Field> &values) {

  std::string data;
  SerializeRow(values, data);
  
  int rc = splinterdb_insert(db_, slice_create(key.size(), key.data()),
                             slice_create(data.size(), data.data()));
  
  if (rc != 0) {
    throw utils::Exception("SplinterDB insert failed");
  }
  return kOK;
}

DB::Status SplinterdbDB::DeleteSingle(const std::string &table,
                                      const std::string &key) {

  int rc = splinterdb_delete(db_, slice_create(key.size(), key.data()));
  if (rc != 0) {
    throw utils::Exception("SplinterDB delete failed");
  }
  return kOK;
}

DB *NewSplinterdbDB() {
  return new SplinterdbDB;
}

const bool registered = DBFactory::RegisterDB("splinterdb", NewSplinterdbDB);

}  // namespace ycsbc
