#ifndef YCSB_C_ELASTIC_DB_H_
#define YCSB_C_ELASTIC_DB_H_

#include <string>
#include <mutex>

#include "core/async_db_interface.h"
#include "utils/properties.h"

#include <rocksdb/elastic_lsm.h>

namespace ycsbc {

class ElasticDB : public DB {
 public:
  ElasticDB() {}
  ~ElasticDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string>* fields, std::vector<std::vector<Field>> &result);

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Delete(const std::string &table, const std::string &key);

  bool IsAsyncDB() const override {
    return true;
  }

  Status AddTask(std::function<void()> task) override {
    return elastic_db_->SubmitTask(std::move(task)).ok() ? kOK : kError;
  }

 private:
  enum RocksFormat {
    kSingleRow,
  };
  RocksFormat format_;

  void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                  std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);
  void GetElasticOptions(const utils::Properties &props, rocksdb::ElasticLSMOptions *elastic_options);
  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                   const std::vector<std::string> &fields);
  static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                   const std::vector<std::string> &fields);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

  int fieldcount_;

  static std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  static rocksdb::ElasticLSM *elastic_db_;
  static int ref_cnt_;
  static std::mutex mu_;
  static rocksdb::WriteOptions wopt_;
};

DB *NewElasticDB();

} // ycsbc

#endif // YCSB_C_ELASTIC_DB_H_

