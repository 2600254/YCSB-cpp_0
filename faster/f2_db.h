#ifndef YCSB_C_F2_DB_H_
#define YCSB_C_F2_DB_H_

#include <string>
#include <iostream>
#include <mutex>
#include <vector>
#include <cstring>
#include <string_view>
#include <memory>
#include "../core/db_factory.h"
#include "core/db.h"
#include "utils/properties.h"

// FASTER includes
#include "core/f2.h"
#include "core/utility.h"


namespace ycsbc {  

constexpr size_t kF2MaxKeySize = 24;
constexpr size_t kF2MaxValueSize = 128; 




struct F2Key {
  uint32_t len;
  char data[kF2MaxKeySize];

  F2Key() : len(0) {std::memset(data, 0, kF2MaxKeySize);}

  F2Key(const std::string& key) {
    len = static_cast<uint32_t>(std::min(key.size(), kF2MaxKeySize));
    std::memset(data, 0, kF2MaxKeySize);
    if (len > 0) {
      std::memcpy(data, key.data(), len);
    }
  }
  inline constexpr uint32_t size() const {
    return static_cast<uint32_t>(sizeof(F2Key));
  }
  inline FASTER::core::KeyHash GetHash() const {
  return KeyHash{ FasterHashHelper<char>::compute(data,len) };
}
  inline bool operator==(const F2Key& other) const {
    if (len != other.len) return false;
    return std::memcmp(data, other.data, len) == 0;
  }
};
struct F2Value {
  uint32_t len;
  char data[kF2MaxValueSize];
  
  F2Value() : len(0) {}
  inline constexpr uint32_t size() const {
    return static_cast<uint32_t>(sizeof(F2Value));
  }
};

class F2Db : public DB {
 public:
  typedef F2Key Key;
  typedef F2Value Value;
  
  typedef FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler, 1ULL << 30> DiskType;
  typedef FASTER::core::F2Kv<Key, Value, DiskType> F2KvType;

  F2Db() {}
  ~F2Db() {}

  void Init() override;
  void Cleanup() override;

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) override{return DB::Status::kOK;};

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) override{return DB::Status::kOK;};

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) override{return DB::Status::kOK;};

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) override{return DB::Status::kOK;};

  Status Delete(const std::string &table, const std::string &key) override{return DB::Status::kOK;};

  
  static std::unique_ptr<F2KvType> store_;

 private:
  bool session_started_ = false;
  FASTER::core::Guid session_id_;
  static std::atomic<int> ref_cnt_;
  static std::mutex mu_;

  void SerializeRow(const std::vector<Field> &values, std::string &data);
  void DeserializeRow(const std::string &data, std::vector<Field> &values);
  void DeserializeRowFilter(std::vector<Field> &values, const std::string &data, const std::vector<std::string> &fields);
};

DB *NewF2DB();

} // namespace ycsbc

#endif // YCSB_C_F2_DB_H_