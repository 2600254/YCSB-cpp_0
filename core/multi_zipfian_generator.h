#ifndef YCSB_C_MULTI_ZIPFIAN_GENERATOR_H_
#define YCSB_C_MULTI_ZIPFIAN_GENERATOR_H_

#include "scrambled_zipfian_generator.h"

namespace ycsbc
{
  enum class OperationType
  {
    kRead,
    kWrite
  };
  class MultiZipfianGenerator
  {
  public:
    MultiZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const, double read_hotspot_pos = 0, double write_hotspot_pos = 0)
        : read_generator_(min, max, zipfian_const), write_generator_(min, max, zipfian_const),
          key_min_(min), key_max_(max) {
      UpdateHotspotPosition(read_hotspot_pos, write_hotspot_pos);
    }

    MultiZipfianGenerator(uint64_t min, uint64_t max) 
        : MultiZipfianGenerator(min, max, ZipfianGenerator::kZipfianConst) {}

    MultiZipfianGenerator(uint64_t num_items) 
        : MultiZipfianGenerator(0, num_items - 1) {}

    ~MultiZipfianGenerator() {}

    uint64_t Next(OperationType op_type) {
      if (op_type == OperationType::kRead)
      {
        uint64_t pos = read_generator_.Next() + read_hotspot_pos_;
        return pos > key_min_ ? pos - (key_max_ - key_min_) : pos;
      }
      else
      {
        uint64_t pos = write_generator_.Next() + write_hotspot_pos_;
        return pos > key_min_ ? pos - (key_max_ - key_min_) : pos;
      }
    }

    void UpdateHotspotPosition(double read_hotspot_pos, double write_hotspot_pos) {
      read_hotspot_pos_ = read_hotspot_pos * (key_max_ - key_min_) + key_min_;
      write_hotspot_pos_ = write_hotspot_pos * (key_max_ - key_min_) + key_min_;
    }

  private:
    ScrambledZipfianGenerator read_generator_;
    ScrambledZipfianGenerator write_generator_;
    uint64_t key_min_;
    uint64_t key_max_;
    uint64_t read_hotspot_pos_;
    uint64_t write_hotspot_pos_;
  };

}
#endif //YCSB_C_MULTI_ZIPFIAN_GENERATOR_H_