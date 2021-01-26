//
//  scrambled_zipfian_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/8/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_
#define YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_

#include "generator.h"

#include <atomic>
#include <cstdint>
#include "utils.h"
#include "zipfian_generator.h"

namespace ycsbc {

class ScrambledZipfianGenerator : public Generator<uint64_t> {
 public:
  ScrambledZipfianGenerator(uint64_t min, uint64_t max,
      double zipfian_const = ZipfianGenerator::kZipfianConst) :
      base_(min), num_items_(max - min + 1),
      generator_(min, max, zipfian_const) { }
  
  ScrambledZipfianGenerator(uint64_t num_items) :
      ScrambledZipfianGenerator(0, num_items - 1) { }
  
  uint64_t Next();
  uint64_t Last();
  
 private:
  const uint64_t base_;
  const uint64_t num_items_;
  ZipfianGenerator generator_;

  uint64_t Scramble(uint64_t value) const;
};

inline uint64_t ScrambledZipfianGenerator::Scramble(uint64_t value) const {
  return base_ + utils::FNVHash64(value) % num_items_;
}

inline uint64_t ScrambledZipfianGenerator::Next() {
  return Scramble(generator_.Next());
}

inline uint64_t ScrambledZipfianGenerator::Last() {
  return Scramble(generator_.Last());
}

}

#endif // YCSB_C_SCRAMBLED_ZIPFIAN_GENERATOR_H_
