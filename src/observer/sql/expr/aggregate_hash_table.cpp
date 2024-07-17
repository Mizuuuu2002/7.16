/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/expr/aggregate_hash_table.h"

// ----------------------------------StandardAggregateHashTable------------------

RC StandardAggregateHashTable::add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk)
{
  RC rc = RC::SUCCESS;

  size_t num_rows = groups_chunk.rows();
  for (size_t row_idx = 0; row_idx < num_rows; ++row_idx) {
    // 计算分组键
    std::vector<Value> group_keys;
    for (size_t col_idx = 0; col_idx < groups_chunk.column_num(); ++col_idx) {
      Value value = groups_chunk.column(col_idx).get_value(row_idx);
      group_keys.push_back(value);
    }

    // 查找或创建分组
    auto it = hash_table_.find(group_keys);
    if (it == hash_table_.end()) {
      // 初始化新的分组
      std::vector<AggregateValue> aggrs(aggrs_chunk.column_num());
      for (size_t col_idx = 0; col_idx < aggrs_chunk.column_num(); ++col_idx) {
        aggrs[col_idx].initialize(aggrs_chunk.column(col_idx).type());
      }
      hash_table_[group_keys] = std::move(aggrs);
      it = hash_table_.find(group_keys);
    }

    // 更新聚合结果
    auto &aggrs = it->second;
    for (size_t col_idx = 0; col_idx < aggrs_chunk.column_num(); ++col_idx) {
      Value value = aggrs_chunk.column(col_idx).get_value(row_idx);
      aggrs[col_idx].update(value);
    }
  }

  return rc;
}

void StandardAggregateHashTable::Scanner::open_scan()
{
  it_  = static_cast<StandardAggregateHashTable *>(hash_table_)->begin();
  end_ = static_cast<StandardAggregateHashTable *>(hash_table_)->end();
}

RC StandardAggregateHashTable::Scanner::next(Chunk &output_chunk)
{
  if (it_ == end_) {
    return RC::RECORD_EOF;
  }
  while (it_ != end_ && output_chunk.rows() <= output_chunk.capacity()) {
    auto &group_by_values = it_->first;
    auto &aggrs           = it_->second;
    for (int i = 0; i < output_chunk.column_num(); i++) {
      auto col_idx = output_chunk.column_ids(i);
      if (col_idx >= static_cast<int>(group_by_values.size())) {
        output_chunk.column(i).append_one((char *)aggrs[col_idx - group_by_values.size()].data());
      } else {
        output_chunk.column(i).append_one((char *)group_by_values[col_idx].data());
      }
    }
    it_++;
  }
  if (it_ == end_) {
    return RC::SUCCESS;
  }

  return RC::SUCCESS;
}

size_t StandardAggregateHashTable::VectorHash::operator()(const vector<Value> &vec) const
{
  size_t hash = 0;
  for (const auto &elem : vec) {
    hash ^= std::hash<string>()(elem.to_string());
  }
  return hash;
}

bool StandardAggregateHashTable::VectorEqual::operator()(const vector<Value> &lhs, const vector<Value> &rhs) const
{
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (rhs[i].compare(lhs[i]) != 0) {
      return false;
    }
  }
  return true;
}

// ----------------------------------LinearProbingAggregateHashTable------------------
#ifdef USE_SIMD
template <typename V>
/**
 * 向线性探测聚合哈希表中添加一个新的块。
 * 此函数用于在哈希表中添加一组已经聚合过的数据。它首先检查输入的分组块和聚合块是否满足特定条件，
 * 然后将聚合块的数据添加到哈希表中。
 * 
 * @param group_chunk 分组块，包含需要进行聚合操作的数据组。
 * @param aggr_chunk 聚合块，包含已经根据分组键进行过聚合计算的结果。
 * @return 返回操作结果，如果操作成功则返回RC::SUCCESS，否则返回RC::INVALID_ARGUMENT。
 */
RC LinearProbingAggregateHashTable<V>::add_chunk(Chunk &group_chunk, Chunk &aggr_chunk)
{
  // 检查分组块和聚合块的列数是否都为1，因为此哈希表仅支持单列分组和聚合。
  if (group_chunk.column_num() != 1 || aggr_chunk.column_num() != 1) {
    LOG_WARN("group_chunk and aggr_chunk size must be 1.");
    return RC::INVALID_ARGUMENT;
  }
  
  // 检查分组块和聚合块的行数是否相等，确保每个分组都有对应的结果。
  if (group_chunk.rows() != aggr_chunk.rows()) {
    LOG_WARN("group_chunk and aggr_chunk rows must be equal.");
    return RC::INVALID_ARGUMENT;
  }
  
  // 将聚合块的数据批量添加到哈希表中，这里假设哈希表已经正确初始化并准备好接收数据。
  add_batch((int *)group_chunk.column(0).data(), (V *)aggr_chunk.column(0).data(), group_chunk.rows());
  
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::open_scan()
{
  capacity_   = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->capacity();
  size_       = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->size();
  scan_pos_   = 0;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::Scanner::next(Chunk &output_chunk)
{
  if (scan_pos_ >= capacity_ || scan_count_ >= size_) {
    return RC::RECORD_EOF;
  }
  auto linear_probing_hash_table = static_cast<LinearProbingAggregateHashTable *>(hash_table_);
  while (scan_pos_ < capacity_ && scan_count_ < size_ && output_chunk.rows() <= output_chunk.capacity()) {
    int key;
    V   value;
    RC  rc = linear_probing_hash_table->iter_get(scan_pos_, key, value);
    if (rc == RC::SUCCESS) {
      output_chunk.column(0).append_one((char *)&key);
      output_chunk.column(1).append_one((char *)&value);
      scan_count_++;
    }
    scan_pos_++;
  }
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::close_scan()
{
  capacity_   = -1;
  size_       = -1;
  scan_pos_   = -1;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::get(int key, V &value)
{
  RC  rc          = RC::SUCCESS;
  int index       = (key % capacity_ + capacity_) % capacity_;
  int iterate_cnt = 0;
  while (true) {
    if (keys_[index] == EMPTY_KEY) {
      rc = RC::NOT_EXIST;
      break;
    } else if (keys_[index] == key) {
      value = values_[index];
      break;
    } else {
      index += 1;
      index %= capacity_;
      iterate_cnt++;
      if (iterate_cnt > capacity_) {
        rc = RC::NOT_EXIST;
        break;
      }
    }
  }
  return rc;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::iter_get(int pos, int &key, V &value)
{
  RC rc = RC::SUCCESS;
  if (keys_[pos] == LinearProbingAggregateHashTable<V>::EMPTY_KEY) {
    rc = RC::NOT_EXIST;
  } else {
    key   = keys_[pos];
    value = values_[pos];
  }
  return rc;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::aggregate(V *value, V value_to_aggregate)
{
  if (aggregate_type_ == AggregateExpr::Type::SUM) {
    *value += value_to_aggregate;
  } else {
    ASSERT(false, "unsupported aggregate type");
  }
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize()
{
  capacity_ *= 2;
  std::vector<int> new_keys(capacity_);
  std::vector<V>   new_values(capacity_);

  for (size_t i = 0; i < keys_.size(); i++) {
    auto &key   = keys_[i];
    auto &value = values_[i];
    if (key != EMPTY_KEY) {
      int index = (key % capacity_ + capacity_) % capacity_;
      while (new_keys[index] != EMPTY_KEY) {
        index = (index + 1) % capacity_;
      }
      new_keys[index]   = key;
      new_values[index] = value;
    }
  }

  keys_   = std::move(new_keys);
  values_ = std::move(new_values);
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize_if_need()
{
  if (size_ >= capacity_ / 2) {
    resize();
  }
}

template <typename V>
void LinearProbingAggregateHashTable<V>::add_batch(int *input_keys, V *input_values, int len)
{
  // your code here
  // inv (invalid) 表示是否有效，inv[i] = -1 表示有效，inv[i] = 0 表示无效。
  // key[SIMD_WIDTH],value[SIMD_WIDTH] 表示当前循环中处理的键值对。
  // off (offset) 表示线性探测冲突时的偏移量，key[i] 每次遇到冲突键，则off[i]++，如果key[i] 已经完成聚合，则off[i] = 0，
  // i = 0 表示selective load 的起始位置。
  // inv 全部初始化为 -1
  // off 全部初始化为 0
  std::vector<int> inv(SIMD_WIDTH, -1); // 初始化 inv 向量，所有元素设为-1，表示有效。
  std::vector<int> off(SIMD_WIDTH, 0); // 初始化 off 向量，所有元素设为0，表示初始偏移量。
  int i=0;
  while (i + SIMD_WIDTH <= len) {
    int valid_count = 0;
    
    // 1: 根据 `inv` 变量的值，从 `input_keys` 中 `selective load` `SIMD_WIDTH` 个不同的输入键值对。
    for (int j = 0; j < SIMD_WIDTH; ++j) {
      if (inv[j] == -1) { // 如果 inv[j] = -1 表示该位置有效，可以加载新数据
        keys_[j] = input_keys[i + valid_count];
        values_[j] = input_values[i + valid_count];
        inv[j] = 0; // 设置为0表示正在处理
        ++valid_count;
      }
    }
    
    // 2. 计算 i += |inv|, `|inv|` 表示 `inv` 中有效的个数 
    i += valid_count;

    // 3. 计算 hash 值，
    for (int j = 0; j < SIMD_WIDTH; ++j) {
      if (inv[j] == 0) { // 只对正在处理的键值对计算hash值
        hash_values_[j] = hash_function(keys_[j]);
      }
    }

    // 4. 根据聚合类型（目前只支持 sum），在哈希表中更新聚合结果。
    // 如果本次循环，没有找到key[i] 在哈希表中的位置，则不更新聚合结果。
    for (int j = 0; j < SIMD_WIDTH; ++j) {
      if (inv[j] == 0) { // 对于正在处理的键值对
        int index = hash_values_[j] % table_size_;
        while (table_keys_[index] != keys_[j]) { // 线性探测直到找到或到达空位
          if (table_keys_[index] == -1) { // 如果是空位，插入新键值对
            table_keys_[index] = keys_[j];
            table_values_[index] = values_[j];
            break;
          } else {
            ++index; // 继续线性探测
            ++off[j]; // 增加偏移量
          }
        }
        if (table_keys_[index] == keys_[j]) { // 如果找到了键，则更新聚合结果
          table_values_[index] += values_[j];
          inv[j] = -1; // 聚合完成，标记为有效
          off[j] = 0; // 重置偏移量
        }
      }
    }
    
    // 5. gather 操作，根据 hash 值将 keys_ 的 gather 结果写入 table_key 中。
    // 这一步通常在SIMD环境下使用，此处简化处理，直接使用了线性探测。
    // 6. 更新 inv 和 off。如果本次循环key[i] 聚合完成，则inv[i]=-1，表示该位置在下次循环中读取新的键值对。
    // 如果本次循环 key[i] 未在哈希表中聚合完成（table_key[i] != key[i]），则inv[i] = 0，表示该位置在下次循环中不需要读取新的键值对。
    // 如果本次循环中，key[i]聚合完成，则off[i] 更新为 0，表示线性探测偏移量为 0，key[i] 未完成聚合，则off[i]++,表示线性探测偏移量加 1。
    for (int j = 0; j < SIMD_WIDTH; ++j) {
      if (inv[j] == 0) {
        int index = hash_values_[j] % table_size_;
        while (table_keys_[index] != keys_[j]) {
          if (table_keys_[index] == -1){
            table_keys_[index] = keys_[j];
            table_values_[index] = values_[j];
            break;
          }
          else{
            ++index; // 继续线性探测
            ++off[j]; // 增加偏移量
          }
        }
        if(table_keys_[index] == keys_[j]){
          table_values_[index] += values_[j];
          inv[j] = -1; // 聚合完成，标记为有效
          off[j] = 0; // 重置偏移量
        }
      }
    }
  }
  
  // 7. 通过标量线性探测，处理剩余键值对
  for (; i < len; ++i) {
    int index = hash_function(input_keys[i]) % table_size_;
    while (table_keys_[index] != input_keys[i]) {
      if (table_keys_[index] == -1) {
        table_keys_[index] = input_keys[i];
        table_values_[index] = input_values[i];
        break;
      } else {
        ++index;
      }
    }
    if (table_keys_[index] == input_keys[i]) {
      table_values_[index] += input_values[i];
    }
  }
  // inv (invalid) 表示是否有效，inv[i] = -1 表示有效，inv[i] = 0 表示无效。
  // key[SIMD_WIDTH],value[SIMD_WIDTH] 表示当前循环中处理的键值对。
  // off (offset) 表示线性探测冲突时的偏移量，key[i] 每次遇到冲突键，则off[i]++，如果key[i] 已经完成聚合，则off[i] = 0，
  // i = 0 表示selective load 的起始位置。
  // inv 全部初始化为 -1
  // off 全部初始化为 0

  // for (; i + SIMD_WIDTH <= len;) {
    // 1: 根据 `inv` 变量的值，从 `input_keys` 中 `selective load` `SIMD_WIDTH` 个不同的输入键值对。
    // 2. 计算 i += |inv|, `|inv|` 表示 `inv` 中有效的个数 
    // 3. 计算 hash 值，
    // 4. 根据聚合类型（目前只支持 sum），在哈希表中更新聚合结果。如果本次循环，没有找到key[i] 在哈希表中的位置，则不更新聚合结果。
    // 5. gather 操作，根据 hash 值将 keys_ 的 gather 结果写入 table_key 中。
    // 6. 更新 inv 和 off。如果本次循环key[i] 聚合完成，则inv[i]=-1，表示该位置在下次循环中读取新的键值对。
    // 如果本次循环 key[i] 未在哈希表中聚合完成（table_key[i] != key[i]），则inv[i] = 0，表示该位置在下次循环中不需要读取新的键值对。
    // 如果本次循环中，key[i]聚合完成，则off[i] 更新为 0，表示线性探测偏移量为 0，key[i] 未完成聚合，则off[i]++,表示线性探测偏移量加 1。
  // }
  //7. 通过标量线性探测，处理剩余键值对

  // resize_if_need();
}

template <typename V>
const int LinearProbingAggregateHashTable<V>::EMPTY_KEY = 0xffffffff;
template <typename V>
const int LinearProbingAggregateHashTable<V>::DEFAULT_CAPACITY = 16384;

template class LinearProbingAggregateHashTable<int>;
template class LinearProbingAggregateHashTable<float>;
#endif
