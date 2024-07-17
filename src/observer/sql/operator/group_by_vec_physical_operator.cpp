/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"
RC GroupByVecPhysicalOperator::open(Trx *trx) {
  RC rc = RC::SUCCESS;
  // 打开下层算子
  if ((rc = child_->open(trx)) != RC::SUCCESS) {
    return rc;
  }
  
  // 初始化哈希表
  Chunk chunk;
  while ((rc = child_->next(chunk)) == RC::SUCCESS) {
    // 计算分组键
    std::vector<Value> group_keys;
    for (const auto &expr : group_by_exprs_) {
      Value value;
      if ((rc = expr->evaluate(chunk, value)) != RC::SUCCESS) {
        return rc;
      }
      group_keys.push_back(value);
    }
    
    // 查找或创建分组
    auto it = aggregate_hash_table_.find(group_keys);
    if (it == aggregate_hash_table_.end()) {
      // 初始化新的分组
      aggregate_hash_table_[group_keys] = AggregateRow();
      it = aggregate_hash_table_.find(group_keys);
    }
    
    // 更新聚合结果
    for (auto *expr : expressions_) {
      if ((rc = expr->update_aggregate(chunk, it->second)) != RC::SUCCESS) {
        return rc;
      }
    }
  }
  
  return rc == RC::RECORD_EOF ? RC::SUCCESS : rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk) {
  // 检查是否有更多结果
  if (aggregate_iter_ == aggregate_hash_table_.end()) {
    return RC::RECORD_EOF;
  }
  
  // 构造返回结果
  chunk.clear();
  for (const auto &value : aggregate_iter_->first) {
    chunk.append_column(value);
  }
  for (const auto &agg_value : aggregate_iter_->second.values) {
    chunk.append_column(agg_value);
  }
  
  // 移动到下一个分组
  ++aggregate_iter_;
  return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::close() {
  // 关闭下层算子
  RC rc = child_->close();
  aggregate_hash_table_.clear();
  return rc;
}
