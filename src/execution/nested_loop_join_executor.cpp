//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      now_id_(0) {}

/* void NestedLoopJoinExecutor::Init() { */
/*     Tuple left_tuple; */
/*     RID left_rid; */
/*     left_executor_->Init(); */
/*     try { */
/*         while (left_executor_->Next(&left_tuple, &left_rid)) { */
/*             left_tuples.emplace_back(left_tuple); */
/*         } */
/*     } catch(Exception &e) { */
/*         throw Exception(ExceptionType::UNKNOWN_TYPE, " NestedLoopJoinExecutor: left_executor_ execute error. "); */
/*         return; */
/*     } */
/*     left_size = left_tuples.size(); */

/*     Tuple right_tuple; */
/*     RID right_rid; */
/*     right_executor_->Init(); */
/*     try { */
/*         while (right_executor_->Next(&right_tuple, &right_rid)) { */
/*             right_tuples.emplace_back(right_tuple); */
/*         } */
/*     } catch(Exception &e) { */
/*         throw Exception(ExceptionType::UNKNOWN_TYPE, " NestedLoopJoinExecutor: right_executor_ execute error. "); */
/*         return; */
/*     } */

/*     right_size = right_tuples.size(); */
/* } */

/* bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) { */
/*     while (left_id < left_size) { */
/*         while (right_id < right_size) { */
/*             if (plan_->Predicate() == nullptr || plan_->Predicate() */
/*                                                ->EvaluateJoin(&left_tuples[left_id],
 * left_executor_->GetOutputSchema(), */
/*                                                               &right_tuples[right_id],
 * right_executor_->GetOutputSchema()) */
/*                                                .GetAs<bool>()) { */
/*                 std::vector<Value> output; */
/*                 for (const auto &col : GetOutputSchema()->GetColumns()) { */
/*                     output.push_back(col.GetExpr()->EvaluateJoin(&left_tuples[left_id],
 * left_executor_->GetOutputSchema(), &right_tuples[right_id], */
/*                                                        right_executor_->GetOutputSchema())); */
/*                 } */
/*                 *tuple = Tuple(output, GetOutputSchema()); */
/*                 *rid = tuple->GetRid(); */
/*                 right_id++; */
/*                 return true; */
/*             } */
/*             right_id++; */
/*         } */
/*         left_id++; */
/*     } */
/*     return false; */
/* } */

/**
 * Note: we need to do something different with operations before. In general, we need to keep left tuple_id and right
 * tuple_id, and changed them every time the Next function is invoked. So this is troublesome, if we store result of
 * nest loop from the start, then we just need to increment result tuple_id
 * */

void NestedLoopJoinExecutor::Init() {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;

  left_executor_->Init();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->Predicate() == nullptr || plan_->Predicate()
                                               ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                              &right_tuple, right_executor_->GetOutputSchema())
                                               .GetAs<bool>()) {
        std::vector<Value> output;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                       right_executor_->GetOutputSchema()));
        }
        result_tuples_.emplace_back(Tuple(output, GetOutputSchema()));
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (now_id_ < result_tuples_.size()) {
    *tuple = result_tuples_[now_id_];
    *rid = tuple->GetRid();
    now_id_++;
    return true;
  }
  return false;
}

}  // namespace bustub
