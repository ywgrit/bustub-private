UPDATE: 
增加了mock_scan_executor.cpp mock_scan_executor.h在相应的文件夹
否则有几个测试在本地没法过

### 将测试文件相应的放入如下目录 (原有文件要覆盖)：
**total 12:**
/bustub/test/execution/
 - executor_test.cpp
 - executor_test_util.h
 - grading_aggregation_executor_test.cpp
 - grading_delete_executor_test.cpp
 - grading_distinct_executor_test.cpp
 - grading_executor_integrated_test.cpp
 - grading_hash_join_executor_test.cpp
 - grading_insert_executor_test.cpp
 - grading_limit_executor_test.cpp
 - grading_nested_loop_join_executor_test.cpp
 - grading_sequential_scan_executor_test.cpp
 - grading_update_executor_test.cpp

### 源文件 (原有文件要覆盖)
**total 2:** 
/bustub/src/include/execution/plans/
 - abstract_plan.h
 - mock_scan_plan.h


用法如下（在build目录下）：
```shell
cmake ..
make 测试名
./test/测试名
```

测试名就是测试文件不带文件格式的名字，如 `grading_aggregation_executor_test.cpp` --> `grading_aggregation_executor_test`
