# lab2.0 - Grid-Search Optimized Scheduler

基于 lab1.0，通过 20 组合全量网格搜索找到最优参数配置。

---

## 与 lab1.0 相比的变更

### Cost 函数参数调整

| 参数 | lab1.0 | lab2.0 | 说明 |
|------|:---:|:---:|------|
| FLEX_W | 2.0 | 2.0 | 不变 |
| SLACK_W（等效权重） | 0.20（`0.60 × 平均剩余比例`） | **0.40**（`0.40 × SUM 剩余比例`） | slack 权重大幅提升，鼓励更紧凑的放置 |
| WASTE_W | 0.25 | **0.25** | 保留（网格扫描证实有效） |
| Backfill | 迭代收敛（最多 100 轮） | **单次扫描** | 网格扫描发现迭代无额外收益，关闭以节约时间 |

### 性能优化

| 项目 | lab1.0 | lab2.0 | 说明 |
|------|:---:|:---:|------|
| 预计算倒数 | 有（`inv_gpu_count`/`inv_cpu_cores`/`inv_memory`） | 有 | 保留，热路径乘法替代除法 |
| inline cost | 有 | 有 | 保留 |
| 灵活性预计算 | 有（`mfi[]`） | 有 | 保留 |
| 不可调度任务处理 | 静默跳过 | 静默跳过 | 保留 |

### 参数可配置化

lab1.0 的宏定义直接硬编码在 scheduler.h 中。lab2.0 将所有调优参数统一为宏：
```cpp
#define FLEX_W 2.0        // 机器灵活度权重
#define SLACK_W 0.40      // 资源紧凑度权重（网格搜索最优）
#define WASTE_W 0.25      // 显存浪费惩罚  
#define BACKFILL_ITER 0   // 0=单次回填, 1=迭代收敛
```

### 为何改成单次 Backfill

20 组网格扫描中，所有 single/iter 配对结果完全一致，迭代收敛循环没有产生任何额外的任务放置。说明在当前代价函数和任务集合下，单次扫描已足够——多轮收敛是纯开销。

---

## 构建

```bash
g++ -std=c++17 -O2 src/main.cpp src/parser.cpp src/machine_state.cpp \
    src/scheduler.cpp src/output.cpp -o main
```
