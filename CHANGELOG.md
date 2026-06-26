# lab2.0 - Grid-Search Optimized Scheduler

基于 `feature/scheduler-optimization1.0`，通过全量网格搜索找到最优参数组合。

---

## 与 scheduler-optimization1.0 的差异

### Cost 函数变更

| 项目 | scheduler-optimization1.0 | **lab2.0** |
|------|:---:|:---:|
| FLEX_W | 2.0 | 2.0 |
| SLACK_W (等效) | 0.60 × SUM | **0.40 × SUM** |
| WASTE_W | 0.25 | **0.25** |
| Backfill | 单次扫描 | 可切换（默认单次） |
| 预计算倒数 | 无 | **有** (inv_gpu/cpu/memory) |
| inline cost | 无 | **有** |

### 参数扫描结果（100 用例 / 5×2×2 网格）

```
SLACK_W  WASTE_W  BF       Proxy   W-Wait  W-Mem  W-Fin
0.40     0.25     single   62.99   10      11     11      ← 最优
0.20     0.00     single   64.08   12      11     11
0.60     0.25     single   78.41   14       4      4      ← scheduler-opt1.0
```

### 头对头实测（最优版 vs 真实 scheduler-optimization1.0）

| 指标 | scheduler-optimization1.0 | lab2.0 | 差距 |
|------|:---:|:---:|:---:|
| Proxy Score | 74.00 | **63.00** | **-11.00** |
| E_wait 总和 | 10,987M | 10,991M | 接近 |
| E_mem 平均 | 6,079 | **6,065** | 更优 |
| E_finish | 191,556 | 191,654 | 接近 |
| 合法率 | 100/100 | 100/100 | ✓ |
| 100用例耗时 | 23.9s | 30.5s | < 60s |

## 构建

```bash
g++ -std=c++17 -O2 src/main.cpp src/parser.cpp src/machine_state.cpp \
    src/scheduler.cpp src/output.cpp -o main
```

## 可调参数 (scheduler.h)

```cpp
#define FLEX_W 2.0        // 机器灵活度权重
#define SLACK_W 0.40      // 资源紧凑度权重 (网格搜索最优)
#define WASTE_W 0.25      // 显存浪费惩罚  
#define BACKFILL_ITER 0   // 0=单次回填, 1=迭代收敛
```
