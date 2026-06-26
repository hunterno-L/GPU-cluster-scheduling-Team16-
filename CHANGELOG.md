# lab2.0 - Grid-Search Optimized Scheduler

基于 lab1.0，通过三轮网格搜索找到最优参数配置。

---

## 与新评分公式的适配

2026-06-26 课程修正了 E_memory 公式：

**旧公式**：`E_memory = (1/H) × Σ_t Σ_s (G_s·VG_s - Σ v_i)`  
**新公式**：`E_memory = Σ_i p_i × (u_i·VG_si - v_i)`

新公式去掉了除以 H 的归一化，使 E_memory 和 E_finish 不再重叠。**按新公式三轮网格扫描**：

第一轮（粗扫 0.20~0.60）→ 最优 0.20  
第二轮（细扫 0.05~0.30）→ 最优 0.05  
第三轮（边界 0.00~0.05）→ **最优 0.00**

```
SLACK_W   WASTE_W      Proxy   W-Wait  W-Mem  W-Fin
0.00      0.25       64.01   20      37     11      ← 最终最优
0.05      0.25       63.17   19      39     10
0.10      0.25       73.06   14      14     12
0.20      0.25       96.52   13      10      7
0.00      0.00       79.95   22      14     14
```

**结论**：新公式下 SLACK_W=0.00（完全不考虑紧凑度），纯 flexibility + waste 选机器最优。WASTE_W=0.25 必不可少。

---

## 与 lab1.0 相比的变更

| 参数 | lab1.0 | lab2.0 | 说明 |
|------|:---:|:---:|------|
| FLEX_W | 2.0 | 2.0 | 不变 |
| SLACK_W | 0.60×avg(等效0.20) | **0.00** | 新公式下紧凑度无收益，关闭 |
| WASTE_W | 0.25 | **0.25** | 保留（新公式直接衡量显存浪费） |
| Backfill | 迭代收敛 | **单次** | 网格扫描证实无额外收益 |
| 预计算倒数 | 有 | 有 | 保留 |
| inline cost | 有 | 有 | 保留 |

### 可调参数

```cpp
#define FLEX_W 2.0        // 机器灵活度权重
#define SLACK_W 0.00      // 资源紧凑度权重（关闭）
#define WASTE_W 0.25      // 显存浪费惩罚
#define BACKFILL_ITER 0   // 0=单次回填
```

### 实测结果（vs scheduler-optimization1.0 baseline）

```
Version          Proxy   W-Wait  W-Mem  W-Fin
baseline        132.00   35      16     16
our-best         67.00   24      77     31    ← 大胜65分
```

100/100 合法，总耗时 10.2s，单例最大 0.99s。

## 构建

```bash
g++ -std=c++17 -O2 main.cpp parser.cpp machine_state.cpp scheduler.cpp output.cpp -o main
```
