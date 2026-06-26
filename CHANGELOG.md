# lab2.0 - Grid-Search Optimized Scheduler

基于 lab1.0，通过全量网格搜索找到最优参数配置。

---

## 与新评分公式的适配

2026-06-26 课程修正了 E_memory 公式：

**旧公式**：`E_memory = (1/H) × Σ_t Σ_s (G_s·VG_s - Σ v_i)`  
**新公式**：`E_memory = Σ_i p_i × (u_i·VG_si - v_i)`

新公式去掉了除以 H 的归一化，使 E_memory 和 E_finish 不再重叠。**按新公式重新网格扫描**，结果如下：

```
SLACK_W   WASTE_W      Proxy   W-Wait  W-Mem  W-Fin
0.20      0.25       64.43   19      48     10      ← 最优
0.30      0.25       72.45   10      19     10
0.40      0.25       78.03   10      16     11
0.20      0.00       102.6   12       6     11
```

**结论：WASTE_W=0.25 在新公式下全面碾压 WASTE_W=0.0（因为它直接衡量了每个任务的显存浪费）。SLACK_W 回到 0.20。**

---

## 与 lab1.0 相比的变更

| 参数 | lab1.0 | lab2.0 | 说明 |
|------|:---:|:---:|------|
| FLEX_W | 2.0 | 2.0 | 不变 |
| SLACK_W（等效） | 0.20（`0.60 × avg`） | **0.20**（`0.20 × SUM`） | 语义统一，权重等效 |
| WASTE_W | 0.25 | **0.25** | 保留（新公式下必需） |
| Backfill | 迭代收敛 | **单次** | 网格扫描证实无额外收益 |
| 预计算倒数 | 有 | 有 | 保留 |
| inline cost | 有 | 有 | 保留 |

### 可调参数

```cpp
#define FLEX_W 2.0        // 机器灵活度权重
#define SLACK_W 0.20      // 资源紧凑度权重（新公式网格扫描最优）
#define WASTE_W 0.25      // 显存浪费惩罚（新公式下必需）
#define BACKFILL_ITER 0   // 0=单次回填
```

## 构建

```bash
g++ -std=c++17 -O2 main.cpp parser.cpp machine_state.cpp scheduler.cpp output.cpp -o main
```
