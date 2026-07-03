# lab4.0 CHANGELOG

## 相比 lab3.0 的核心改进

### 1. 架构升级：从单策略到多策略 meta-scheduler

**lab3.0**：程序用一组固定参数生成单次调度并输出。

**lab4.0**：程序先用 **12 组不同参数组合** 生成多个候选调度解，然后对每个实例在候选池内做 **内部 min-max 归一化评分**，选择最优输出。

```
lab3.0:  输入 ──► [单次调度(固定参数)] ──► 输出

lab4.0:  输入 ──► [调度1(参数A)]
                  [调度2(参数B)]
                  ...
                  [调度12(参数L)] ──► 候选池归一化评分 ──► 最优输出
```

### 2. 参数系统从编译期改为运行时

| lab3.0 | lab4.0 |
|--------|--------|
| `#define AGE_W 0.4` 等编译期宏 | `SchedulerParams` 运行时结构体 |
| 每改一个参数需重新编译 | 同一份二进制支持任意参数 |
| ReadyJobCompare 无状态 | ReadyJobCompare 持 params 指针 |

### 3. 候选解选择评分

```c
score = 1.25 × norm(E_wait) + 1.0 × norm(E_memory) + 1.0 × norm(E_finish)
```

三项指标在候选池内部各自做 min-max 归一化，使正式评分逻辑接近官方逐实例归一化方式。权重 1.25/1.0/1.0 在分析报告3中经验证优于等权。

### 4. 12 组策略参数覆盖

| 策略数 | 覆盖维 |
|--------|--------|
| 1 组 | lab3.0 最优参数 (AG=0.4, CP=1.5) |
| 3 组 | 不同 aging 强度 (0 / 0.2 / 0.3) |
| 3 组 | 不同 cost premium 激进程度 (1.1 / 2.0 / 3.0) |
| 2 组 | 不同 MEM ratio (0.7 / 1.5) |
| 1 组 | 纯 WSPT 基线（无 aging、无 mem-trade-off） |
| 2 组 | 短任务偏好 (DURATION_EXP=1.1) |

---

## 评测结果（100 例，修正后评分公式）

| 版本 | Proxy | E_wait | E_memory | E_finish | 耗时 |
|---|:---:|---|---|---|---|
| **lab4.0** | **29.02** | **10,976M** | **2,823** | **2,250K** | 198s |
| lab3.0 | 93.94 | 10,987M | 3,003 | 2,257K | 23s |
| Fusion-v2 | 107.86 | 10,991M | 2,875 | 2,263K | 25s |
| lab2.0 | 150.03 | 10,990M | 3,527 | 2,259K | 14s |

- Proxy 相比 lab3.0 提升 **65 分** (93→29)，相比 lab2.0 提升 **121 分** (150→29)
- 三项原始指标**全部最优**
- 100/100 合法，单例最大耗时 ~10s，远低于 60s 限制

---

## 修改的文件

| 文件 | 变更 |
|------|------|
| `src/scheduler.h` | 新增 `SchedulerParams` 结构体；`ReadyJobCompare` 持 params 指针；`SchedulerEngine` 构造函数新增 params 参数 |
| `src/scheduler.cpp` | 参数引用改为 `params.xxx`；`schedule()` 中 priority_queue、比较器、priority 计算均使用运行时参数 |
| `src/main.cpp` | 完全重写：12 策略候选池、调度执行、内部归一化评分、最优解选择 |
| `src/models.h` | 不变 |
| `src/machine_state.*` | 不变 |
| `src/parser.*` | 不变 |
| `src/output.*` | 不变 |
| `run.sh` | 不变 |
| `build.sh` | 不变 |
