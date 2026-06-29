# lab3.0 CHANGELOG

## 相比 lab2.0 的改动

### 1. 新增 Part-A 调度顺序参数（`#define` 可配置，默认关闭）

| 参数 | 默认值 | 作用 |
|---|:---:|---|
| `AGE_W` | 0.0 | 等待时间老化权重。job 等待越久优先级提升越多，使用 log 衰减防止饥饿 |
| `DURATION_EXP` | 1.0 | WSPT 中 duration 的指数。>1 更强偏好短任务，=1 即标准 WSPT |
| `HIGH_PRIORITY_BOOST` | 0.0 | 最高权重 job 的额外保护力度 |

实现位置：`ReadyJobCompare::operator()` 中计算 effective priority。

### 2. 新增 Part-B 选机策略：mem-trade-off（`#define` 可配置，默认开启）

| 参数 | 默认值 | 作用 |
|---|:---:|---|
| `MEM_TRADE_OFF_RATIO` | 1.00 | 替换当前最优机器的阈值：候选机器的显存浪费 < 最优机器的浪费 × ratio |
| `COST_PREMIUM_RATIO` | 2.00 | 允许替换的最高 cost 溢价倍率：候选机器的 cost ≤ 最优机器的 cost × ratio |

实现位置：`tryOne()` 中二阶段选机逻辑。第一阶段按 cost 选最优机器，第二阶段在 cost 容忍范围内寻找更低显存浪费的替代机器。

### 3. 修复 waiting_time 更新 bug

lab2.0 无 aging 功能。Fusion 原始版本中 aging 的 `waiting_time` 除以 100 且 backfill 回推时不更新。lab3.0 已修复：
- `waiting_time = current_time - release_time`（精确值，不除 100）
- 每次 job 回推到 pending 队列时重新计算 waiting_time

### 4. `schedule()` 中的 priority 计算

从 `weight / duration` 改为 `weight / pow(duration, DURATION_EXP)`，默认值 1.0 时与 lab2.0 行为完全一致。

---

## 参数调优结果（100 例，修正后的评分公式）

| 版本 | Proxy | E_wait | E_memory | E_finish |
|---|:---:|---|---|---|
| lab2.0 (基线) | 136.89 | 10,990M | 3,527 | 2,259K |
| A1b (AGE=0.3) | 126.45 | 10,987M | 3,530 | 2,258K |
| Fusion-v2 原版 | 93.29 | 10,991M | 2,875 | 2,263K |
| **lab3.0 最优 (AGE=0.4, CP=1.5)** | **85.16** | 10,987M | 3,003 | 2,257K |

### 默认参数行为

所有新参数默认值保证与 lab2.0 行为一致：
- `AGE_W=0.0` → aging 短路跳过
- `DURATION_EXP=1.0` → priority 同 lab2.0
- `HIGH_PRIORITY_BOOST=0.0` → 无额外加成
- `MEM_TRADE_OFF_RATIO=1.00, COST_PREMIUM_RATIO=2.00` → mem-trade-off 生效

### 兼容性

修改仅涉及 `scheduler.h` 和 `scheduler.cpp`，`machine_state.*`、`parser.*`、`output.*`、`models.h`、`main.cpp` 与 lab2.0 完全相同。
