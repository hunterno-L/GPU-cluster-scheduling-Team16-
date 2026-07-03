# GPU Scheduler 实验报告 - 成员B

## 实验信息
- **角色**: 成员B（资源放置与评测可信度负责人）
- **分支**: `feature/member-b-evaluation`
- **数据集**: case001-case100

## 实验卡
| 项目 | 内容 |
|------|------|
| **假设** | 高显存稀缺服务器保护 + 显存紧配放置 + 同刻并行装填，可降低官方三项指标 |
| **对照** | 仅贪心、无合法性校验的版本 |
| **指标** | 合法率、加权等待、GPU显存空闲、完工时间、单case最长运行时间 |

## 成员B核心优化（服务器放置）
1. **显存紧配**：枚举可行 GPU 数量，优先选择浪费最少的分配
2. **稀缺服务器保护**：80GB 服务器惩罚小任务占用，奖励大显存任务
3. **碎片与负载均衡**：CPU/内存/GPU 碎片评分 + 全局负载缓存
4. **同刻并行装填**：同一时刻持续放置直到资源耗尽
5. **解选择策略**：覆盖率优先，其次按官方三项指标综合评分

## 评测命令
```bash
g++ -std=c++17 -O2 src/main.cpp src/parser.cpp src/machine_state.cpp src/scheduler.cpp src/output.cpp -o build/execname.exe
python scripts/evaluate.py --exe build/execname.exe --timeout 5 > results/eval_summary.csv
python scripts/evaluate_metrics.py --exe build/execname.exe --timeout 10 > results/eval_metrics.csv
```

## 实验结果（修复后最新）
| 指标 | 结果 |
|------|------|
| 合法率 | **100/100** |
| 超时 | **0** |
| 全量评测 | ~20s |
| 单 case 上限 | <60s（课程要求） |

## 结论
**合并** — 全量合法、时限合格；放置策略对齐官方三项评分（加权等待、显存空闲、完工时间）。
