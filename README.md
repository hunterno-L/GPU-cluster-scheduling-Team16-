# GPU-Team16 调度器

GPU 服务器任务调度课程设计项目（CMake + C++17）。

## 目录结构

```
GPU-Team16/
├── src/              # 源代码（提交用）
├── scripts/          # 批量评测脚本
├── tests/            # 本地调试/回归测试（不参与提交编译）
├── docs/             # 实验报告与题目文档摘录
├── results/          # 最新评测结果
│   └── archive/      # 历史评测记录
├── build/            # 编译输出（run.sh 调用 build/execname）
├── build.sh          # 编译脚本
├── run.sh            # 运行脚本
└── CMakeLists.txt
```

## 快速开始

```bash
bash build.sh
bash run.sh < 输入文件.in > 输出.out
```

Windows 本地调试：

```powershell
g++ -std=c++17 -O2 src/main.cpp src/parser.cpp src/machine_state.cpp src/scheduler.cpp src/output.cpp -o build/execname.exe
Get-Content 输入.in | ./build/execname.exe > 输出.out
```

## 批量评测

数据集默认路径：`../02-数据集`（与上级目录 `02-数据集` 对应）。

```bash
python scripts/evaluate.py --exe build/execname.exe --timeout 5 > results/eval_summary.csv
python scripts/evaluate_metrics.py --exe build/execname.exe --timeout 10 > results/eval_metrics.csv
python scripts/test_parallel_start.py build/execname.exe
```

## 成员分工

- **成员 A**：任务排序与等待时间优化
- **成员 B**：服务器放置、评测可信度与实验报告（见 `docs/EXPERIMENT_REPORT.md`）
