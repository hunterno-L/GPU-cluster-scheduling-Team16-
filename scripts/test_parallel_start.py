from __future__ import annotations

import subprocess
import sys
from pathlib import Path


INPUT = """2 2
1 80 32 128
1 80 32 128
0 10 1 40 1 1 1
0 10 1 40 1 1 1
"""


def main() -> int:
    exe = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("build/execname_opt.exe")
    proc = subprocess.run(
        [str(exe)],
        input=INPUT.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        print(f"runtime error: {proc.returncode}")
        return 1

    lines = [line.strip() for line in proc.stdout.decode().splitlines() if line.strip()]
    if len(lines) != 2:
        print(f"expected 2 records, got {len(lines)}")
        print(proc.stdout.decode())
        return 1

    starts = [int(line.split()[2]) for line in lines]
    if starts != [0, 0]:
        print(f"expected both jobs to start at 0, got {starts}")
        print(proc.stdout.decode())
        return 1

    print("parallel-start test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

