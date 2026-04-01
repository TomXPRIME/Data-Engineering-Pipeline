"""
SPX Pipeline 快速测试脚本

运行方式:
    python test_pipeline.py

用途: 清理旧数据 -> Simulator (小样本) -> Ingestion -> ELT -> Gold Build -> 验证

测试范围: 2024-01-02 ~ 2024-01-31 (约20个交易日)
预计耗时: 5-10 分钟
"""

import subprocess
import sys
from pathlib import Path

PYTHON = "C:/miniconda3/envs/qf5214_project/python.exe"
REPO_ROOT = Path(__file__).parent.resolve()

CLEANUP = (
    PYTHON,
    "-c",
    (
        "import shutil; "
        "from pathlib import Path; "
        "[shutil.rmtree(p, ignore_errors=True) for p in ['output/landing_zone', 'output/silver']]; "
        "[Path(p).unlink(missing_ok=True) for p in ['duckdb/spx_analytics.duckdb', 'output/.watermark']]; "
        "print('Cleaned up')"
    ),
)

CMDS = [
    # 1. 清理旧数据
    (CLEANUP, "清理旧数据"),

    # 2. 初始化 Bronze 表
    ((PYTHON, "duckdb/init_bronze.py"), "初始化 Bronze 表"),

    # 3. Simulator (2024年1月，约20交易日)
    ((PYTHON, "-m", "pipeline.simulators.comprehensive_simulator",
      "--mode", "backfill", "--start", "2024-01-02", "--end", "2024-01-31"), "Simulator"),

    # 4. Ingestion Engine
    ((PYTHON, "-m", "pipeline.ingestion_engine", "--mode", "scan"), "Ingestion Engine"),

    # 5. ELT Pipeline
    ((PYTHON, "-m", "pipeline.elt_pipeline", "--resource", "price"), "ELT: Price"),
    ((PYTHON, "-m", "pipeline.elt_pipeline", "--resource", "fundamentals"), "ELT: Fundamentals"),
    ((PYTHON, "-m", "pipeline.elt_pipeline", "--resource", "transcripts"), "ELT: Transcripts"),
    ((PYTHON, "-m", "pipeline.elt_pipeline", "--resource", "sentiment"), "ELT: Sentiment"),

    # 6. Gold Layer Build
    ((PYTHON, "gold/build_gold_layer.py"), "Gold Layer Build"),
]


def run_cmd(cmd: tuple, label: str) -> bool:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode == 0:
        print(f"  [OK] {label} 完成")
        return True
    else:
        print(f"  [FAIL] {label} 失败 (exit {result.returncode})")
        return False


def main():
    print("=" * 60)
    print("  SPX Pipeline 快速测试")
    print("  测试范围: 2024-01-02 ~ 2024-01-31")
    print("  预计耗时: 5-10 分钟")
    print("=" * 60)

    results = {}
    for cmd, label in CMDS:
        ok = run_cmd(cmd, label)
        results[label] = "OK" if ok else "FAIL"
        if not ok:
            print(f"\n!!! {label} 失败，停止测试 !!!")
            break

    # 7. 验证 Gold 视图
    print(f"\n{'='*60}")
    print("  Gold 视图验证")
    print(f"{'='*60}")
    subprocess.run((PYTHON, "gold/tests/test_gold_views.py"), cwd=REPO_ROOT)

    # 汇总
    print(f"\n{'='*60}")
    print("  测试汇总")
    print(f"{'='*60}")
    for label, status in results.items():
        symbol = "[OK]" if status == "OK" else "[FAIL]"
        print(f"  {symbol} {label}")

    all_ok = all(s == "OK" for s in results.values())
    if all_ok:
        print(f"\n所有阶段测试通过!")
    else:
        print(f"\n部分阶段失败，请检查日志。")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
