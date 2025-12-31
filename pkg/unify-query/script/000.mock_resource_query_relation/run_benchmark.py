#!/usr/bin/env python3
"""
总调度脚本 - 执行两个方案的基准测试并保存结果

使用方法:
    python run_benchmark.py                           # 运行两个方案的完整测试
    python run_benchmark.py --plan 00                 # 只运行方案00
    python run_benchmark.py --plan 01                 # 只运行方案01
    python run_benchmark.py --duration 120            # 设置测试时长
    python run_benchmark.py --concurrency 50          # 设置并发数
    python run_benchmark.py --init-data               # 先初始化数据再测试 (使用 004.import_real_data.py)
    python run_benchmark.py --init-data --data-count 10000  # 指定 Pod 数量
    python run_benchmark.py --compare                 # 运行测试后生成对比报告

结果存放位置:
    - 方案00: 00.plan_created_at/tests/{日期}/
    - 方案01: 01.plan_active_windows/tests/{日期}/
    - 对比报告: results/{日期}/
"""

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime
from pathlib import Path


# 使用相对路径
SCRIPT_DIR = Path(__file__).parent
PLAN_00_DIR = SCRIPT_DIR / "00.plan_created_at"
PLAN_01_DIR = SCRIPT_DIR / "01.plan_active_windows"
COMPARISON_DIR = SCRIPT_DIR / "results"


def get_date_str():
    """获取日期字符串 (YYYYMMDD)"""
    return datetime.now().strftime("%Y%m%d")


def get_plan_tests_dir(plan: str) -> Path:
    """获取方案的测试结果目录"""
    if plan == "00":
        return PLAN_00_DIR / "tests" / get_date_str()
    else:
        return PLAN_01_DIR / "tests" / get_date_str()


def ensure_tests_dir(plan: str) -> Path:
    """确保方案的测试结果目录存在"""
    tests_dir = get_plan_tests_dir(plan)
    tests_dir.mkdir(parents=True, exist_ok=True)
    return tests_dir


def ensure_comparison_dir() -> Path:
    """确保对比报告目录存在"""
    comparison_dir = COMPARISON_DIR / get_date_str()
    comparison_dir.mkdir(parents=True, exist_ok=True)
    return comparison_dir


def get_timestamp():
    """获取时间戳字符串"""
    return datetime.now().strftime("%H%M%S")


def get_result_filename(concurrency: int, read_ratio: int, suffix: str = "") -> str:
    """生成结果文件名，与现有命名规范保持一致"""
    # 命名规范: result_{concurrency}.json 或 result_rw{read_ratio}_{concurrency}.json
    if read_ratio == 70:
        # 默认读写比例，使用简单命名
        return f"result_{concurrency}{suffix}.json"
    else:
        # 非默认读写比例，包含读写比例信息
        return f"result_rw{read_ratio}_{concurrency}{suffix}.json"


def run_command(cmd: list, cwd: Path = None, capture_output: bool = False):
    """运行命令"""
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"Working dir: {cwd or os.getcwd()}")
    print(f"{'='*60}\n")
    
    if capture_output:
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        return result
    else:
        result = subprocess.run(cmd, cwd=cwd)
        return result


def init_data(plan: str, count: int = 10000):
    """初始化测试数据 (使用 004.import_real_data.py)"""
    if plan == "00":
        plan_dir = PLAN_00_DIR
    else:
        plan_dir = PLAN_01_DIR
    
    print(f"\n{'#'*60}")
    print(f"# Initializing data for Plan {plan}")
    print(f"# Pod count: {count}")
    print(f"{'#'*60}")
    
    # 使用 004.import_real_data.py 导入模拟数据
    cmd = ["python3", "004.import_real_data.py", "--count", str(count)]
    result = run_command(cmd, cwd=plan_dir)
    if result.returncode != 0:
        print(f"Error: Data import failed with code {result.returncode}")
        return False
    
    return True


def run_benchmark(
    plan: str,
    duration: int,
    concurrency: int,
    read_ratio: int,
    heartbeat_ratio: int,
    output_file: Path
):
    """运行单个方案的基准测试"""
    if plan == "00":
        plan_dir = PLAN_00_DIR
        plan_name = "Plan 00: created_at"
    else:
        plan_dir = PLAN_01_DIR
        plan_name = "Plan 01: active_windows"
    
    print(f"\n{'#'*60}")
    print(f"# Running Benchmark: {plan_name}")
    print(f"# Duration: {duration}s, Concurrency: {concurrency}")
    print(f"# Read Ratio: {read_ratio}%, Heartbeat Ratio: {heartbeat_ratio}%")
    print(f"# Output: {output_file}")
    print(f"{'#'*60}")
    
    # 使用相对路径作为输出文件参数
    # benchmark 脚本在 plan_dir 下运行，输出文件相对于 plan_dir
    relative_output = os.path.relpath(output_file, plan_dir)
    
    cmd = [
        "python3", "003.benchmark_surrealdb.py",
        "--duration", str(duration),
        "--concurrency", str(concurrency),
        "--read-ratio", str(read_ratio),
        "--heartbeat-ratio", str(heartbeat_ratio),
        "--output", relative_output
    ]
    
    result = run_command(cmd, cwd=plan_dir)
    
    if result.returncode != 0:
        print(f"Error: Benchmark failed with code {result.returncode}")
        return False
    
    return True


def load_result(filepath: Path) -> dict:
    """加载测试结果"""
    if not filepath.exists():
        return None
    with open(filepath, 'r') as f:
        return json.load(f)


def generate_comparison_report(result_00: dict, result_01: dict, output_file: Path):
    """生成对比报告"""
    report = {
        "generated_at": datetime.now().isoformat(),
        "plans": {
            "plan_00": "created_at (Function-based upsert)",
            "plan_01": "active_windows (UPSERT MERGE + Event)"
        },
        "comparison": {}
    }
    
    # 对比 QPS
    qps_00 = result_00.get("summary", {}).get("qps", 0)
    qps_01 = result_01.get("summary", {}).get("qps", 0)
    qps_diff = qps_01 - qps_00
    qps_diff_pct = (qps_diff / qps_00 * 100) if qps_00 > 0 else 0
    
    report["comparison"]["qps"] = {
        "plan_00": qps_00,
        "plan_01": qps_01,
        "diff": round(qps_diff, 2),
        "diff_percent": round(qps_diff_pct, 2),
        "winner": "plan_01" if qps_01 > qps_00 else "plan_00"
    }
    
    # 对比延迟
    latency_00 = result_00.get("latency", {}).get("overall", {})
    latency_01 = result_01.get("latency", {}).get("overall", {})
    
    report["comparison"]["latency"] = {
        "p50": {
            "plan_00": latency_00.get("p50", 0),
            "plan_01": latency_01.get("p50", 0),
            "winner": "plan_01" if latency_01.get("p50", 0) < latency_00.get("p50", 0) else "plan_00"
        },
        "p95": {
            "plan_00": latency_00.get("p95", 0),
            "plan_01": latency_01.get("p95", 0),
            "winner": "plan_01" if latency_01.get("p95", 0) < latency_00.get("p95", 0) else "plan_00"
        },
        "p99": {
            "plan_00": latency_00.get("p99", 0),
            "plan_01": latency_01.get("p99", 0),
            "winner": "plan_01" if latency_01.get("p99", 0) < latency_00.get("p99", 0) else "plan_00"
        },
        "avg": {
            "plan_00": latency_00.get("avg", 0),
            "plan_01": latency_01.get("avg", 0),
            "winner": "plan_01" if latency_01.get("avg", 0) < latency_00.get("avg", 0) else "plan_00"
        }
    }
    
    # 对比心跳延迟
    hb_00 = result_00.get("latency", {}).get("heartbeat", {})
    hb_01 = result_01.get("latency", {}).get("heartbeat", {})
    
    report["comparison"]["heartbeat_latency"] = {
        "p50": {
            "plan_00": hb_00.get("p50", 0),
            "plan_01": hb_01.get("p50", 0),
            "winner": "plan_01" if hb_01.get("p50", 0) < hb_00.get("p50", 0) else "plan_00"
        },
        "p95": {
            "plan_00": hb_00.get("p95", 0),
            "plan_01": hb_01.get("p95", 0),
            "winner": "plan_01" if hb_01.get("p95", 0) < hb_00.get("p95", 0) else "plan_00"
        },
        "p99": {
            "plan_00": hb_00.get("p99", 0),
            "plan_01": hb_01.get("p99", 0),
            "winner": "plan_01" if hb_01.get("p99", 0) < hb_00.get("p99", 0) else "plan_00"
        }
    }
    
    # 对比操作数
    ops_00 = result_00.get("operations", {})
    ops_01 = result_01.get("operations", {})
    
    report["comparison"]["operations"] = {
        "total_ops": {
            "plan_00": ops_00.get("total_ops", 0),
            "plan_01": ops_01.get("total_ops", 0)
        },
        "total_reads": {
            "plan_00": ops_00.get("total_reads", 0),
            "plan_01": ops_01.get("total_reads", 0)
        },
        "total_writes": {
            "plan_00": ops_00.get("total_writes", 0),
            "plan_01": ops_01.get("total_writes", 0)
        },
        "errors": {
            "plan_00": ops_00.get("errors", 0),
            "plan_01": ops_01.get("errors", 0)
        }
    }
    
    # 成功率
    sr_00 = result_00.get("summary", {}).get("success_rate", 0)
    sr_01 = result_01.get("summary", {}).get("success_rate", 0)
    
    report["comparison"]["success_rate"] = {
        "plan_00": sr_00,
        "plan_01": sr_01,
        "winner": "plan_01" if sr_01 > sr_00 else "plan_00"
    }
    
    # 原始结果
    report["raw_results"] = {
        "plan_00": result_00,
        "plan_01": result_01
    }
    
    # 保存报告
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\nComparison report saved to: {output_file}")
    
    # 打印摘要
    print_comparison_summary(report)
    
    return report


def print_comparison_summary(report: dict):
    """打印对比摘要"""
    comp = report["comparison"]
    
    print("\n" + "=" * 70)
    print("BENCHMARK COMPARISON SUMMARY")
    print("=" * 70)
    print(f"Plan 00: created_at (Function-based upsert)")
    print(f"Plan 01: active_windows (UPSERT MERGE + Event)")
    print("-" * 70)
    
    # QPS
    qps = comp["qps"]
    print(f"\nQPS (ops/sec):")
    print(f"  Plan 00: {qps['plan_00']:,.1f}")
    print(f"  Plan 01: {qps['plan_01']:,.1f}")
    print(f"  Diff:    {qps['diff']:+,.1f} ({qps['diff_percent']:+.1f}%)")
    print(f"  Winner:  {qps['winner']}")
    
    # 延迟
    lat = comp["latency"]
    print(f"\nOverall Latency (ms):")
    print(f"  {'Metric':<8} {'Plan 00':>12} {'Plan 01':>12} {'Winner':>12}")
    print(f"  {'-'*8} {'-'*12} {'-'*12} {'-'*12}")
    for metric in ["p50", "p95", "p99", "avg"]:
        print(f"  {metric:<8} {lat[metric]['plan_00']:>12.2f} {lat[metric]['plan_01']:>12.2f} {lat[metric]['winner']:>12}")
    
    # 心跳延迟
    hb = comp["heartbeat_latency"]
    print(f"\nHeartbeat Latency (ms):")
    print(f"  {'Metric':<8} {'Plan 00':>12} {'Plan 01':>12} {'Winner':>12}")
    print(f"  {'-'*8} {'-'*12} {'-'*12} {'-'*12}")
    for metric in ["p50", "p95", "p99"]:
        print(f"  {metric:<8} {hb[metric]['plan_00']:>12.2f} {hb[metric]['plan_01']:>12.2f} {hb[metric]['winner']:>12}")
    
    # 成功率
    sr = comp["success_rate"]
    print(f"\nSuccess Rate:")
    print(f"  Plan 00: {sr['plan_00']:.2f}%")
    print(f"  Plan 01: {sr['plan_01']:.2f}%")
    
    # 操作数
    ops = comp["operations"]
    print(f"\nOperations:")
    print(f"  {'Type':<15} {'Plan 00':>12} {'Plan 01':>12}")
    print(f"  {'-'*15} {'-'*12} {'-'*12}")
    print(f"  {'Total Ops':<15} {ops['total_ops']['plan_00']:>12,} {ops['total_ops']['plan_01']:>12,}")
    print(f"  {'Total Reads':<15} {ops['total_reads']['plan_00']:>12,} {ops['total_reads']['plan_01']:>12,}")
    print(f"  {'Total Writes':<15} {ops['total_writes']['plan_00']:>12,} {ops['total_writes']['plan_01']:>12,}")
    print(f"  {'Errors':<15} {ops['errors']['plan_00']:>12,} {ops['errors']['plan_01']:>12,}")
    
    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Run benchmark for both plans and compare results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_benchmark.py                         # Run both plans
  python run_benchmark.py --plan 00               # Run only Plan 00
  python run_benchmark.py --plan 01               # Run only Plan 01
  python run_benchmark.py --init-data             # Initialize data (10000 pods) before benchmark
  python run_benchmark.py --init-data --data-count 5000  # Initialize with 5000 pods
  python run_benchmark.py --duration 120          # 2 minute test
  python run_benchmark.py --concurrency 50        # 50 concurrent workers
  python run_benchmark.py --compare               # Generate comparison report

Output locations:
  Plan 00 results: 00.plan_created_at/tests/{date}/
  Plan 01 results: 01.plan_active_windows/tests/{date}/
  Comparison:      results/{date}/
        """
    )
    
    parser.add_argument('--plan', type=str, choices=['00', '01', 'both'], default='both',
                        help='Which plan to run (default: both)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Test duration in seconds (default: 60)')
    parser.add_argument('--concurrency', type=int, default=20,
                        help='Number of concurrent workers (default: 20)')
    parser.add_argument('--read-ratio', type=int, default=70,
                        help='Read operation ratio 0-100 (default: 70)')
    parser.add_argument('--heartbeat-ratio', type=int, default=80,
                        help='Heartbeat ratio in write operations 0-100 (default: 80)')
    parser.add_argument('--init-data', action='store_true',
                        help='Initialize test data before running benchmark')
    parser.add_argument('--data-count', type=int, default=10000,
                        help='Number of pods to generate for test data (default: 10000)')
    parser.add_argument('--compare', action='store_true',
                        help='Generate comparison report after benchmark')
    parser.add_argument('--suffix', type=str, default='',
                        help='Suffix to append to result filename')
    
    args = parser.parse_args()
    
    # 确定要运行的方案
    plans_to_run = []
    if args.plan == 'both':
        plans_to_run = ['00', '01']
    else:
        plans_to_run = [args.plan]
    
    # 初始化数据（如果需要）
    if args.init_data:
        for plan in plans_to_run:
            if not init_data(plan, args.data_count):
                print(f"Failed to initialize data for Plan {plan}")
                return 1
    
    # 生成结果文件名
    result_filename = get_result_filename(args.concurrency, args.read_ratio, args.suffix)
    
    # 运行基准测试
    result_files = {}
    for plan in plans_to_run:
        # 确保测试目录存在
        tests_dir = ensure_tests_dir(plan)
        output_file = tests_dir / result_filename
        
        success = run_benchmark(
            plan=plan,
            duration=args.duration,
            concurrency=args.concurrency,
            read_ratio=args.read_ratio,
            heartbeat_ratio=args.heartbeat_ratio,
            output_file=output_file
        )
        if success:
            result_files[plan] = output_file
        else:
            print(f"Warning: Benchmark for Plan {plan} failed")
    
    # 生成对比报告（如果需要且两个方案都运行了）
    if args.compare and '00' in result_files and '01' in result_files:
        result_00 = load_result(result_files['00'])
        result_01 = load_result(result_files['01'])
        
        if result_00 and result_01:
            comparison_dir = ensure_comparison_dir()
            comparison_filename = f"comparison_{result_filename}"
            comparison_file = comparison_dir / comparison_filename
            generate_comparison_report(result_00, result_01, comparison_file)
    
    # 打印结果文件位置
    print(f"\n{'='*60}")
    print("BENCHMARK COMPLETED")
    print(f"{'='*60}")
    print(f"Date: {get_date_str()}")
    for plan, filepath in result_files.items():
        relative_path = os.path.relpath(filepath, SCRIPT_DIR)
        print(f"  Plan {plan}: {relative_path}")
    if args.compare and len(result_files) == 2:
        comparison_path = os.path.relpath(comparison_dir / comparison_filename, SCRIPT_DIR)
        print(f"  Comparison: {comparison_path}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
