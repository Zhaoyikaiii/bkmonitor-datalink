#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark: Redundant Fields vs Array Operations

比较使用冗余字段 (start_time, end_time, active_windows_len) 和
传统数组操作 (active_windows[-1].end_time, array::len()) 的查询性能差异。

Usage:
    python 005.benchmark_redundant_fields.py
    python 005.benchmark_redundant_fields.py --iterations 100
"""

import argparse
import json
import logging
import os
import statistics
import time
from dataclasses import dataclass
from typing import Dict, List, Any, Optional

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# Configuration
# ============================================================================

def _load_yaml_config(filename: str) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError:
        return {}
    if not os.path.exists(filename):
        return {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

_config_file = os.path.join(os.path.dirname(__file__), 'config.yaml')
_config = _load_yaml_config(_config_file)

SURREAL_URL = _config.get('surreal_db', {}).get('url', 'http://localhost:8000')
SURREAL_USER = _config.get('surreal_db', {}).get('username', 'root')
SURREAL_PASS = _config.get('surreal_db', {}).get('password', 'root')
SURREAL_NS = _config.get('surreal_db', {}).get('namespace', 'test')
SURREAL_DB = _config.get('surreal_db', {}).get('database', 'test')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# SurrealDB Client
# ============================================================================

class SurrealDBClient:
    def __init__(self):
        self.url = SURREAL_URL
        self.session = requests.Session()
        self.session.auth = (SURREAL_USER, SURREAL_PASS)
        self.session.headers.update({
            'Accept': 'application/json',
            'NS': SURREAL_NS,
            'DB': SURREAL_DB,
        })
    
    def execute_sql(self, sql: str) -> Optional[List[Dict]]:
        try:
            resp = self.session.post(
                f"{self.url}/sql",
                data=sql,
                headers={'Content-Type': 'text/plain'},
                timeout=30,
                verify=False
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.error(f"SQL error: {resp.status_code} - {resp.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None
    
    def get_table_count(self, table: str) -> int:
        result = self.execute_sql(f"SELECT count() FROM {table} GROUP ALL;")
        if result and result[0].get('result'):
            return result[0]['result'][0].get('count', 0)
        return 0


# ============================================================================
# Benchmark Queries
# ============================================================================

@dataclass
class QueryPair:
    """一对查询：冗余字段版本 vs 数组操作版本"""
    name: str
    description: str
    redundant_sql: str  # 使用冗余字段的查询
    array_sql: str      # 使用数组操作的查询


QUERY_PAIRS = [
    QueryPair(
        name="active_check",
        description="查询当前活跃的资源",
        redundant_sql="SELECT id, pod FROM pod WHERE end_time = NONE LIMIT 100;",
        array_sql="SELECT id, pod FROM pod WHERE active_windows[-1].end_time = NONE LIMIT 100;"
    ),
    QueryPair(
        name="inactive_check",
        description="查询已关闭的资源",
        redundant_sql="SELECT id, pod FROM pod WHERE end_time != NONE LIMIT 100;",
        array_sql="SELECT id, pod FROM pod WHERE active_windows[-1].end_time != NONE LIMIT 100;"
    ),
    QueryPair(
        name="multi_window_check",
        description="查询有多个活跃窗口的资源",
        redundant_sql="SELECT id, pod FROM pod WHERE active_windows_len > 1 LIMIT 100;",
        array_sql="SELECT id, pod FROM pod WHERE array::len(active_windows) > 1 LIMIT 100;"
    ),
    QueryPair(
        name="single_window_check",
        description="查询只有一个窗口的资源",
        redundant_sql="SELECT id, pod FROM pod WHERE active_windows_len = 1 LIMIT 100;",
        array_sql="SELECT id, pod FROM pod WHERE array::len(active_windows) = 1 LIMIT 100;"
    ),
    QueryPair(
        name="first_seen_time",
        description="获取资源首次上报时间",
        redundant_sql="SELECT id, pod, start_time AS first_seen FROM pod LIMIT 100;",
        array_sql="SELECT id, pod, active_windows[0].start_time AS first_seen FROM pod LIMIT 100;"
    ),
    QueryPair(
        name="lifecycle_info",
        description="获取完整生命周期信息",
        redundant_sql="""
            SELECT id, pod, start_time, end_time, active_windows_len,
                IF end_time = NONE THEN 'active' ELSE 'inactive' END AS status
            FROM pod LIMIT 100;
        """,
        array_sql="""
            SELECT id, pod, 
                active_windows[0].start_time AS start_time,
                active_windows[array::len(active_windows) - 1].end_time AS end_time,
                array::len(active_windows) AS active_windows_len,
                IF active_windows[array::len(active_windows) - 1].end_time = NONE THEN 'active' ELSE 'inactive' END AS status
            FROM pod LIMIT 100;
        """
    ),
    QueryPair(
        name="gap_count",
        description="统计上报中断次数",
        redundant_sql="SELECT id, pod, active_windows_len - 1 AS gap_count FROM pod ORDER BY gap_count DESC LIMIT 50;",
        array_sql="SELECT id, pod, array::len(active_windows) - 1 AS gap_count FROM pod ORDER BY gap_count DESC LIMIT 50;"
    ),
    QueryPair(
        name="active_relation_check",
        description="查询活跃的关系",
        redundant_sql="SELECT id, in, out FROM node_with_pod WHERE end_time = NONE LIMIT 100;",
        array_sql="SELECT id, in, out FROM node_with_pod WHERE active_windows[-1].end_time = NONE LIMIT 100;"
    ),
]


# ============================================================================
# Benchmark Runner
# ============================================================================

@dataclass
class BenchmarkResult:
    query_name: str
    description: str
    redundant_times_ms: List[float]
    array_times_ms: List[float]
    redundant_avg_ms: float
    array_avg_ms: float
    speedup: float  # array_avg / redundant_avg
    redundant_result_count: int
    array_result_count: int


class RedundantFieldsBenchmark:
    def __init__(self, iterations: int = 50):
        self.client = SurrealDBClient()
        self.iterations = iterations
        self.results: List[BenchmarkResult] = []
    
    def warmup(self):
        """预热数据库连接"""
        logger.info("Warming up...")
        for _ in range(5):
            self.client.execute_sql("SELECT * FROM pod LIMIT 1;")
        logger.info("Warmup complete")
    
    def run_query(self, sql: str) -> tuple[float, int]:
        """执行查询并返回耗时(ms)和结果数量"""
        start = time.perf_counter()
        result = self.client.execute_sql(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        count = 0
        if result and result[0].get('result'):
            count = len(result[0]['result'])
        
        return elapsed_ms, count
    
    def benchmark_query_pair(self, pair: QueryPair) -> BenchmarkResult:
        """对比测试一对查询"""
        logger.info(f"  Testing: {pair.name} - {pair.description}")
        
        redundant_times = []
        array_times = []
        redundant_count = 0
        array_count = 0
        
        for i in range(self.iterations):
            # 交替执行以减少缓存影响
            if i % 2 == 0:
                t1, c1 = self.run_query(pair.redundant_sql)
                t2, c2 = self.run_query(pair.array_sql)
            else:
                t2, c2 = self.run_query(pair.array_sql)
                t1, c1 = self.run_query(pair.redundant_sql)
            
            redundant_times.append(t1)
            array_times.append(t2)
            redundant_count = c1
            array_count = c2
        
        redundant_avg = statistics.mean(redundant_times)
        array_avg = statistics.mean(array_times)
        speedup = array_avg / redundant_avg if redundant_avg > 0 else 1.0
        
        return BenchmarkResult(
            query_name=pair.name,
            description=pair.description,
            redundant_times_ms=redundant_times,
            array_times_ms=array_times,
            redundant_avg_ms=redundant_avg,
            array_avg_ms=array_avg,
            speedup=speedup,
            redundant_result_count=redundant_count,
            array_result_count=array_count
        )
    
    def run(self):
        """运行所有基准测试"""
        logger.info("=" * 60)
        logger.info("Redundant Fields vs Array Operations Benchmark")
        logger.info("=" * 60)
        
        # 检查数据量
        pod_count = self.client.get_table_count("pod")
        relation_count = self.client.get_table_count("node_with_pod")
        logger.info(f"Data: {pod_count} pods, {relation_count} node_with_pod relations")
        logger.info(f"Iterations per query: {self.iterations}")
        logger.info("")
        
        if pod_count == 0:
            logger.error("No data found! Please run mock data script first.")
            return
        
        self.warmup()
        
        logger.info("\nRunning benchmarks...")
        for pair in QUERY_PAIRS:
            result = self.benchmark_query_pair(pair)
            self.results.append(result)
        
        self.print_results()
    
    def print_results(self):
        """打印结果"""
        logger.info("\n" + "=" * 80)
        logger.info("BENCHMARK RESULTS")
        logger.info("=" * 80)
        
        print(f"\n{'Query':<25} {'Redundant(ms)':<15} {'Array(ms)':<15} {'Speedup':<10} {'Results':<10}")
        print("-" * 80)
        
        total_redundant = 0
        total_array = 0
        
        for r in self.results:
            total_redundant += r.redundant_avg_ms
            total_array += r.array_avg_ms
            
            speedup_str = f"{r.speedup:.2f}x"
            if r.speedup >= 2:
                speedup_str = f"🚀 {speedup_str}"
            elif r.speedup >= 1.5:
                speedup_str = f"⚡ {speedup_str}"
            
            print(f"{r.query_name:<25} {r.redundant_avg_ms:>12.2f}   {r.array_avg_ms:>12.2f}   {speedup_str:<10} {r.redundant_result_count}")
        
        print("-" * 80)
        overall_speedup = total_array / total_redundant if total_redundant > 0 else 1.0
        print(f"{'TOTAL':<25} {total_redundant:>12.2f}   {total_array:>12.2f}   {overall_speedup:.2f}x")
        
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Average speedup using redundant fields: {overall_speedup:.2f}x")
        print(f"Total time saved per query batch: {total_array - total_redundant:.2f}ms")
        print("\nConclusion:")
        if overall_speedup >= 1.5:
            print("✅ Redundant fields provide significant performance improvement!")
        elif overall_speedup >= 1.1:
            print("✅ Redundant fields provide moderate performance improvement.")
        else:
            print("⚠️ Performance difference is minimal in this test environment.")
        
        # 保存详细结果
        self.save_results()
    
    def save_results(self):
        """保存结果到 JSON 文件"""
        output = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "iterations": self.iterations,
            "results": [
                {
                    "name": r.query_name,
                    "description": r.description,
                    "redundant_avg_ms": round(r.redundant_avg_ms, 3),
                    "array_avg_ms": round(r.array_avg_ms, 3),
                    "speedup": round(r.speedup, 3),
                    "result_count": r.redundant_result_count,
                    "redundant_p50": round(statistics.median(r.redundant_times_ms), 3),
                    "array_p50": round(statistics.median(r.array_times_ms), 3),
                }
                for r in self.results
            ]
        }
        
        output_file = os.path.join(os.path.dirname(__file__), 'benchmark_redundant_fields_result.json')
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        logger.info(f"\nDetailed results saved to: {output_file}")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Benchmark redundant fields vs array operations')
    parser.add_argument('--iterations', type=int, default=50, help='Number of iterations per query')
    args = parser.parse_args()
    
    benchmark = RedundantFieldsBenchmark(iterations=args.iterations)
    benchmark.run()


if __name__ == '__main__':
    main()
