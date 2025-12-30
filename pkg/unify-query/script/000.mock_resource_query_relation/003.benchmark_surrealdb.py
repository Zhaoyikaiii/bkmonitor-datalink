#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SurrealDB Benchmark Script

This script performs benchmark tests on SurrealDB operations including:
- Single resource write (insert/update)
- Relation creation/update
- Concurrent operations
- Lifecycle management (renewal vs expiration)

Usage:
    python benchmark_surrealdb.py --all
    python benchmark_surrealdb.py --test single-write --iterations 1000
    python benchmark_surrealdb.py --test concurrent --concurrency 50
    python benchmark_surrealdb.py --test lifecycle
    python benchmark_surrealdb.py --test relation
"""

import argparse
import json
import logging
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable

import requests

# ============================================================================
# Configuration
# ============================================================================

def _load_yaml_config(filename: str) -> Dict[str, Any]:
    """Load YAML configuration file"""
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

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class BenchmarkResult:
    """Result of a single benchmark test"""
    name: str
    iterations: int
    total_time: float
    latencies: List[float] = field(default_factory=list)
    errors: int = 0
    
    @property
    def qps(self) -> float:
        return self.iterations / self.total_time if self.total_time > 0 else 0
    
    @property
    def success_rate(self) -> float:
        return (self.iterations - self.errors) / self.iterations * 100 if self.iterations > 0 else 0
    
    @property
    def p50(self) -> float:
        return self._percentile(50)
    
    @property
    def p95(self) -> float:
        return self._percentile(95)
    
    @property
    def p99(self) -> float:
        return self._percentile(99)
    
    @property
    def avg(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0
    
    @property
    def max_latency(self) -> float:
        return max(self.latencies) if self.latencies else 0
    
    @property
    def min_latency(self) -> float:
        return min(self.latencies) if self.latencies else 0
    
    def _percentile(self, p: int) -> float:
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * p / 100)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "iterations": self.iterations,
            "total_time_sec": round(self.total_time, 3),
            "qps": round(self.qps, 2),
            "latency_ms": {
                "p50": round(self.p50 * 1000, 2),
                "p95": round(self.p95 * 1000, 2),
                "p99": round(self.p99 * 1000, 2),
                "avg": round(self.avg * 1000, 2),
                "min": round(self.min_latency * 1000, 2),
                "max": round(self.max_latency * 1000, 2),
            },
            "errors": self.errors,
            "success_rate": round(self.success_rate, 2)
        }
    
    def print_report(self):
        print(f"\nTest: {self.name} ({self.iterations} iterations)")
        print("-" * 60)
        print(f"  Total Time:     {self.total_time:.2f} seconds")
        print(f"  QPS:            {self.qps:.2f} ops/sec")
        print(f"  Latency (ms):")
        print(f"    - p50:        {self.p50 * 1000:.2f}")
        print(f"    - p95:        {self.p95 * 1000:.2f}")
        print(f"    - p99:        {self.p99 * 1000:.2f}")
        print(f"    - avg:        {self.avg * 1000:.2f}")
        print(f"    - min:        {self.min_latency * 1000:.2f}")
        print(f"    - max:        {self.max_latency * 1000:.2f}")
        print(f"  Success Rate:   {self.success_rate:.1f}%")
        if self.errors > 0:
            print(f"  Errors:         {self.errors}")


# ============================================================================
# SurrealDB Client
# ============================================================================

class SurrealDBBenchmarkClient:
    """Lightweight SurrealDB client for benchmarking"""
    
    def __init__(self):
        self.url = SURREAL_URL
        self.auth = (SURREAL_USER, SURREAL_PASS)
        self.namespace = SURREAL_NS
        self.database = SURREAL_DB
        self.session = requests.Session()
        self.session.verify = False
    
    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL and return result"""
        response = self.session.post(
            f"{self.url}/sql",
            headers={
                'Content-Type': 'text/plain',
                'Accept': 'application/json',
                'surreal-ns': self.namespace,
                'surreal-db': self.database
            },
            auth=self.auth,
            data=sql.encode('utf-8')
        )
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
        return response.json()
    
    def execute_timed(self, sql: str) -> tuple:
        """Execute SQL and return (result, latency_seconds)"""
        start = time.perf_counter()
        result = self.execute_sql(sql)
        latency = time.perf_counter() - start
        return result, latency


# ============================================================================
# Benchmark Tests
# ============================================================================

class SurrealDBBenchmark:
    """Benchmark test suite for SurrealDB"""
    
    def __init__(self):
        self.client = SurrealDBBenchmarkClient()
        self.results: List[BenchmarkResult] = []
    
    def _run_test(
        self,
        name: str,
        iterations: int,
        operation: Callable[[int], str]
    ) -> BenchmarkResult:
        """Run a single benchmark test"""
        latencies = []
        errors = 0
        
        start_time = time.perf_counter()
        
        for i in range(iterations):
            sql = operation(i)
            try:
                _, latency = self.client.execute_timed(sql)
                latencies.append(latency)
            except Exception as e:
                errors += 1
                if errors <= 3:
                    logger.warning(f"Error in iteration {i}: {e}")
        
        total_time = time.perf_counter() - start_time
        
        result = BenchmarkResult(
            name=name,
            iterations=iterations,
            total_time=total_time,
            latencies=latencies,
            errors=errors
        )
        self.results.append(result)
        return result
    
    def _run_concurrent_test(
        self,
        name: str,
        concurrency: int,
        iterations_per_worker: int,
        operation: Callable[[int, int], str]
    ) -> BenchmarkResult:
        """Run concurrent benchmark test"""
        all_latencies = []
        errors = 0
        total_iterations = concurrency * iterations_per_worker
        
        def worker(worker_id: int) -> List[float]:
            client = SurrealDBBenchmarkClient()
            latencies = []
            for i in range(iterations_per_worker):
                sql = operation(worker_id, i)
                try:
                    _, latency = client.execute_timed(sql)
                    latencies.append(latency)
                except Exception:
                    pass
            return latencies
        
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, w) for w in range(concurrency)]
            for future in as_completed(futures):
                try:
                    latencies = future.result()
                    all_latencies.extend(latencies)
                except Exception as e:
                    logger.warning(f"Worker error: {e}")
                    errors += iterations_per_worker
        
        total_time = time.perf_counter() - start_time
        
        result = BenchmarkResult(
            name=name,
            iterations=total_iterations,
            total_time=total_time,
            latencies=all_latencies,
            errors=errors
        )
        self.results.append(result)
        return result
    
    # =========================================================================
    # Test Cases
    # =========================================================================
    
    def test_single_pod_write(self, iterations: int = 100) -> BenchmarkResult:
        """Test single pod resource write"""
        def operation(i: int) -> str:
            now_ms = int(time.time() * 1000)
            return f"""
            fn::upsert_pod(
                {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'bench-pod-{i}' }},
                {now_ms},
                600000
            );
            """
        return self._run_test("Single Pod Write", iterations, operation)
    
    def test_single_node_write(self, iterations: int = 100) -> BenchmarkResult:
        """Test single node resource write"""
        def operation(i: int) -> str:
            now_ms = int(time.time() * 1000)
            return f"""
            fn::upsert_node(
                {{ bcs_cluster_id: 'BENCH', node: 'bench-node-{i}' }},
                {now_ms},
                600000
            );
            """
        return self._run_test("Single Node Write", iterations, operation)
    
    def test_single_system_write(self, iterations: int = 100) -> BenchmarkResult:
        """Test single system resource write"""
        def operation(i: int) -> str:
            now_ms = int(time.time() * 1000)
            ip_suffix = i % 256
            ip_prefix = (i // 256) % 256
            return f"""
            fn::upsert_system(
                {{ bk_cloud_id: '0', bk_target_ip: '10.{ip_prefix}.0.{ip_suffix}' }},
                {now_ms},
                600000
            );
            """
        return self._run_test("Single System Write", iterations, operation)
    
    def test_resource_renewal(self, iterations: int = 100) -> BenchmarkResult:
        """Test resource renewal (update within tolerance)"""
        # First create the resource
        now_ms = int(time.time() * 1000)
        self.client.execute_sql(f"""
            fn::upsert_pod(
                {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'renewal-test' }},
                {now_ms},
                600000
            );
        """)
        
        # Then test renewal
        def operation(i: int) -> str:
            now_ms = int(time.time() * 1000)
            return f"""
            fn::upsert_pod(
                {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'renewal-test' }},
                {now_ms},
                600000
            );
            """
        return self._run_test("Resource Renewal (Update)", iterations, operation)
    
    def test_resource_expiration(self, iterations: int = 100) -> BenchmarkResult:
        """Test resource expiration (create new after tolerance)"""
        def operation(i: int) -> str:
            now_ms = int(time.time() * 1000)
            # Use tolerance=1ms to force expiration
            return f"""
            fn::upsert_pod(
                {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'expiration-test-{i}' }},
                {now_ms},
                1
            );
            """
        return self._run_test("Resource Expiration (New Create)", iterations, operation)
    
    def test_relation_create(self, iterations: int = 100) -> BenchmarkResult:
        """Test relation creation"""
        # First create some resources
        now_ms = int(time.time() * 1000)
        nodes = []
        pods = []
        
        for i in range(min(iterations, 50)):
            result = self.client.execute_sql(f"""
                fn::upsert_node(
                    {{ bcs_cluster_id: 'BENCH', node: 'rel-node-{i}' }},
                    {now_ms},
                    600000
                )
            """)
            if result and len(result) > 0:
                r = result[0].get('result')
                if isinstance(r, dict) and r.get('id'):
                    nodes.append(r.get('id'))
                elif isinstance(r, list) and r and r[0].get('id'):
                    nodes.append(r[0].get('id'))
            
            result = self.client.execute_sql(f"""
                fn::upsert_pod(
                    {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'rel-pod-{i}' }},
                    {now_ms},
                    600000
                )
            """)
            if result and len(result) > 0:
                r = result[0].get('result')
                if isinstance(r, dict) and r.get('id'):
                    pods.append(r.get('id'))
                elif isinstance(r, list) and r and r[0].get('id'):
                    pods.append(r[0].get('id'))
        
        if not nodes or not pods:
            logger.warning(f"Failed to create resources for relation test. nodes={len(nodes)}, pods={len(pods)}")
            return BenchmarkResult("Relation Create", 0, 0)
        
        # Test relation creation
        def operation(i: int) -> str:
            now_ms = int(time.time() * 1000)
            node_idx = i % len(nodes)
            pod_idx = i % len(pods)
            node_id = nodes[node_idx]
            pod_id = pods[pod_idx]
            return f"""
            fn::upsert_relation('node_with_pod', {node_id}, {pod_id}, {now_ms})
            """
        
        return self._run_test("Relation Create", iterations, operation)
    
    def test_relation_update(self, iterations: int = 100) -> BenchmarkResult:
        """Test relation update"""
        # First create a relation
        now_ms = int(time.time() * 1000)
        
        node_result = self.client.execute_sql(f"""
            fn::upsert_node(
                {{ bcs_cluster_id: 'BENCH', node: 'rel-update-node' }},
                {now_ms},
                600000
            )
        """)
        pod_result = self.client.execute_sql(f"""
            fn::upsert_pod(
                {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'rel-update-pod' }},
                {now_ms},
                600000
            )
        """)
        
        # Extract IDs from result
        node_id = None
        pod_id = None
        if node_result and len(node_result) > 0:
            r = node_result[0].get('result')
            if isinstance(r, dict):
                node_id = r.get('id')
            elif isinstance(r, list) and r:
                node_id = r[0].get('id')
        
        if pod_result and len(pod_result) > 0:
            r = pod_result[0].get('result')
            if isinstance(r, dict):
                pod_id = r.get('id')
            elif isinstance(r, list) and r:
                pod_id = r[0].get('id')
        
        if not node_id or not pod_id:
            logger.warning(f"Failed to create resources for relation update test. node_id={node_id}, pod_id={pod_id}")
            return BenchmarkResult("Relation Update", 0, 0)
        
        # Create initial relation
        self.client.execute_sql(f"""
            fn::upsert_relation('node_with_pod', {node_id}, {pod_id}, {now_ms})
        """)
        
        # Test relation update
        def operation(i: int) -> str:
            now_ms = int(time.time() * 1000)
            return f"""
            fn::upsert_relation('node_with_pod', {node_id}, {pod_id}, {now_ms})
            """
        
        return self._run_test("Relation Update", iterations, operation)
    
    def test_concurrent_pod_write(
        self,
        concurrency: int = 10,
        iterations_per_worker: int = 50
    ) -> BenchmarkResult:
        """Test concurrent pod writes"""
        def operation(worker_id: int, i: int) -> str:
            now_ms = int(time.time() * 1000)
            return f"""
            fn::upsert_pod(
                {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'concurrent-pod-{worker_id}-{i}' }},
                {now_ms},
                600000
            );
            """
        return self._run_concurrent_test(
            f"Concurrent Pod Write ({concurrency} workers)",
            concurrency,
            iterations_per_worker,
            operation
        )
    
    def test_concurrent_mixed(
        self,
        concurrency: int = 10,
        iterations_per_worker: int = 50
    ) -> BenchmarkResult:
        """Test concurrent mixed operations (pods, nodes, systems)"""
        def operation(worker_id: int, i: int) -> str:
            now_ms = int(time.time() * 1000)
            op_type = i % 3
            if op_type == 0:
                return f"""
                fn::upsert_pod(
                    {{ bcs_cluster_id: 'BENCH', namespace: 'bench', pod: 'mixed-pod-{worker_id}-{i}' }},
                    {now_ms},
                    600000
                );
                """
            elif op_type == 1:
                return f"""
                fn::upsert_node(
                    {{ bcs_cluster_id: 'BENCH', node: 'mixed-node-{worker_id}-{i}' }},
                    {now_ms},
                    600000
                );
                """
            else:
                return f"""
                fn::upsert_system(
                    {{ bk_cloud_id: '0', bk_target_ip: '10.{worker_id}.{i % 256}.1' }},
                    {now_ms},
                    600000
                );
                """
        
        return self._run_concurrent_test(
            f"Concurrent Mixed Write ({concurrency} workers)",
            concurrency,
            iterations_per_worker,
            operation
        )
    
    # =========================================================================
    # Test Suites
    # =========================================================================
    
    def run_single_write_tests(self, iterations: int = 100):
        """Run all single write tests"""
        logger.info("Running single write tests...")
        self.test_single_pod_write(iterations)
        self.test_single_node_write(iterations)
        self.test_single_system_write(iterations)
    
    def run_lifecycle_tests(self, iterations: int = 100):
        """Run lifecycle management tests"""
        logger.info("Running lifecycle tests...")
        self.test_resource_renewal(iterations)
        self.test_resource_expiration(iterations)
    
    def run_relation_tests(self, iterations: int = 100):
        """Run relation tests"""
        logger.info("Running relation tests...")
        self.test_relation_create(iterations)
        self.test_relation_update(iterations)
    
    def run_concurrent_tests(self, concurrency: int = 10, iterations: int = 50):
        """Run concurrent tests"""
        logger.info(f"Running concurrent tests with {concurrency} workers...")
        self.test_concurrent_pod_write(concurrency, iterations)
        self.test_concurrent_mixed(concurrency, iterations)
    
    def run_all_tests(self, iterations: int = 100, concurrency: int = 10):
        """Run all benchmark tests"""
        self.run_single_write_tests(iterations)
        self.run_lifecycle_tests(iterations)
        self.run_relation_tests(iterations)
        self.run_concurrent_tests(concurrency, iterations // 2)
    
    def print_summary(self):
        """Print summary of all results"""
        print("\n" + "=" * 70)
        print("SurrealDB Benchmark Results Summary")
        print("=" * 70)
        print(f"\nTest Environment:")
        print(f"  URL:       {SURREAL_URL}")
        print(f"  Namespace: {SURREAL_NS}")
        print(f"  Database:  {SURREAL_DB}")
        print(f"  Time:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        for result in self.results:
            result.print_report()
        
        print("\n" + "=" * 70)
        print("Summary Table")
        print("=" * 70)
        print(f"{'Test Name':<45} {'QPS':>8} {'p50(ms)':>10} {'p99(ms)':>10} {'Success':>8}")
        print("-" * 70)
        for result in self.results:
            print(f"{result.name:<45} {result.qps:>8.1f} {result.p50*1000:>10.2f} {result.p99*1000:>10.2f} {result.success_rate:>7.1f}%")
    
    def export_json(self, filename: str):
        """Export results to JSON file"""
        data = {
            "timestamp": datetime.now().isoformat(),
            "environment": {
                "url": SURREAL_URL,
                "namespace": SURREAL_NS,
                "database": SURREAL_DB
            },
            "results": [r.to_dict() for r in self.results]
        }
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Results exported to {filename}")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='SurrealDB Benchmark Tool')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--test', type=str, choices=[
        'single-write', 'lifecycle', 'relation', 'concurrent'
    ], help='Run specific test suite')
    parser.add_argument('--iterations', type=int, default=100, help='Number of iterations per test')
    parser.add_argument('--concurrency', type=int, default=10, help='Concurrency level for concurrent tests')
    parser.add_argument('--output', type=str, help='Output JSON file for results')
    
    args = parser.parse_args()
    
    # Disable SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    benchmark = SurrealDBBenchmark()
    
    # Test connection first
    try:
        benchmark.client.execute_sql("RETURN 1")
        logger.info(f"Connected to SurrealDB at {SURREAL_URL}")
    except Exception as e:
        logger.error(f"Failed to connect to SurrealDB: {e}")
        return 1
    
    # Run tests
    if args.all:
        benchmark.run_all_tests(args.iterations, args.concurrency)
    elif args.test == 'single-write':
        benchmark.run_single_write_tests(args.iterations)
    elif args.test == 'lifecycle':
        benchmark.run_lifecycle_tests(args.iterations)
    elif args.test == 'relation':
        benchmark.run_relation_tests(args.iterations)
    elif args.test == 'concurrent':
        benchmark.run_concurrent_tests(args.concurrency, args.iterations)
    else:
        # Default: run all
        benchmark.run_all_tests(args.iterations, args.concurrency)
    
    # Print results
    benchmark.print_summary()
    
    # Export if requested
    if args.output:
        benchmark.export_json(args.output)
    
    return 0


if __name__ == '__main__':
    exit(main())
