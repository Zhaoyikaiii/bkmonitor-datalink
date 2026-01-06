#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SurrealDB Benchmark Script - Plan 01: Active Windows

统一的混合负载测试，专为 Active Windows 方案设计：
- 使用简单的 UPSERT MERGE 语法（由 Event 自动管理 active_windows）
- 读写混合执行
- 定期获取 SurrealDB 健康状态
- 可配置读写比例、并发数、持续时间

与 Plan 00 (created_at) 的区别：
- Plan 00: 使用 fn::upsert_pod/fn::upsert_service 函数
- Plan 01: 使用简单 UPSERT MERGE，Event 自动管理生命周期

Usage:
    python 005.benchmark_query.py
    python 005.benchmark_query.py --concurrency 50 --duration 60
    python 005.benchmark_query.py --read-ratio 80 --health-interval 5
"""

import argparse
import json
import logging
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import requests

# Disable SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
class HealthStatus:
    """SurrealDB 健康状态"""
    timestamp: float
    status: str  # "ok", "error"
    latency_ms: float
    error_msg: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": datetime.fromtimestamp(self.timestamp).isoformat(),
            "status": self.status,
            "latency_ms": round(self.latency_ms, 2),
            "error": self.error_msg
        }


@dataclass
class OperationStats:
    """操作统计 - 与 Plan 00 保持一致"""
    read_single: int = 0
    read_batch: int = 0
    read_relation: int = 0
    write_heartbeat: int = 0
    write_new: int = 0
    errors: int = 0
    
    def total_reads(self) -> int:
        return self.read_single + self.read_batch + self.read_relation
    
    def total_writes(self) -> int:
        return self.write_heartbeat + self.write_new
    
    def total_ops(self) -> int:
        return self.total_reads() + self.total_writes()
    
    def to_dict(self) -> Dict[str, int]:
        return {
            "read_single": self.read_single,
            "read_batch": self.read_batch,
            "read_relation": self.read_relation,
            "write_heartbeat": self.write_heartbeat,
            "write_new": self.write_new,
            "total_reads": self.total_reads(),
            "total_writes": self.total_writes(),
            "total_ops": self.total_ops(),
            "errors": self.errors
        }


@dataclass
class LatencyStats:
    """延迟统计 - 与 Plan 00 保持一致"""
    latencies: List[float] = field(default_factory=list)
    read_latencies: List[float] = field(default_factory=list)
    write_latencies: List[float] = field(default_factory=list)
    heartbeat_latencies: List[float] = field(default_factory=list)
    
    def _percentile(self, data: List[float], p: int) -> float:
        if not data:
            return 0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * p / 100)
        return sorted_data[min(idx, len(sorted_data) - 1)]
    
    def _stats(self, data: List[float]) -> Dict[str, float]:
        if not data:
            return {"p50": 0, "p95": 0, "p99": 0, "avg": 0, "min": 0, "max": 0}
        return {
            "p50": round(self._percentile(data, 50) * 1000, 2),
            "p95": round(self._percentile(data, 95) * 1000, 2),
            "p99": round(self._percentile(data, 99) * 1000, 2),
            "avg": round(statistics.mean(data) * 1000, 2),
            "min": round(min(data) * 1000, 2),
            "max": round(max(data) * 1000, 2)
        }
    
    def overall_stats(self) -> Dict[str, float]:
        return self._stats(self.latencies)
    
    def read_stats(self) -> Dict[str, float]:
        return self._stats(self.read_latencies)
    
    def write_stats(self) -> Dict[str, float]:
        return self._stats(self.write_latencies)
    
    def heartbeat_stats(self) -> Dict[str, float]:
        return self._stats(self.heartbeat_latencies)


# ============================================================================
# SurrealDB Client
# ============================================================================

class SurrealDBClient:
    """SurrealDB client with health check support"""
    
    def __init__(self):
        self.url = SURREAL_URL
        self.auth = (SURREAL_USER, SURREAL_PASS)
        self.namespace = SURREAL_NS
        self.database = SURREAL_DB
        self.session = requests.Session()
        self.session.verify = False
    
    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL and return result"""
        full_sql = f"USE NS {self.namespace} DB {self.database}; {sql}"
        
        response = self.session.post(
            f"{self.url}/sql",
            headers={
                'Content-Type': 'text/plain; charset=utf-8',
                'Accept': 'application/json'
            },
            auth=self.auth,
            data=full_sql.encode('utf-8')
        )
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
        
        results = response.json()
        
        # Check for errors (skip USE statement)
        for i, result in enumerate(results[1:], 1):
            if result.get('status') == 'ERR':
                error_detail = result.get('detail') or result.get('result', 'Unknown error')
                raise Exception(f"SQL error: {error_detail}")
        
        return results[1:] if len(results) > 1 else results
    
    def execute_timed(self, sql: str) -> Tuple[List[Dict], float]:
        """Execute SQL and return (result, latency_seconds)"""
        start = time.perf_counter()
        result = self.execute_sql(sql)
        latency = time.perf_counter() - start
        return result, latency
    
    def health_check(self) -> HealthStatus:
        """检查 SurrealDB 健康状态"""
        timestamp = time.time()
        start = time.perf_counter()
        
        try:
            response = self.session.get(
                f"{self.url}/health",
                timeout=5
            )
            latency = (time.perf_counter() - start) * 1000
            
            if response.status_code == 200:
                return HealthStatus(
                    timestamp=timestamp,
                    status="ok",
                    latency_ms=latency
                )
            else:
                return HealthStatus(
                    timestamp=timestamp,
                    status="error",
                    latency_ms=latency,
                    error_msg=f"HTTP {response.status_code}"
                )
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            return HealthStatus(
                timestamp=timestamp,
                status="error",
                latency_ms=latency,
                error_msg=str(e)
            )
    
    def get_info_db(self) -> Dict[str, Any]:
        """获取 DATABASE 级别信息"""
        try:
            result = self.execute_sql("INFO FOR DB;")
            if result and result[0].get('result'):
                return result[0]['result']
        except Exception as e:
            return {"error": str(e)}
        return {}
    
    def get_table_count(self, table: str) -> int:
        """获取表的记录数"""
        try:
            result = self.execute_sql(f"SELECT count() FROM {table} GROUP ALL;")
            if result and result[0].get('result'):
                data = result[0]['result']
                if data and len(data) > 0:
                    return data[0].get('count', 0)
        except Exception:
            pass
        return 0


# ============================================================================
# Data Pool
# ============================================================================

class DataPool:
    """缓存现有数据，用于随机选择进行心跳更新和查询 - 与 Plan 00 保持一致"""
    
    def __init__(self, client: SurrealDBClient):
        self.client = client
        self.pods: List[Dict] = []
        self.services: List[Dict] = []
        self.pod_ids: List[str] = []
        self.service_ids: List[str] = []
        self.namespaces: List[Tuple[str, str]] = []  # (cluster_id, namespace)
        self._lock = threading.Lock()
    
    def load_existing_data(self, limit: int = 5000):
        """加载现有数据到内存池"""
        logger.info(f"Loading existing data (limit={limit})...")
        
        # 加载 pods
        result = self.client.execute_sql(f"SELECT * FROM pod LIMIT {limit};")
        if result and result[0].get('result'):
            self.pods = result[0]['result']
            self.pod_ids = [p['id'] for p in self.pods if p.get('id')]
        logger.info(f"  Loaded {len(self.pods)} pods")
        
        # 加载 services
        result = self.client.execute_sql(f"SELECT * FROM service LIMIT {limit};")
        if result and result[0].get('result'):
            self.services = result[0]['result']
            self.service_ids = [s['id'] for s in self.services if s.get('id')]
        logger.info(f"  Loaded {len(self.services)} services")
        
        # 提取 namespaces
        self.namespaces = list(set(
            (p.get('bcs_cluster_id', ''), p.get('namespace', ''))
            for p in self.pods
            if p.get('bcs_cluster_id') and p.get('namespace')
        ))
        logger.info(f"  Found {len(self.namespaces)} unique namespaces")
    
    def get_random_pod(self) -> Optional[Dict]:
        if not self.pods:
            return None
        return random.choice(self.pods)
    
    def get_random_service(self) -> Optional[Dict]:
        if not self.services:
            return None
        return random.choice(self.services)
    
    def get_random_pod_id(self) -> Optional[str]:
        if not self.pod_ids:
            return None
        return random.choice(self.pod_ids)
    
    def get_random_service_id(self) -> Optional[str]:
        if not self.service_ids:
            return None
        return random.choice(self.service_ids)
    
    def get_random_namespace(self) -> Optional[Tuple[str, str]]:
        if not self.namespaces:
            return None
        return random.choice(self.namespaces)


# ============================================================================
# Unified Mixed Load Benchmark
# ============================================================================

class UnifiedBenchmark:
    """
    统一混合负载测试 - Active Windows 方案
    
    使用简单 UPSERT MERGE 语法，Event 自动管理 active_windows
    """
    
    def __init__(self):
        self.client = SurrealDBClient()
        self.data_pool = DataPool(self.client)
        
        # 统计数据
        self.op_stats = OperationStats()
        self.latency_stats = LatencyStats()
        self.health_records: List[HealthStatus] = []
        
        # 数据库信息
        self.table_counts_before: Dict[str, int] = {}
        self.table_counts_after: Dict[str, int] = {}
        
        # 线程安全
        self._stats_lock = threading.Lock()
        self._health_lock = threading.Lock()
        
        # 控制标志
        self._stop_flag = threading.Event()
        
        # 测试参数
        self.start_time: float = 0
        self.end_time: float = 0
    
    def collect_table_counts(self) -> Dict[str, int]:
        """收集表记录数"""
        tables = [
            "pod", "service", "node", "deployment", "replicaset", "container",
            "pod_with_service", "node_with_pod", "deployment_with_replicaset",
            "pod_with_replicaset", "container_with_pod"
        ]
        counts = {}
        for table in tables:
            counts[table] = self.client.get_table_count(table)
        return counts
    
    def _escape_string(self, s: str) -> str:
        """转义字符串中的单引号"""
        return str(s).replace("'", "\\'")
    
    def _build_record_id(self, table: str, dimensions: Dict[str, Any]) -> str:
        """构建确定性记录 ID"""
        sorted_items = sorted(dimensions.items())
        kv_parts = [f"{k}={v}" for k, v in sorted_items]
        kv_str = ",".join(kv_parts)
        return f"{table}:⟨{kv_str}⟩"
    
    # =========================================================================
    # SQL 生成 - 读操作
    # =========================================================================
    
    def _sql_read_pod_by_dims(self, pod: Dict) -> str:
        """按维度读取 pod"""
        return f"""
        SELECT * FROM pod 
        WHERE bcs_cluster_id = '{self._escape_string(pod.get('bcs_cluster_id', ''))}' 
          AND namespace = '{self._escape_string(pod.get('namespace', ''))}' 
          AND pod = '{self._escape_string(pod.get('pod', ''))}' 
        LIMIT 1;
        """
    
    def _sql_read_pods_batch(self, cluster_id: str, namespace: str, limit: int = 50) -> str:
        """批量读取 pods"""
        return f"""
        SELECT * FROM pod 
        WHERE bcs_cluster_id = '{self._escape_string(cluster_id)}' 
          AND namespace = '{self._escape_string(namespace)}' 
        LIMIT {limit};
        """
    
    def _sql_read_relations_by_pod(self, pod_id: str) -> str:
        """查询 pod 的所有关系"""
        return f"SELECT * FROM pod_with_service WHERE in = {pod_id};"
    
    # =========================================================================
    # SQL 生成 - 写操作 (Active Windows 方案: 简单 UPSERT MERGE)
    # =========================================================================
    
    def _sql_heartbeat_pod(self, pod: Dict) -> str:
        """
        Pod 心跳更新 - Active Windows 方案
        
        使用简单的 UPSERT MERGE，Event 会自动管理 start_time, end_time, active_windows, windows_count
        """
        record_id = self._build_record_id('pod', {
            'bcs_cluster_id': pod.get('bcs_cluster_id', ''),
            'namespace': pod.get('namespace', ''),
            'pod': pod.get('pod', '')
        })
        
        return f"""
        UPSERT {record_id} MERGE {{
            bcs_cluster_id: '{self._escape_string(pod.get('bcs_cluster_id', ''))}',
            namespace: '{self._escape_string(pod.get('namespace', ''))}',
            pod: '{self._escape_string(pod.get('pod', ''))}'
        }};
        """
    
    def _sql_heartbeat_service(self, service: Dict) -> str:
        """
        Service 心跳更新 - Active Windows 方案
        """
        record_id = self._build_record_id('service', {
            'bcs_cluster_id': service.get('bcs_cluster_id', ''),
            'namespace': service.get('namespace', ''),
            'service': service.get('service', '')
        })
        
        return f"""
        UPSERT {record_id} MERGE {{
            bcs_cluster_id: '{self._escape_string(service.get('bcs_cluster_id', ''))}',
            namespace: '{self._escape_string(service.get('namespace', ''))}',
            service: '{self._escape_string(service.get('service', ''))}'
        }};
        """
    
    def _sql_write_new_pod(self, suffix: str) -> str:
        """写入新 pod - Active Windows 方案"""
        record_id = self._build_record_id('pod', {
            'bcs_cluster_id': 'BENCH-NEW',
            'namespace': 'bench-new',
            'pod': f'new-pod-{suffix}'
        })
        
        return f"""
        UPSERT {record_id} MERGE {{
            bcs_cluster_id: 'BENCH-NEW',
            namespace: 'bench-new',
            pod: 'new-pod-{suffix}'
        }};
        """
    
    def _sql_write_new_service(self, suffix: str) -> str:
        """写入新 service - Active Windows 方案"""
        record_id = self._build_record_id('service', {
            'bcs_cluster_id': 'BENCH-NEW',
            'namespace': 'bench-new',
            'service': f'new-svc-{suffix}'
        })
        
        return f"""
        UPSERT {record_id} MERGE {{
            bcs_cluster_id: 'BENCH-NEW',
            namespace: 'bench-new',
            service: 'new-svc-{suffix}'
        }};
        """
    
    # =========================================================================
    # 健康检查线程
    # =========================================================================
    
    def _health_check_worker(self, interval: float):
        """定期健康检查线程"""
        logger.info(f"Health check worker started (interval={interval}s)")
        
        while not self._stop_flag.is_set():
            health = self.client.health_check()
            
            with self._health_lock:
                self.health_records.append(health)
            
            # 打印健康状态
            status_icon = "✓" if health.status == "ok" else "✗"
            elapsed = time.time() - self.start_time
            logger.info(f"[{elapsed:.1f}s] Health: {status_icon} {health.status} ({health.latency_ms:.1f}ms)")
            
            # 等待下次检查
            self._stop_flag.wait(interval)
        
        logger.info("Health check worker stopped")
    
    # =========================================================================
    # 混合负载 Worker
    # =========================================================================
    
    def _mixed_load_worker(
        self,
        worker_id: int,
        read_ratio: int,
        heartbeat_ratio: int
    ):
        """
        混合负载工作线程
        
        Args:
            worker_id: 工作线程 ID
            read_ratio: 读操作占比 (0-100)
            heartbeat_ratio: 写操作中心跳更新占比 (0-100)
        """
        client = SurrealDBClient()
        
        local_stats = OperationStats()
        local_latencies = LatencyStats()
        
        while not self._stop_flag.is_set():
            rand = random.random() * 100
            sql = None
            op_type = None
            
            try:
                if rand < read_ratio:
                    # === 读操作 ===
                    # 与 Plan 00 保持一致的读操作分布，确保公平对比
                    read_type = random.random()
                    
                    if read_type < 0.4:
                        # 单条读取 (40%)
                        pod = self.data_pool.get_random_pod()
                        if pod:
                            sql = self._sql_read_pod_by_dims(pod)
                            op_type = "read_single"
                    
                    elif read_type < 0.7:
                        # 批量读取 (30%)
                        ns = self.data_pool.get_random_namespace()
                        if ns:
                            sql = self._sql_read_pods_batch(ns[0], ns[1], 50)
                            op_type = "read_batch"
                    
                    else:
                        # 关系查询 (30%)
                        pod_id = self.data_pool.get_random_pod_id()
                        if pod_id:
                            sql = self._sql_read_relations_by_pod(pod_id)
                            op_type = "read_relation"
                
                else:
                    # === 写操作 ===
                    write_rand = random.random() * 100
                    
                    if write_rand < heartbeat_ratio:
                        # 心跳更新
                        if random.random() < 0.6:
                            pod = self.data_pool.get_random_pod()
                            if pod:
                                sql = self._sql_heartbeat_pod(pod)
                                op_type = "write_heartbeat"
                        else:
                            service = self.data_pool.get_random_service()
                            if service:
                                sql = self._sql_heartbeat_service(service)
                                op_type = "write_heartbeat"
                    else:
                        # 新增写入
                        suffix = f"{worker_id}-{random.randint(0, 999999)}"
                        if random.random() < 0.5:
                            sql = self._sql_write_new_pod(suffix)
                        else:
                            sql = self._sql_write_new_service(suffix)
                        op_type = "write_new"
                
                if sql and op_type:
                    _, latency = client.execute_timed(sql)
                    
                    # 更新本地统计 - 与 Plan 00 保持一致
                    local_latencies.latencies.append(latency)
                    
                    if op_type == "read_single":
                        local_stats.read_single += 1
                        local_latencies.read_latencies.append(latency)
                    elif op_type == "read_batch":
                        local_stats.read_batch += 1
                        local_latencies.read_latencies.append(latency)
                    elif op_type == "read_relation":
                        local_stats.read_relation += 1
                        local_latencies.read_latencies.append(latency)
                    elif op_type == "write_heartbeat":
                        local_stats.write_heartbeat += 1
                        local_latencies.heartbeat_latencies.append(latency)
                    elif op_type == "write_new":
                        local_stats.write_new += 1
                        local_latencies.write_latencies.append(latency)
            
            except Exception as e:
                local_stats.errors += 1
        
        # 合并到全局统计
        with self._stats_lock:
            self.op_stats.read_single += local_stats.read_single
            self.op_stats.read_batch += local_stats.read_batch
            self.op_stats.read_relation += local_stats.read_relation
            self.op_stats.write_heartbeat += local_stats.write_heartbeat
            self.op_stats.write_new += local_stats.write_new
            self.op_stats.errors += local_stats.errors
            
            self.latency_stats.latencies.extend(local_latencies.latencies)
            self.latency_stats.read_latencies.extend(local_latencies.read_latencies)
            self.latency_stats.write_latencies.extend(local_latencies.write_latencies)
            self.latency_stats.heartbeat_latencies.extend(local_latencies.heartbeat_latencies)
    
    # =========================================================================
    # 运行测试
    # =========================================================================
    
    def run(
        self,
        concurrency: int = 20,
        duration: int = 60,
        read_ratio: int = 70,
        heartbeat_ratio: int = 80,
        health_interval: float = 5.0
    ):
        """
        运行统一混合负载测试
        
        Args:
            concurrency: 并发工作线程数
            duration: 测试持续时间（秒）
            read_ratio: 读操作占比 (0-100)
            heartbeat_ratio: 写操作中心跳更新占比 (0-100)
            health_interval: 健康检查间隔（秒）
        """
        logger.info("=" * 70)
        logger.info("SurrealDB Benchmark - Plan 01: Active Windows")
        logger.info("=" * 70)
        logger.info(f"Configuration:")
        logger.info(f"  URL:              {SURREAL_URL}")
        logger.info(f"  Namespace/DB:     {SURREAL_NS}/{SURREAL_DB}")
        logger.info(f"  Concurrency:      {concurrency} workers")
        logger.info(f"  Duration:         {duration} seconds")
        logger.info(f"  Read Ratio:       {read_ratio}%")
        logger.info(f"  Heartbeat Ratio:  {heartbeat_ratio}% (of writes)")
        logger.info(f"  Health Interval:  {health_interval}s")
        logger.info("")
        
        # 收集测试前的表记录数
        logger.info("Collecting table counts before benchmark...")
        self.table_counts_before = self.collect_table_counts()
        for table, count in sorted(self.table_counts_before.items()):
            if count > 0:
                logger.info(f"  {table}: {count}")
        
        # 加载数据
        self.data_pool.load_existing_data(limit=10000)
        
        if not self.data_pool.pods:
            logger.error("No data loaded, cannot run benchmark")
            return
        
        # 初始健康检查
        initial_health = self.client.health_check()
        logger.info(f"Initial health: {initial_health.status} ({initial_health.latency_ms:.1f}ms)")
        
        # 重置状态
        self._stop_flag.clear()
        self.op_stats = OperationStats()
        self.latency_stats = LatencyStats()
        self.health_records = [initial_health]
        
        self.start_time = time.time()
        
        logger.info(f"\nStarting benchmark at {datetime.now().strftime('%H:%M:%S')}...")
        logger.info("-" * 70)
        
        # 启动健康检查线程
        health_thread = threading.Thread(
            target=self._health_check_worker,
            args=(health_interval,),
            daemon=True
        )
        health_thread.start()
        
        # 启动工作线程
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(
                    self._mixed_load_worker,
                    worker_id,
                    read_ratio,
                    heartbeat_ratio
                )
                for worker_id in range(concurrency)
            ]
            
            # 等待指定时间
            time.sleep(duration)
            
            # 停止所有线程
            self._stop_flag.set()
            
            # 等待工作线程完成
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.warning(f"Worker error: {e}")
        
        self.end_time = time.time()
        
        # 最终健康检查
        final_health = self.client.health_check()
        with self._health_lock:
            self.health_records.append(final_health)
        
        # 收集测试后的表记录数
        logger.info("Collecting table counts after benchmark...")
        self.table_counts_after = self.collect_table_counts()
        
        logger.info("-" * 70)
        logger.info(f"Benchmark completed at {datetime.now().strftime('%H:%M:%S')}")
    
    # =========================================================================
    # 结果输出
    # =========================================================================
    
    def print_results(self):
        """打印测试结果"""
        total_time = self.end_time - self.start_time
        total_ops = self.op_stats.total_ops()
        qps = total_ops / total_time if total_time > 0 else 0
        
        print("\n" + "=" * 70)
        print("BENCHMARK RESULTS - Plan 01: Active Windows")
        print("=" * 70)
        
        # 基本统计
        print(f"\n{'─' * 70}")
        print("OVERALL STATISTICS")
        print(f"{'─' * 70}")
        print(f"  Total Time:       {total_time:.2f} seconds")
        print(f"  Total Operations: {total_ops:,}")
        print(f"  QPS:              {qps:,.1f} ops/sec")
        print(f"  Errors:           {self.op_stats.errors}")
        success_rate = (total_ops - self.op_stats.errors) / total_ops * 100 if total_ops > 0 else 0
        print(f"  Success Rate:     {success_rate:.2f}%")
        
        # 操作分布
        print(f"\n{'─' * 70}")
        print("OPERATION BREAKDOWN")
        print(f"{'─' * 70}")
        total_reads = self.op_stats.total_reads()
        total_writes = self.op_stats.total_writes()
        print(f"  Read Operations:  {total_reads:,} ({total_reads/total_ops*100:.1f}%)" if total_ops > 0 else "  Read Operations:  0")
        print(f"    - Single:       {self.op_stats.read_single:,}")
        print(f"    - Batch:        {self.op_stats.read_batch:,}")
        print(f"    - Relation:     {self.op_stats.read_relation:,}")
        print(f"  Write Operations: {total_writes:,} ({total_writes/total_ops*100:.1f}%)" if total_ops > 0 else "  Write Operations: 0")
        print(f"    - Heartbeat:    {self.op_stats.write_heartbeat:,}")
        print(f"    - New:          {self.op_stats.write_new:,}")
        
        # 延迟统计
        print(f"\n{'─' * 70}")
        print("LATENCY STATISTICS (ms)")
        print(f"{'─' * 70}")
        
        overall = self.latency_stats.overall_stats()
        print(f"  Overall ({len(self.latency_stats.latencies):,} samples):")
        print(f"    p50: {overall['p50']:.2f}  p95: {overall['p95']:.2f}  p99: {overall['p99']:.2f}")
        print(f"    avg: {overall['avg']:.2f}  min: {overall['min']:.2f}  max: {overall['max']:.2f}")
        
        if self.latency_stats.read_latencies:
            read = self.latency_stats.read_stats()
            print(f"  Read ({len(self.latency_stats.read_latencies):,} samples):")
            print(f"    p50: {read['p50']:.2f}  p95: {read['p95']:.2f}  p99: {read['p99']:.2f}")
        
        if self.latency_stats.heartbeat_latencies:
            hb = self.latency_stats.heartbeat_stats()
            print(f"  Heartbeat ({len(self.latency_stats.heartbeat_latencies):,} samples):")
            print(f"    p50: {hb['p50']:.2f}  p95: {hb['p95']:.2f}  p99: {hb['p99']:.2f}")
        
        if self.latency_stats.write_latencies:
            write = self.latency_stats.write_stats()
            print(f"  Write New ({len(self.latency_stats.write_latencies):,} samples):")
            print(f"    p50: {write['p50']:.2f}  p95: {write['p95']:.2f}  p99: {write['p99']:.2f}")
        
        # 健康检查记录
        print(f"\n{'─' * 70}")
        print("HEALTH CHECK RECORDS")
        print(f"{'─' * 70}")
        
        ok_count = sum(1 for h in self.health_records if h.status == "ok")
        error_count = len(self.health_records) - ok_count
        avg_health_latency = statistics.mean(h.latency_ms for h in self.health_records) if self.health_records else 0
        
        print(f"  Total Checks:     {len(self.health_records)}")
        print(f"  OK:               {ok_count}")
        print(f"  Errors:           {error_count}")
        print(f"  Avg Latency:      {avg_health_latency:.2f}ms")
        
        if error_count > 0:
            print(f"  Error Details:")
            for h in self.health_records:
                if h.status == "error":
                    ts = datetime.fromtimestamp(h.timestamp).strftime('%H:%M:%S')
                    print(f"    [{ts}] {h.error_msg}")
        
        # 表记录数变化
        print(f"\n{'─' * 70}")
        print("TABLE RECORD CHANGES")
        print(f"{'─' * 70}")
        for table in sorted(set(self.table_counts_before.keys()) | set(self.table_counts_after.keys())):
            before = self.table_counts_before.get(table, 0)
            after = self.table_counts_after.get(table, 0)
            diff = after - before
            if before > 0 or after > 0:
                sign = "+" if diff > 0 else ""
                print(f"  {table:30s}: {before:>8d} -> {after:>8d} ({sign}{diff})")
        
        print("\n" + "=" * 70)
    
    def export_json(self, filename: str):
        """导出结果到 JSON 文件"""
        total_time = self.end_time - self.start_time
        total_ops = self.op_stats.total_ops()
        
        data = {
            "plan": "01.plan_active_windows",
            "timestamp": datetime.now().isoformat(),
            "environment": {
                "url": SURREAL_URL,
                "namespace": SURREAL_NS,
                "database": SURREAL_DB
            },
            "data_pool": {
                "pods": len(self.data_pool.pods),
                "services": len(self.data_pool.services),
                "namespaces": len(self.data_pool.namespaces)
            },
            "summary": {
                "total_time_sec": round(total_time, 2),
                "total_ops": total_ops,
                "qps": round(total_ops / total_time, 2) if total_time > 0 else 0,
                "success_rate": round((total_ops - self.op_stats.errors) / total_ops * 100, 2) if total_ops > 0 else 0
            },
            "operations": self.op_stats.to_dict(),
            "latency": {
                "overall": self.latency_stats.overall_stats(),
                "read": self.latency_stats.read_stats(),
                "write": self.latency_stats.write_stats(),
                "heartbeat": self.latency_stats.heartbeat_stats()
            },
            "health_checks": [h.to_dict() for h in self.health_records],
            "table_counts": {
                "before": self.table_counts_before,
                "after": self.table_counts_after,
                "changes": {
                    table: {
                        "before": self.table_counts_before.get(table, 0),
                        "after": self.table_counts_after.get(table, 0),
                        "diff": self.table_counts_after.get(table, 0) - self.table_counts_before.get(table, 0)
                    }
                    for table in set(self.table_counts_before.keys()) | set(self.table_counts_after.keys())
                }
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Results exported to {filename}")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='SurrealDB Benchmark - Plan 01: Active Windows',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 005.benchmark_query.py
  python 005.benchmark_query.py --concurrency 50 --duration 120
  python 005.benchmark_query.py --read-ratio 80 --health-interval 10
  python 005.benchmark_query.py --output result.json
        """
    )
    parser.add_argument('--concurrency', type=int, default=20,
                        help='Number of concurrent workers (default: 20)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Test duration in seconds (default: 60)')
    parser.add_argument('--read-ratio', type=int, default=70,
                        help='Read operation ratio 0-100 (default: 70)')
    parser.add_argument('--heartbeat-ratio', type=int, default=80,
                        help='Heartbeat ratio in write operations 0-100 (default: 80)')
    parser.add_argument('--health-interval', type=float, default=5.0,
                        help='Health check interval in seconds (default: 5.0)')
    parser.add_argument('--output', type=str,
                        help='Output JSON file for results')
    
    args = parser.parse_args()
    
    benchmark = UnifiedBenchmark()
    
    # Test connection first
    try:
        benchmark.client.execute_sql("RETURN 1;")
        logger.info(f"Connected to SurrealDB at {SURREAL_URL}")
    except Exception as e:
        logger.error(f"Failed to connect to SurrealDB: {e}")
        return 1
    
    # Run benchmark
    benchmark.run(
        concurrency=args.concurrency,
        duration=args.duration,
        read_ratio=args.read_ratio,
        heartbeat_ratio=args.heartbeat_ratio,
        health_interval=args.health_interval
    )
    
    # Print results
    benchmark.print_results()
    
    # Export if requested
    if args.output:
        benchmark.export_json(args.output)
    
    return 0


if __name__ == '__main__':
    exit(main())
