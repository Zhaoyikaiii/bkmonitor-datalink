#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SurrealDB Benchmark Script - Unified Mixed Load Test

统一的混合负载测试，不区分场景：
- 读写混合执行
- 定期获取 SurrealDB 健康状态
- 可配置读写比例、并发数、持续时间

Usage:
    python 003.benchmark_surrealdb.py
    python 003.benchmark_surrealdb.py --concurrency 50 --duration 60
    python 003.benchmark_surrealdb.py --read-ratio 80 --health-interval 5
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

# 默认生命周期容忍时间（毫秒）
TOLERANCE_TIME_MS = 600000

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
    """操作统计"""
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
    """延迟统计"""
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
            "p50": self._percentile(data, 50) * 1000,
            "p95": self._percentile(data, 95) * 1000,
            "p99": self._percentile(data, 99) * 1000,
            "avg": statistics.mean(data) * 1000,
            "min": min(data) * 1000,
            "max": max(data) * 1000
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
    
    def execute_timed(self, sql: str) -> Tuple[List[Dict], float]:
        """Execute SQL and return (result, latency_seconds)"""
        start = time.perf_counter()
        result = self.execute_sql(sql)
        latency = time.perf_counter() - start
        return result, latency
    
    def health_check(self) -> HealthStatus:
        """
        检查 SurrealDB 健康状态
        
        使用 /health 接口获取状态
        """
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
    
    def get_info_root(self) -> Dict[str, Any]:
        """
        获取 ROOT 级别信息
        
        INFO FOR ROOT - 获取所有 namespaces 和用户信息
        """
        try:
            result = self.execute_sql("INFO FOR ROOT;")
            if result and result[0].get('result'):
                return result[0]['result']
        except Exception as e:
            return {"error": str(e)}
        return {}
    
    def get_info_ns(self) -> Dict[str, Any]:
        """
        获取 NAMESPACE 级别信息
        
        INFO FOR NS - 获取当前 namespace 下的 databases 和用户
        """
        try:
            result = self.execute_sql("INFO FOR NS;")
            if result and result[0].get('result'):
                return result[0]['result']
        except Exception as e:
            return {"error": str(e)}
        return {}
    
    def get_info_db(self) -> Dict[str, Any]:
        """
        获取 DATABASE 级别信息
        
        INFO FOR DB - 获取当前 database 的 tables, functions, params, users 等
        """
        try:
            result = self.execute_sql("INFO FOR DB;")
            if result and result[0].get('result'):
                return result[0]['result']
        except Exception as e:
            return {"error": str(e)}
        return {}
    
    def get_info_table(self, table: str) -> Dict[str, Any]:
        """
        获取 TABLE 级别信息
        
        INFO FOR TABLE <table> - 获取表的 schema, indexes, events, fields 等
        """
        try:
            result = self.execute_sql(f"INFO FOR TABLE {table};")
            if result and result[0].get('result'):
                return result[0]['result']
        except Exception as e:
            return {"error": str(e)}
        return {}
    
    def get_all_info(self) -> Dict[str, Any]:
        """
        获取所有级别的完整信息
        
        Returns:
            包含 root, namespace, database, tables 信息的字典
        """
        info = {
            "root": self.get_info_root(),
            "namespace": self.get_info_ns(),
            "database": self.get_info_db(),
            "tables": {}
        }
        
        # 获取所有表的信息
        db_info = info["database"]
        if isinstance(db_info, dict) and "tables" in db_info:
            tables = db_info.get("tables", {})
            if isinstance(tables, dict):
                for table_name in tables.keys():
                    info["tables"][table_name] = self.get_info_table(table_name)
        
        return info
    
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
    """缓存现有数据，用于随机选择进行心跳更新和查询"""
    
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
    统一混合负载测试
    
    所有操作类型混合执行，定期检查健康状态
    """
    
    def __init__(self):
        self.client = SurrealDBClient()
        self.data_pool = DataPool(self.client)
        
        # 统计数据
        self.op_stats = OperationStats()
        self.latency_stats = LatencyStats()
        self.health_records: List[HealthStatus] = []
        
        # SurrealDB INFO 信息
        self.info_before: Dict[str, Any] = {}
        self.info_after: Dict[str, Any] = {}
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
    
    def collect_db_info(self) -> Tuple[Dict[str, Any], Dict[str, int]]:
        """
        收集数据库 INFO 信息和表记录数
        
        Returns:
            (info_dict, table_counts_dict)
        """
        info = self.client.get_all_info()
        
        # 获取各表记录数
        table_counts = {}
        tables = ["pod", "service", "pod_with_service"]
        for table in tables:
            table_counts[table] = self.client.get_table_count(table)
        
        return info, table_counts
    
    def print_db_info(self, info: Dict[str, Any], table_counts: Dict[str, int], title: str = "DATABASE INFO"):
        """
        打印数据库 INFO 信息
        """
        print(f"\n{'─' * 70}")
        print(title)
        print(f"{'─' * 70}")
        
        # Namespace 信息
        ns_info = info.get("namespace", {})
        if ns_info and not ns_info.get("error"):
            print(f"\n  [NAMESPACE: {SURREAL_NS}]")
            if "databases" in ns_info:
                dbs = ns_info.get("databases", {})
                print(f"    Databases: {len(dbs)}")
                for db_name in list(dbs.keys())[:5]:
                    print(f"      - {db_name}")
            if "users" in ns_info:
                users = ns_info.get("users", {})
                print(f"    Users: {len(users)}")
        
        # Database 信息
        db_info = info.get("database", {})
        if db_info and not db_info.get("error"):
            print(f"\n  [DATABASE: {SURREAL_DB}]")
            
            # Tables
            tables = db_info.get("tables", {})
            print(f"    Tables: {len(tables)}")
            for table_name, table_def in list(tables.items())[:10]:
                count = table_counts.get(table_name, "?")
                print(f"      - {table_name}: {count} records")
            
            # Functions
            functions = db_info.get("functions", {})
            if functions:
                print(f"    Functions: {len(functions)}")
                for fn_name in list(functions.keys())[:10]:
                    print(f"      - {fn_name}")
            
            # Params
            params = db_info.get("params", {})
            if params:
                print(f"    Params: {len(params)}")
            
            # Users
            users = db_info.get("users", {})
            if users:
                print(f"    Users: {len(users)}")
        
        # Table 详细信息（索引等）
        tables_info = info.get("tables", {})
        if tables_info:
            print(f"\n  [TABLE DETAILS]")
            for table_name, table_info in tables_info.items():
                if table_info and not table_info.get("error"):
                    print(f"    {table_name}:")
                    
                    # Indexes
                    indexes = table_info.get("indexes", {})
                    if indexes:
                        print(f"      Indexes: {len(indexes)}")
                        for idx_name, idx_def in indexes.items():
                            # 简化显示索引定义
                            idx_str = str(idx_def)[:80] + "..." if len(str(idx_def)) > 80 else str(idx_def)
                            print(f"        - {idx_name}: {idx_str}")
                    
                    # Fields
                    fields = table_info.get("fields", {})
                    if fields:
                        print(f"      Fields: {len(fields)}")
                    
                    # Events
                    events = table_info.get("events", {})
                    if events:
                        print(f"      Events: {len(events)}")
    
    def _escape_string(self, s: str) -> str:
        """转义字符串中的单引号"""
        return str(s).replace("'", "\\'")
    
    # =========================================================================
    # SQL 生成
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
    
    def _sql_heartbeat_pod(self, pod: Dict) -> str:
        """Pod 心跳更新"""
        now_ms = int(time.time() * 1000)
        return f"""
        fn::upsert_pod(
            {{ bcs_cluster_id: '{self._escape_string(pod.get('bcs_cluster_id', ''))}', 
               namespace: '{self._escape_string(pod.get('namespace', ''))}', 
               pod: '{self._escape_string(pod.get('pod', ''))}' }},
            {now_ms},
            {TOLERANCE_TIME_MS}
        );
        """
    
    def _sql_heartbeat_service(self, service: Dict) -> str:
        """Service 心跳更新"""
        now_ms = int(time.time() * 1000)
        return f"""
        fn::upsert_service(
            {{ bcs_cluster_id: '{self._escape_string(service.get('bcs_cluster_id', ''))}', 
               namespace: '{self._escape_string(service.get('namespace', ''))}', 
               service: '{self._escape_string(service.get('service', ''))}' }},
            {now_ms},
            {TOLERANCE_TIME_MS}
        );
        """
    
    def _sql_write_new_pod(self, suffix: str) -> str:
        """写入新 pod"""
        now_ms = int(time.time() * 1000)
        return f"""
        fn::upsert_pod(
            {{ bcs_cluster_id: 'BENCH-NEW', namespace: 'bench-new', pod: 'new-pod-{suffix}' }},
            {now_ms},
            {TOLERANCE_TIME_MS}
        );
        """
    
    def _sql_write_new_service(self, suffix: str) -> str:
        """写入新 service"""
        now_ms = int(time.time() * 1000)
        return f"""
        fn::upsert_service(
            {{ bcs_cluster_id: 'BENCH-NEW', namespace: 'bench-new', service: 'new-svc-{suffix}' }},
            {now_ms},
            {TOLERANCE_TIME_MS}
        );
        """
    
    # =========================================================================
    # 健康检查线程
    # =========================================================================
    
    def _health_check_worker(self, interval: float):
        """
        定期健康检查线程
        
        Args:
            interval: 检查间隔（秒）
        """
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
                    
                    # 更新本地统计
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
        logger.info("=" * 60)
        logger.info("SurrealDB Unified Mixed Load Benchmark")
        logger.info("=" * 60)
        logger.info(f"Configuration:")
        logger.info(f"  URL:              {SURREAL_URL}")
        logger.info(f"  Concurrency:      {concurrency} workers")
        logger.info(f"  Duration:         {duration} seconds")
        logger.info(f"  Read Ratio:       {read_ratio}%")
        logger.info(f"  Heartbeat Ratio:  {heartbeat_ratio}% (of writes)")
        logger.info(f"  Health Interval:  {health_interval}s")
        logger.info("")
        
        # 收集测试前的 DB INFO
        logger.info("Collecting database info before benchmark...")
        self.info_before, self.table_counts_before = self.collect_db_info()
        
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
        logger.info("-" * 60)
        
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
        
        # 收集测试后的 DB INFO
        logger.info("Collecting database info after benchmark...")
        self.info_after, self.table_counts_after = self.collect_db_info()
        
        logger.info("-" * 60)
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
        print("BENCHMARK RESULTS")
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
        print(f"  Read Operations:  {self.op_stats.total_reads():,} ({self.op_stats.total_reads()/total_ops*100:.1f}%)")
        print(f"    - Single:       {self.op_stats.read_single:,}")
        print(f"    - Batch:        {self.op_stats.read_batch:,}")
        print(f"    - Relation:     {self.op_stats.read_relation:,}")
        print(f"  Write Operations: {self.op_stats.total_writes():,} ({self.op_stats.total_writes()/total_ops*100:.1f}%)")
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
        
        if self.latency_stats.write_latencies:
            write = self.latency_stats.write_stats()
            print(f"  Write ({len(self.latency_stats.write_latencies):,} samples):")
            print(f"    p50: {write['p50']:.2f}  p95: {write['p95']:.2f}  p99: {write['p99']:.2f}")
        
        if self.latency_stats.heartbeat_latencies:
            hb = self.latency_stats.heartbeat_stats()
            print(f"  Heartbeat ({len(self.latency_stats.heartbeat_latencies):,} samples):")
            print(f"    p50: {hb['p50']:.2f}  p95: {hb['p95']:.2f}  p99: {hb['p99']:.2f}")
        
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
        
        # 数据库 INFO（测试前）
        if self.info_before:
            self.print_db_info(self.info_before, self.table_counts_before, "DATABASE INFO (BEFORE)")
        
        # 数据库 INFO（测试后）及变化
        if self.info_after:
            self.print_db_info(self.info_after, self.table_counts_after, "DATABASE INFO (AFTER)")
            
            # 打印记录数变化
            print(f"\n{'─' * 70}")
            print("TABLE RECORD CHANGES")
            print(f"{'─' * 70}")
            for table in ["pod", "service", "pod_with_service"]:
                before = self.table_counts_before.get(table, 0)
                after = self.table_counts_after.get(table, 0)
                diff = after - before
                sign = "+" if diff > 0 else ""
                print(f"  {table}: {before} -> {after} ({sign}{diff})")
        
        print("\n" + "=" * 70)
    
    def export_json(self, filename: str):
        """导出结果到 JSON 文件"""
        total_time = self.end_time - self.start_time
        total_ops = self.op_stats.total_ops()
        
        data = {
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
            "db_info": {
                "before": {
                    "info": self.info_before,
                    "table_counts": self.table_counts_before
                },
                "after": {
                    "info": self.info_after,
                    "table_counts": self.table_counts_after
                },
                "changes": {
                    table: {
                        "before": self.table_counts_before.get(table, 0),
                        "after": self.table_counts_after.get(table, 0),
                        "diff": self.table_counts_after.get(table, 0) - self.table_counts_before.get(table, 0)
                    }
                    for table in ["pod", "service", "pod_with_service"]
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
        description='SurrealDB Unified Mixed Load Benchmark',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 003.benchmark_surrealdb.py
  python 003.benchmark_surrealdb.py --concurrency 50 --duration 120
  python 003.benchmark_surrealdb.py --read-ratio 80 --health-interval 10
  python 003.benchmark_surrealdb.py --output result.json
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
    
    # Disable SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
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
