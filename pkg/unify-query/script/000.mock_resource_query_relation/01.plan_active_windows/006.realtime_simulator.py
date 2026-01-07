#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Realtime Simulator - Plan 01: Active Windows

模拟真实场景的数据流：
1. 持续从 unify-query API 获取新数据并导入
2. 同时定期刷新已有资源的心跳
3. 模拟资源的生命周期（新增、存活、消亡）

默认行为:
    - 每 5 分钟从 API 获取 100 条新数据
    - 每 30 秒刷新心跳，30% 的资源不刷新（模拟消亡）
    - 无限运行直到手动停止

场景模拟:
    - 新资源不断被发现并写入 (模拟 Pod 创建)
    - 已有资源定期发送心跳 (模拟 Pod 存活)
    - 30% 资源停止心跳后自然过期 (模拟 Pod 删除)

架构:
    ┌─────────────────────────────────────────────────────────────┐
    │                   Realtime Simulator                        │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
    │  │ Data Fetcher│  │  Heartbeat  │  │   Stats Reporter    │ │
    │  │   Thread    │  │   Thread    │  │      Thread         │ │
    │  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
    │         │                │                    │             │
    │         ▼                ▼                    ▼             │
    │  ┌──────────────────────────────────────────────────────┐  │
    │  │                    SurrealDB                          │  │
    │  │   (Event 自动管理 active_windows 生命周期)            │  │
    │  └──────────────────────────────────────────────────────┘  │
    └─────────────────────────────────────────────────────────────┘

Usage:
    # 默认模式: 5分钟获取100条数据, 30秒刷新心跳, 30%消亡率, 无限运行
    python 006.realtime_simulator.py

    # 只运行数据获取 (不刷新心跳)
    python 006.realtime_simulator.py --no-heartbeat

    # 只运行心跳刷新 (不获取新数据)
    python 006.realtime_simulator.py --no-fetch

    # 自定义间隔
    python 006.realtime_simulator.py --fetch-interval 60 --heartbeat-interval 30

    # 指定运行时长 (1小时后停止)
    python 006.realtime_simulator.py --duration 3600

    # 使用模拟数据而非真实API
    python 006.realtime_simulator.py --mock

    # 自定义消亡率 (50%的资源停止心跳)
    python 006.realtime_simulator.py --churn-rate 0.5
"""

import argparse
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

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


_script_dir = os.path.dirname(__file__)
_config_file = os.path.join(_script_dir, 'config.yaml')
_schema_file = os.path.join(_script_dir, 'schema.sql')
_config = _load_yaml_config(_config_file)

# SurrealDB 配置
_surreal_cfg = _config.get('surreal_db', {})
SURREAL_URL = _surreal_cfg.get('url', 'http://localhost:8000')
SURREAL_USER = _surreal_cfg.get('username', 'root')
SURREAL_PASS = _surreal_cfg.get('password', 'root')
SURREAL_NS = _surreal_cfg.get('namespace', 'test')
SURREAL_DB = _surreal_cfg.get('database', 'test')

# Unify-Query API 配置
_unify_cfg = _config.get('unify_query', {})
UNIFY_QUERY_BASE_URL = _unify_cfg.get('url', '').rsplit('/query/ts', 1)[0]
UNIFY_QUERY_HEADERS = {
    'Content-Type': 'application/json',
    'X-Bkapi-Authorization': json.dumps({
        'bk_app_code': _unify_cfg.get('app_code', ''),
        'bk_app_secret': _unify_cfg.get('app_secret', ''),
        'bk_username': _unify_cfg.get('username', 'admin')
    }),
    'X-Bk-Scope-Space-Uid': _unify_cfg.get('space_uid', '')
}

# 默认配置
DEFAULT_FETCH_INTERVAL = 300     # 数据获取间隔（秒）- 5分钟
DEFAULT_FETCH_LIMIT = 100        # 每次获取数据条数
DEFAULT_HEARTBEAT_INTERVAL = 30  # 心跳刷新间隔（秒）
DEFAULT_CHURN_RATE = 0.3         # 默认消亡率 30%（每次心跳刷新时30%的资源不刷新）
DEFAULT_STATS_INTERVAL = 10      # 统计报告间隔（秒）
DEFAULT_BATCH_SIZE = 100         # 批量处理大小
TIME_RANGE = 86400               # 查询时间范围（秒）
TOLERANCE_TIME_MS = 600000       # 生命周期容忍时间（毫秒）- 10分钟

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SimulatorStats:
    """模拟器统计"""
    # 数据获取统计
    fetch_cycles: int = 0
    records_fetched: int = 0
    new_records_imported: int = 0
    
    # 心跳刷新统计
    heartbeat_cycles: int = 0
    heartbeats_sent: int = 0
    heartbeats_skipped: int = 0  # 模拟消亡的资源
    
    # 错误统计
    fetch_errors: int = 0
    heartbeat_errors: int = 0
    
    # 时间统计
    start_time: float = 0
    last_fetch_time: float = 0
    last_heartbeat_time: float = 0
    
    # 资源统计
    total_pods: int = 0
    total_services: int = 0
    total_relations: int = 0
    active_pods: int = 0  # 正在发送心跳的 Pod
    churned_pods: int = 0  # 已停止心跳的 Pod
    
    def duration(self) -> float:
        return time.time() - self.start_time if self.start_time > 0 else 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "duration_sec": round(self.duration(), 1),
            "fetch": {
                "cycles": self.fetch_cycles,
                "records_fetched": self.records_fetched,
                "new_records_imported": self.new_records_imported,
                "errors": self.fetch_errors
            },
            "heartbeat": {
                "cycles": self.heartbeat_cycles,
                "heartbeats_sent": self.heartbeats_sent,
                "heartbeats_skipped": self.heartbeats_skipped,
                "errors": self.heartbeat_errors
            },
            "resources": {
                "total_pods": self.total_pods,
                "total_services": self.total_services,
                "total_relations": self.total_relations,
                "active_pods": self.active_pods,
                "churned_pods": self.churned_pods
            }
        }


# ============================================================================
# SurrealDB Client
# ============================================================================

class SurrealDBClient:
    """SurrealDB HTTP REST API client"""

    def __init__(self):
        self.url = SURREAL_URL
        self.auth = (SURREAL_USER, SURREAL_PASS)
        self.namespace = SURREAL_NS
        self.database = SURREAL_DB
        self.session = requests.Session()
        self.session.verify = False
        self._lock = threading.Lock()

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query via HTTP REST API (thread-safe)"""
        full_sql = f"USE NS {self.namespace} DB {self.database}; {sql}"

        with self._lock:
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
                # Ignore "already exists" errors
                if 'already exists' not in str(error_detail).lower():
                    raise Exception(f"SQL error: {error_detail}")

        return results[1:] if len(results) > 1 else results

    def datetime_to_ms(self, dt: datetime = None) -> int:
        """Convert datetime to milliseconds timestamp"""
        if dt is None:
            dt = datetime.utcnow()
        return int(dt.timestamp() * 1000)

    def _escape_value(self, v: Any) -> str:
        """Escape value for SurrealDB"""
        if isinstance(v, (int, float)):
            return str(v)
        else:
            v_escaped = str(v).replace("'", "\\'")
            return f"'{v_escaped}'"

    def _build_record_id(self, table: str, dimensions: Dict[str, Any]) -> str:
        """Build deterministic record ID"""
        sorted_items = sorted(dimensions.items())
        kv_parts = [f"{k}={v}" for k, v in sorted_items]
        kv_str = ",".join(kv_parts)
        return f"{table}:⟨{kv_str}⟩"

    def upsert_resource(self, table: str, dimensions: Dict[str, Any], end_time: int = None) -> bool:
        """
        Upsert a resource with end_time for lifecycle event trigger.
        
        Args:
            table: Table name
            dimensions: Resource dimension fields (used for ID generation)
            end_time: Timestamp in milliseconds. If None, uses current time.
                      REQUIRED by schema Event to trigger lifecycle management.
        """
        record_id = self._build_record_id(table, dimensions)
        
        # Build SET clause with dimensions
        set_parts = [f"{k}: {self._escape_value(v)}" for k, v in dimensions.items()]
        
        # Add end_time (required by schema Event)
        if end_time is None:
            end_time = self.datetime_to_ms()
        set_parts.append(f"end_time: {end_time}")
        
        set_clause = ", ".join(set_parts)
        sql = f"UPSERT {record_id} MERGE {{ {set_clause} }};"
        
        try:
            self.execute_sql(sql)
            return True
        except Exception as e:
            logger.debug(f"upsert {table} failed: {e}")
            return False

    def upsert_relation(
        self,
        relation_table: str,
        from_table: str,
        from_dimensions: Dict[str, Any],
        to_table: str,
        to_dimensions: Dict[str, Any],
        end_time: int = None
    ) -> bool:
        """
        Upsert a relation with end_time for lifecycle event trigger.
        
        Args:
            relation_table: Relation table name (e.g., "pod_with_service")
            from_table: Source node table name
            from_dimensions: Source node dimension fields
            to_table: Target node table name
            to_dimensions: Target node dimension fields
            end_time: Timestamp in milliseconds. If None, uses current time.
                      REQUIRED by schema Event to trigger lifecycle management.
        """
        from_id = self._build_record_id(from_table, from_dimensions)
        to_id = self._build_record_id(to_table, to_dimensions)
        
        # Add end_time (required by schema Event)
        if end_time is None:
            end_time = self.datetime_to_ms()
        
        sql = f"RETURN fn::upsert_relation('{relation_table}', {from_id}, {to_id}, {end_time});"
        
        try:
            self.execute_sql(sql)
            return True
        except Exception as e:
            logger.debug(f"upsert relation {relation_table} failed: {e}")
            return False

    def batch_upsert_resources(self, table: str, resources: List[Dict[str, Any]], end_time: int = None) -> int:
        """
        Batch upsert resources with end_time for lifecycle event trigger.
        
        Args:
            table: Table name
            resources: List of resource dimension dicts
            end_time: Timestamp in milliseconds. If None, uses current time.
                      REQUIRED by schema Event to trigger lifecycle management.
        """
        if not resources:
            return 0
        
        # Use same end_time for all resources in this batch
        if end_time is None:
            end_time = self.datetime_to_ms()
        
        success_count = 0
        sql_parts = []
        
        for dimensions in resources:
            record_id = self._build_record_id(table, dimensions)
            set_parts = [f"{k}: {self._escape_value(v)}" for k, v in dimensions.items()]
            # Add end_time (required by schema Event)
            set_parts.append(f"end_time: {end_time}")
            set_clause = ", ".join(set_parts)
            sql_parts.append(f"UPSERT {record_id} MERGE {{ {set_clause} }};")
        
        # Execute in batches
        for i in range(0, len(sql_parts), DEFAULT_BATCH_SIZE):
            batch_sql = "\n".join(sql_parts[i:i + DEFAULT_BATCH_SIZE])
            try:
                self.execute_sql(batch_sql)
                success_count += min(DEFAULT_BATCH_SIZE, len(sql_parts) - i)
            except Exception as e:
                logger.warning(f"batch upsert {table} failed: {e}")
        
        return success_count

    def get_table_count(self, table: str) -> int:
        """Get table record count"""
        try:
            result = self.execute_sql(f"SELECT count() FROM {table} GROUP ALL;")
            if result and result[0].get('result'):
                data = result[0]['result']
                if data and len(data) > 0:
                    return data[0].get('count', 0)
        except Exception:
            pass
        return 0

    def get_table_records(self, table: str, limit: int = 10000) -> List[Dict[str, Any]]:
        """Get table records"""
        try:
            result = self.execute_sql(f"SELECT * FROM {table} LIMIT {limit};")
            if result and result[0].get('result'):
                return result[0]['result']
        except Exception as e:
            logger.warning(f"Failed to get records from {table}: {e}")
        return []

    def health_check(self) -> bool:
        """Check SurrealDB health"""
        try:
            response = self.session.get(f"{self.url}/health", timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    def init_schema(self) -> bool:
        """Initialize schema from schema.sql"""
        if not os.path.exists(_schema_file):
            logger.warning(f"Schema file not found: {_schema_file}")
            return False
        
        with open(_schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        schema_sql = schema_sql.replace('{tolerance_time_ms}', str(TOLERANCE_TIME_MS))
        
        try:
            self.execute_sql(schema_sql)
            logger.info("Schema initialized")
            return True
        except Exception as e:
            logger.warning(f"Schema init failed (may already exist): {e}")
            return True  # Continue even if schema exists


# ============================================================================
# Data Fetcher - 从 unify-query API 获取数据
# ============================================================================

class DataFetcher:
    """数据获取器 - 从 unify-query API 获取真实数据"""

    def __init__(self, client: SurrealDBClient, use_mock: bool = False):
        self.client = client
        self.use_mock = use_mock
        self.seen_keys: Set[str] = set()  # 已导入的记录 key
        self._mock_counter = 0

    def _query_ts_data(self, limit: int = 10000) -> List[Dict]:
        """从 unify-query API 获取数据"""
        if not UNIFY_QUERY_BASE_URL:
            logger.warning("unify-query API not configured")
            return []
        
        url = f"{UNIFY_QUERY_BASE_URL}/query/ts"
        now = int(time.time())
        
        data = {
            'query_list': [{
                'data_source': 'bkmonitor',
                'field_name': 'pod_with_service_relation',
                'is_regexp': False,
                'function': [{
                    'method': 'count',
                    'dimensions': ['service', 'bcs_cluster_id', 'namespace', 'pod']
                }],
                'time_aggregation': {},
                'reference_name': 'a',
                'limit': limit,
                'conditions': {'field_list': []}
            }],
            'metric_merge': 'a',
            'start_time': str(now - TIME_RANGE),
            'end_time': str(now),
            'step': '60s',
            'timezone': 'Asia/Shanghai',
            'instant': True
        }
        
        try:
            resp = requests.post(
                url, json=data, headers=UNIFY_QUERY_HEADERS, 
                timeout=120, verify=False
            )
            if resp.status_code == 200:
                result = resp.json()
                series_list = result.get('series', [])
                
                relations = []
                for series in series_list:
                    group_keys = series.get('group_keys', [])
                    group_values = series.get('group_values', [])
                    
                    if len(group_keys) != len(group_values):
                        continue
                    
                    record = dict(zip(group_keys, group_values))
                    if all(k in record for k in ['bcs_cluster_id', 'namespace', 'pod', 'service']):
                        relations.append(record)
                
                return relations
            else:
                logger.warning(f"API query failed: {resp.status_code}")
        except Exception as e:
            logger.warning(f"API query error: {e}")
        
        return []

    def _generate_mock_data(self, count: int = 100) -> List[Dict]:
        """生成模拟数据"""
        relations = []
        for i in range(count):
            self._mock_counter += 1
            relations.append({
                'bcs_cluster_id': 'BCS-K8S-MOCK',
                'namespace': f'ns-{random.randint(1, 10)}',
                'pod': f'pod-{self._mock_counter:06d}',
                'service': f'svc-{random.choice(["api", "web", "worker", "gateway"])}'
            })
        return relations

    def fetch_and_import(self, limit: int = DEFAULT_FETCH_LIMIT) -> Tuple[int, int]:
        """
        获取数据并导入
        
        Returns:
            (fetched_count, new_imported_count)
        """
        # 获取数据
        if self.use_mock:
            relations = self._generate_mock_data(min(limit, 100))
        else:
            relations = self._query_ts_data(limit)
        
        if not relations:
            return 0, 0
        
        fetched_count = len(relations)
        new_count = 0
        
        # 过滤已存在的记录
        new_relations = []
        for r in relations:
            key = f"{r['bcs_cluster_id']}|{r['namespace']}|{r['pod']}|{r['service']}"
            if key not in self.seen_keys:
                self.seen_keys.add(key)
                new_relations.append(r)
        
        if not new_relations:
            return fetched_count, 0
        
        # 批量导入
        # 1. 导入 Pods
        pods = []
        for r in new_relations:
            pods.append({
                'bcs_cluster_id': r['bcs_cluster_id'],
                'namespace': r['namespace'],
                'pod': r['pod']
            })
        
        # 去重
        unique_pods = {f"{p['bcs_cluster_id']}|{p['namespace']}|{p['pod']}": p for p in pods}
        self.client.batch_upsert_resources('pod', list(unique_pods.values()))
        
        # 2. 导入 Services
        services = []
        for r in new_relations:
            services.append({
                'bcs_cluster_id': r['bcs_cluster_id'],
                'namespace': r['namespace'],
                'service': r['service']
            })
        
        unique_services = {f"{s['bcs_cluster_id']}|{s['namespace']}|{s['service']}": s for s in services}
        self.client.batch_upsert_resources('service', list(unique_services.values()))
        
        # 3. 导入关系
        for r in new_relations:
            pod_data = {
                'bcs_cluster_id': r['bcs_cluster_id'],
                'namespace': r['namespace'],
                'pod': r['pod']
            }
            service_data = {
                'bcs_cluster_id': r['bcs_cluster_id'],
                'namespace': r['namespace'],
                'service': r['service']
            }
            if self.client.upsert_relation('pod_with_service', 'pod', pod_data, 'service', service_data):
                new_count += 1
        
        return fetched_count, new_count


# ============================================================================
# Heartbeat Refresher - 心跳刷新器
# ============================================================================

class HeartbeatRefresher:
    """心跳刷新器"""

    def __init__(
        self, 
        client: SurrealDBClient, 
        churn_rate: float = 0.0,
        sample_ratio: float = 1.0
    ):
        """
        Args:
            client: SurrealDB 客户端
            churn_rate: 资源消亡率 (0.0-1.0)，每次刷新时有多少比例的资源停止心跳
            sample_ratio: 采样比例 (0.0-1.0)
        """
        self.client = client
        self.churn_rate = min(max(churn_rate, 0.0), 1.0)
        self.sample_ratio = min(max(sample_ratio, 0.0), 1.0)
        self.churned_keys: Set[str] = set()  # 已停止心跳的资源
        self._cached_pods: List[Dict] = []
        self._cached_services: List[Dict] = []
        self._last_cache_time: float = 0
        self._cache_ttl: float = 60  # 缓存 TTL

    def _refresh_cache(self):
        """刷新资源缓存"""
        now = time.time()
        if now - self._last_cache_time < self._cache_ttl:
            return
        
        self._cached_pods = self.client.get_table_records('pod', limit=50000)
        self._cached_services = self.client.get_table_records('service', limit=10000)
        self._last_cache_time = now
        logger.debug(f"Cache refreshed: {len(self._cached_pods)} pods, {len(self._cached_services)} services")

    def _build_pod_key(self, pod: Dict) -> str:
        """构建 Pod 唯一键"""
        return f"{pod.get('bcs_cluster_id', '')}|{pod.get('namespace', '')}|{pod.get('pod', '')}"

    def _build_service_key(self, svc: Dict) -> str:
        """构建 Service 唯一键"""
        return f"{svc.get('bcs_cluster_id', '')}|{svc.get('namespace', '')}|{svc.get('service', '')}"

    def refresh_heartbeats(self) -> Tuple[int, int, int]:
        """
        刷新心跳
        
        Returns:
            (sent_count, skipped_count, error_count)
        """
        self._refresh_cache()
        
        sent_count = 0
        skipped_count = 0
        error_count = 0
        
        # 刷新 Pods
        pods_to_refresh = self._cached_pods
        if self.sample_ratio < 1.0:
            sample_size = max(1, int(len(pods_to_refresh) * self.sample_ratio))
            pods_to_refresh = random.sample(pods_to_refresh, sample_size)
        
        pod_batch = []
        for pod in pods_to_refresh:
            key = self._build_pod_key(pod)
            
            # 检查是否已停止心跳
            if key in self.churned_keys:
                skipped_count += 1
                continue
            
            # 模拟资源消亡
            if self.churn_rate > 0 and random.random() < self.churn_rate:
                self.churned_keys.add(key)
                skipped_count += 1
                continue
            
            pod_batch.append({
                'bcs_cluster_id': pod.get('bcs_cluster_id', ''),
                'namespace': pod.get('namespace', ''),
                'pod': pod.get('pod', '')
            })
        
        if pod_batch:
            count = self.client.batch_upsert_resources('pod', pod_batch)
            sent_count += count
            if count < len(pod_batch):
                error_count += len(pod_batch) - count
        
        # 刷新 Services
        services_to_refresh = self._cached_services
        if self.sample_ratio < 1.0:
            sample_size = max(1, int(len(services_to_refresh) * self.sample_ratio))
            services_to_refresh = random.sample(services_to_refresh, sample_size)
        
        service_batch = []
        for svc in services_to_refresh:
            service_batch.append({
                'bcs_cluster_id': svc.get('bcs_cluster_id', ''),
                'namespace': svc.get('namespace', ''),
                'service': svc.get('service', '')
            })
        
        if service_batch:
            count = self.client.batch_upsert_resources('service', service_batch)
            sent_count += count
            if count < len(service_batch):
                error_count += len(service_batch) - count
        
        return sent_count, skipped_count, error_count

    def get_churned_count(self) -> int:
        """获取已停止心跳的资源数"""
        return len(self.churned_keys)


# ============================================================================
# Realtime Simulator
# ============================================================================

class RealtimeSimulator:
    """实时模拟器"""

    def __init__(
        self,
        fetch_interval: int = DEFAULT_FETCH_INTERVAL,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
        stats_interval: int = DEFAULT_STATS_INTERVAL,
        enable_fetch: bool = True,
        enable_heartbeat: bool = True,
        use_mock: bool = False,
        churn_rate: float = 0.0,
        sample_ratio: float = 1.0,
        duration: Optional[int] = None
    ):
        self.fetch_interval = fetch_interval
        self.heartbeat_interval = heartbeat_interval
        self.stats_interval = stats_interval
        self.enable_fetch = enable_fetch
        self.enable_heartbeat = enable_heartbeat
        self.duration = duration
        
        self.client = SurrealDBClient()
        self.fetcher = DataFetcher(self.client, use_mock=use_mock)
        self.refresher = HeartbeatRefresher(
            self.client, 
            churn_rate=churn_rate,
            sample_ratio=sample_ratio
        )
        
        self.stats = SimulatorStats()
        self._stop_flag = threading.Event()
        self._stats_lock = threading.Lock()

    def _signal_handler(self, signum, frame):
        """信号处理"""
        logger.info("\nReceived stop signal, shutting down...")
        self._stop_flag.set()

    def _fetch_worker(self):
        """数据获取工作线程"""
        logger.info("Data fetcher started")
        
        while not self._stop_flag.is_set():
            try:
                fetched, imported = self.fetcher.fetch_and_import()
                
                with self._stats_lock:
                    self.stats.fetch_cycles += 1
                    self.stats.records_fetched += fetched
                    self.stats.new_records_imported += imported
                    self.stats.last_fetch_time = time.time()
                
                if imported > 0:
                    logger.info(f"[Fetch] Fetched {fetched}, imported {imported} new records")
                else:
                    logger.debug(f"[Fetch] Fetched {fetched}, no new records")
                    
            except Exception as e:
                with self._stats_lock:
                    self.stats.fetch_errors += 1
                logger.warning(f"[Fetch] Error: {e}")
            
            # 等待下次获取
            self._stop_flag.wait(self.fetch_interval)
        
        logger.info("Data fetcher stopped")

    def _heartbeat_worker(self):
        """心跳刷新工作线程"""
        logger.info("Heartbeat refresher started")
        
        while not self._stop_flag.is_set():
            try:
                sent, skipped, errors = self.refresher.refresh_heartbeats()
                
                with self._stats_lock:
                    self.stats.heartbeat_cycles += 1
                    self.stats.heartbeats_sent += sent
                    self.stats.heartbeats_skipped += skipped
                    self.stats.heartbeat_errors += errors
                    self.stats.last_heartbeat_time = time.time()
                    self.stats.churned_pods = self.refresher.get_churned_count()
                
                logger.info(f"[Heartbeat] Sent {sent}, skipped {skipped}, errors {errors}")
                
            except Exception as e:
                with self._stats_lock:
                    self.stats.heartbeat_errors += 1
                logger.warning(f"[Heartbeat] Error: {e}")
            
            # 等待下次刷新
            self._stop_flag.wait(self.heartbeat_interval)
        
        logger.info("Heartbeat refresher stopped")

    def _stats_worker(self):
        """统计报告工作线程"""
        logger.info("Stats reporter started")
        
        while not self._stop_flag.is_set():
            self._stop_flag.wait(self.stats_interval)
            
            if self._stop_flag.is_set():
                break
            
            # 更新资源统计
            with self._stats_lock:
                self.stats.total_pods = self.client.get_table_count('pod')
                self.stats.total_services = self.client.get_table_count('service')
                self.stats.total_relations = self.client.get_table_count('pod_with_service')
                self.stats.active_pods = self.stats.total_pods - self.stats.churned_pods
            
            # 打印统计
            self._print_stats()
        
        logger.info("Stats reporter stopped")

    def _print_stats(self):
        """打印统计信息"""
        with self._stats_lock:
            duration = self.stats.duration()
            
            logger.info("-" * 60)
            logger.info(f"[Stats] Duration: {duration:.0f}s")
            logger.info(f"[Stats] Fetch: cycles={self.stats.fetch_cycles}, "
                       f"fetched={self.stats.records_fetched}, "
                       f"imported={self.stats.new_records_imported}, "
                       f"errors={self.stats.fetch_errors}")
            logger.info(f"[Stats] Heartbeat: cycles={self.stats.heartbeat_cycles}, "
                       f"sent={self.stats.heartbeats_sent}, "
                       f"skipped={self.stats.heartbeats_skipped}, "
                       f"errors={self.stats.heartbeat_errors}")
            logger.info(f"[Stats] Resources: pods={self.stats.total_pods}, "
                       f"services={self.stats.total_services}, "
                       f"relations={self.stats.total_relations}, "
                       f"churned={self.stats.churned_pods}")
            logger.info("-" * 60)

    def run(self) -> int:
        """运行模拟器"""
        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("=" * 70)
        logger.info("Realtime Simulator - Plan 01: Active Windows")
        logger.info("=" * 70)
        logger.info(f"Configuration:")
        logger.info(f"  SurrealDB URL:      {SURREAL_URL}")
        logger.info(f"  Namespace/DB:       {SURREAL_NS}/{SURREAL_DB}")
        logger.info(f"  Fetch Interval:     {self.fetch_interval}s")
        logger.info(f"  Heartbeat Interval: {self.heartbeat_interval}s")
        logger.info(f"  Stats Interval:     {self.stats_interval}s")
        logger.info(f"  Enable Fetch:       {self.enable_fetch}")
        logger.info(f"  Enable Heartbeat:   {self.enable_heartbeat}")
        logger.info(f"  Duration:           {self.duration}s" if self.duration else "  Duration:           Infinite")
        logger.info(f"  Churn Rate:         {self.refresher.churn_rate:.1%}")
        logger.info(f"  Sample Ratio:       {self.refresher.sample_ratio:.1%}")
        logger.info("")

        # 检查连接
        if not self.client.health_check():
            logger.error("Failed to connect to SurrealDB")
            return 1

        logger.info("Connected to SurrealDB")

        # 初始化统计
        self.stats.start_time = time.time()

        # 启动工作线程
        threads = []

        if self.enable_fetch:
            fetch_thread = threading.Thread(
                target=self._fetch_worker,
                name="DataFetcher",
                daemon=True
            )
            threads.append(fetch_thread)
            fetch_thread.start()

        if self.enable_heartbeat:
            heartbeat_thread = threading.Thread(
                target=self._heartbeat_worker,
                name="Heartbeat",
                daemon=True
            )
            threads.append(heartbeat_thread)
            heartbeat_thread.start()

        # 统计报告线程
        stats_thread = threading.Thread(
            target=self._stats_worker,
            name="StatsReporter",
            daemon=True
        )
        threads.append(stats_thread)
        stats_thread.start()

        logger.info(f"Started {len(threads)} worker threads")
        logger.info("Press Ctrl+C to stop")
        logger.info("")

        # 等待完成
        try:
            if self.duration:
                self._stop_flag.wait(self.duration)
                logger.info(f"Duration limit reached ({self.duration}s)")
                self._stop_flag.set()
            else:
                # 无限等待
                while not self._stop_flag.is_set():
                    self._stop_flag.wait(1)
        except Exception as e:
            logger.error(f"Error: {e}")
            self._stop_flag.set()

        # 等待线程结束
        for thread in threads:
            thread.join(timeout=5)

        # 打印最终统计
        logger.info("")
        logger.info("=" * 70)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 70)
        self._print_stats()
        logger.info("=" * 70)

        return 0


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Realtime Simulator - Plan 01: Active Windows',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 默认模式: 5分钟获取100条, 30秒刷新心跳, 30%消亡率, 无限运行
  python 006.realtime_simulator.py

  # 只运行数据获取
  python 006.realtime_simulator.py --no-heartbeat

  # 只运行心跳刷新
  python 006.realtime_simulator.py --no-fetch

  # 自定义间隔 (1分钟获取, 10秒心跳)
  python 006.realtime_simulator.py --fetch-interval 60 --heartbeat-interval 10

  # 使用模拟数据
  python 006.realtime_simulator.py --mock

  # 自定义消亡率 (50%的资源停止心跳)
  python 006.realtime_simulator.py --churn-rate 0.5

  # 运行1小时后停止
  python 006.realtime_simulator.py --duration 3600

  # 初始化 schema 后运行
  python 006.realtime_simulator.py --init-schema
        """
    )

    parser.add_argument('--fetch-interval', type=int, default=DEFAULT_FETCH_INTERVAL,
                        help=f'Data fetch interval in seconds (default: {DEFAULT_FETCH_INTERVAL})')
    parser.add_argument('--heartbeat-interval', type=int, default=DEFAULT_HEARTBEAT_INTERVAL,
                        help=f'Heartbeat refresh interval in seconds (default: {DEFAULT_HEARTBEAT_INTERVAL})')
    parser.add_argument('--stats-interval', type=int, default=DEFAULT_STATS_INTERVAL,
                        help=f'Stats report interval in seconds (default: {DEFAULT_STATS_INTERVAL})')
    parser.add_argument('--no-fetch', action='store_true',
                        help='Disable data fetching')
    parser.add_argument('--no-heartbeat', action='store_true',
                        help='Disable heartbeat refresh')
    parser.add_argument('--mock', action='store_true',
                        help='Use mock data instead of real API')
    parser.add_argument('--churn-rate', type=float, default=DEFAULT_CHURN_RATE,
                        help=f'Resource churn rate 0.0-1.0 (default: {DEFAULT_CHURN_RATE})')
    parser.add_argument('--sample-ratio', type=float, default=1.0,
                        help='Heartbeat sample ratio 0.0-1.0 (default: 1.0)')
    parser.add_argument('--duration', type=int, default=None,
                        help='Total duration in seconds (default: infinite)')
    parser.add_argument('--init-schema', action='store_true',
                        help='Initialize schema before running')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # 初始化 schema
    if args.init_schema:
        client = SurrealDBClient()
        if not client.init_schema():
            logger.error("Failed to initialize schema")
            return 1
        logger.info("Schema initialized successfully")

    # 创建模拟器
    simulator = RealtimeSimulator(
        fetch_interval=args.fetch_interval,
        heartbeat_interval=args.heartbeat_interval,
        stats_interval=args.stats_interval,
        enable_fetch=not args.no_fetch,
        enable_heartbeat=not args.no_heartbeat,
        use_mock=args.mock,
        churn_rate=args.churn_rate,
        sample_ratio=args.sample_ratio,
        duration=args.duration
    )

    return simulator.run()


if __name__ == '__main__':
    sys.exit(main())
