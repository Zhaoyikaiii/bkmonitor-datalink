#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Heartbeat Refresh Script - Plan 01: Active Windows

定时刷新心跳脚本，负责对 SurrealDB 中的资源数据写入新的心跳。
通过定期发送 UPSERT 请求，Event 会自动管理 active_windows 生命周期。

功能:
    1. 定时刷新: 按配置的间隔定期刷新所有资源的心跳
    2. 选择性刷新: 可指定只刷新特定类型的资源
    3. 批量刷新: 支持批量处理以提高效率
    4. 随机采样: 可配置只刷新部分资源（用于模拟真实场景）
    5. 统计报告: 输出刷新统计信息

Write Pattern (Active Windows):
    UPSERT pod:⟨bcs_cluster_id=X,namespace=N,pod=P⟩ MERGE {
        bcs_cluster_id: "X",
        namespace: "N",
        pod: "P"
    };

Usage:
    # 默认模式: 每60秒刷新一次所有资源
    python 005.heartbeat_refresh.py

    # 指定刷新间隔
    python 005.heartbeat_refresh.py --interval 30

    # 只刷新指定资源类型
    python 005.heartbeat_refresh.py --tables pod,service

    # 随机采样刷新 (只刷新50%的资源)
    python 005.heartbeat_refresh.py --sample-ratio 0.5

    # 单次刷新模式 (不循环)
    python 005.heartbeat_refresh.py --once

    # 指定运行时长 (秒)
    python 005.heartbeat_refresh.py --duration 3600

    # 并发刷新
    python 005.heartbeat_refresh.py --concurrency 4

    # 静默模式
    python 005.heartbeat_refresh.py --quiet
"""

import argparse
import logging
import os
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

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
_config = _load_yaml_config(_config_file)

# SurrealDB 配置
_surreal_cfg = _config.get('surreal_db', {})
SURREAL_URL = _surreal_cfg.get('url', 'http://localhost:8000')
SURREAL_USER = _surreal_cfg.get('username', 'root')
SURREAL_PASS = _surreal_cfg.get('password', 'root')
SURREAL_NS = _surreal_cfg.get('namespace', 'test')
SURREAL_DB = _surreal_cfg.get('database', 'test')

# 默认配置
DEFAULT_INTERVAL = 60  # 默认刷新间隔（秒）
DEFAULT_BATCH_SIZE = 100  # 默认批量大小
DEFAULT_SAMPLE_RATIO = 1.0  # 默认采样比例（1.0 = 100%）

# 支持的资源表
RESOURCE_TABLES = [
    'pod', 'service', 'node', 'container', 'deployment', 'replicaset',
    'statefulset', 'daemonset', 'job', 'ingress', 'cluster', 'namespace',
    'system', 'k8s_address', 'domain', 'apm_service', 'apm_service_instance',
    'datasource', 'bklogconfig', 'biz', 'set', 'module', 'host',
    'app_version', 'git_commit', 'environment', 'metric'
]

# 支持的关系表
RELATION_TABLES = [
    'pod_with_service', 'node_with_pod', 'node_with_system',
    'deployment_with_replicaset', 'pod_with_replicaset', 'container_with_pod',
    'job_with_pod', 'pod_with_statefulset', 'daemonset_with_pod',
    'ingress_with_service', 'k8s_address_with_service', 'domain_with_service',
    'apm_service_with_apm_service_instance', 'apm_service_instance_with_pod',
    'apm_service_instance_with_system', 'datasource_with_pod', 'datasource_with_node',
    'bklogconfig_with_datasource', 'biz_with_set', 'module_with_set',
    'host_with_module', 'host_with_system', 'app_version_with_container',
    'app_version_with_system', 'container_with_environment', 'environment_with_system',
    'app_version_with_git_commit', 'pod_to_pod', 'pod_to_system',
    'system_to_pod', 'system_to_system', 'service_to_service'
]

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
class RefreshStats:
    """刷新统计"""
    total_records: int = 0
    refreshed_records: int = 0
    skipped_records: int = 0
    errors: int = 0
    start_time: float = 0
    end_time: float = 0
    tables_processed: Dict[str, int] = field(default_factory=dict)

    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time > 0 else 0

    def success_rate(self) -> float:
        total = self.refreshed_records + self.errors
        return (self.refreshed_records / total * 100) if total > 0 else 0

    def qps(self) -> float:
        duration = self.duration()
        return self.refreshed_records / duration if duration > 0 else 0


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

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query via HTTP REST API"""
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

    def get_table_records(self, table: str, limit: int = 10000) -> List[Dict[str, Any]]:
        """获取表中的所有记录"""
        try:
            result = self.execute_sql(f"SELECT * FROM {table} LIMIT {limit};")
            if result and result[0].get('result'):
                return result[0]['result']
        except Exception as e:
            logger.warning(f"Failed to get records from {table}: {e}")
        return []

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

    def health_check(self) -> bool:
        """检查 SurrealDB 健康状态"""
        try:
            response = self.session.get(f"{self.url}/health", timeout=5)
            return response.status_code == 200
        except Exception:
            return False


# ============================================================================
# Heartbeat Refresher
# ============================================================================

class HeartbeatRefresher:
    """心跳刷新器"""

    def __init__(
        self,
        tables: Optional[List[str]] = None,
        include_relations: bool = False,
        sample_ratio: float = 1.0,
        batch_size: int = DEFAULT_BATCH_SIZE,
        concurrency: int = 1
    ):
        """
        初始化心跳刷新器

        Args:
            tables: 要刷新的表列表，None 表示所有表
            include_relations: 是否包含关系表
            sample_ratio: 采样比例 (0.0-1.0)
            batch_size: 批量处理大小
            concurrency: 并发数
        """
        self.client = SurrealDBClient()
        self.tables = tables or RESOURCE_TABLES.copy()
        self.include_relations = include_relations
        self.sample_ratio = min(max(sample_ratio, 0.0), 1.0)
        self.batch_size = batch_size
        self.concurrency = concurrency

        # 如果包含关系表，添加到列表
        if include_relations:
            self.tables.extend(RELATION_TABLES)

        # 过滤掉不存在的表
        self._filter_existing_tables()

        # 线程安全
        self._stats_lock = threading.Lock()

    def _filter_existing_tables(self):
        """过滤掉不存在或为空的表"""
        existing_tables = []
        for table in self.tables:
            count = self.client.get_table_count(table)
            if count > 0:
                existing_tables.append(table)
        self.tables = existing_tables
        logger.info(f"Found {len(self.tables)} non-empty tables to refresh")

    def _escape_value(self, v: Any) -> str:
        """转义值"""
        if isinstance(v, (int, float)):
            return str(v)
        else:
            v_escaped = str(v).replace("'", "\\'")
            return f"'{v_escaped}'"

    def _build_record_id(self, table: str, dimensions: Dict[str, Any]) -> str:
        """构建确定性记录 ID"""
        sorted_items = sorted(dimensions.items())
        kv_parts = [f"{k}={v}" for k, v in sorted_items]
        kv_str = ",".join(kv_parts)
        return f"{table}:⟨{kv_str}⟩"

    def _get_dimension_fields(self, table: str) -> List[str]:
        """获取表的维度字段（用于构建记录 ID）"""
        # 定义每个表的维度字段
        dimension_map = {
            # Kubernetes
            'pod': ['bcs_cluster_id', 'namespace', 'pod'],
            'service': ['bcs_cluster_id', 'namespace', 'service'],
            'node': ['bcs_cluster_id', 'node'],
            'container': ['bcs_cluster_id', 'namespace', 'pod', 'container'],
            'deployment': ['bcs_cluster_id', 'namespace', 'deployment'],
            'replicaset': ['bcs_cluster_id', 'namespace', 'replicaset'],
            'statefulset': ['bcs_cluster_id', 'namespace', 'statefulset'],
            'daemonset': ['bcs_cluster_id', 'namespace', 'daemonset'],
            'job': ['bcs_cluster_id', 'namespace', 'job'],
            'ingress': ['bcs_cluster_id', 'namespace', 'ingress'],
            'cluster': ['bcs_cluster_id'],
            'namespace': ['bcs_cluster_id', 'namespace'],
            # Network
            'system': ['bk_cloud_id', 'bk_target_ip'],
            'k8s_address': ['bcs_cluster_id', 'address'],
            'domain': ['bcs_cluster_id', 'domain'],
            # APM
            'apm_service': ['apm_application_name', 'apm_service_name'],
            'apm_service_instance': ['apm_application_name', 'apm_service_name', 'apm_service_instance_name'],
            # Data Source
            'datasource': ['bk_data_id'],
            'bklogconfig': ['bklogconfig_namespace', 'bklogconfig_name'],
            # CMDB
            'biz': ['bk_biz_id'],
            'set': ['bk_set_id'],
            'module': ['bk_module_id'],
            'host': ['bk_host_id'],
            # App Version
            'app_version': ['app_name', 'version'],
            'git_commit': ['git_repo', 'commit_id'],
            'environment': ['environment'],
            # Metric
            'metric': ['metric_name'],
        }
        return dimension_map.get(table, [])

    def _build_heartbeat_sql(self, table: str, record: Dict[str, Any]) -> Optional[str]:
        """
        构建心跳 SQL

        对于资源表: 使用 UPSERT MERGE
        对于关系表: 使用 UPDATE
        """
        # 检查是否是关系表
        if table in RELATION_TABLES:
            # 关系表直接 UPDATE
            record_id = record.get('id')
            if not record_id:
                return None
            return f"UPDATE {record_id};"

        # 资源表: 提取维度字段
        dim_fields = self._get_dimension_fields(table)
        if not dim_fields:
            # 未知表，尝试使用 id 直接更新
            record_id = record.get('id')
            if record_id:
                return f"UPDATE {record_id};"
            return None

        # 构建维度字典
        dimensions = {}
        for field in dim_fields:
            if field in record:
                dimensions[field] = record[field]
            else:
                # 缺少必要字段，跳过
                return None

        # 构建 UPSERT SQL
        record_id = self._build_record_id(table, dimensions)
        set_parts = [f"{k}: {self._escape_value(v)}" for k, v in dimensions.items()]
        set_clause = ", ".join(set_parts)

        return f"UPSERT {record_id} MERGE {{ {set_clause} }};"

    def _refresh_table(self, table: str, stats: RefreshStats) -> int:
        """刷新单个表的记录"""
        records = self.client.get_table_records(table)
        if not records:
            return 0

        # 采样
        if self.sample_ratio < 1.0:
            sample_size = max(1, int(len(records) * self.sample_ratio))
            records = random.sample(records, sample_size)

        refreshed = 0
        errors = 0

        # 批量处理
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            sql_parts = []

            for record in batch:
                sql = self._build_heartbeat_sql(table, record)
                if sql:
                    sql_parts.append(sql)

            if sql_parts:
                batch_sql = "\n".join(sql_parts)
                try:
                    self.client.execute_sql(batch_sql)
                    refreshed += len(sql_parts)
                except Exception as e:
                    errors += len(sql_parts)
                    logger.warning(f"Batch refresh failed for {table}: {e}")

        with self._stats_lock:
            stats.refreshed_records += refreshed
            stats.errors += errors
            stats.tables_processed[table] = refreshed

        return refreshed

    def refresh_once(self) -> RefreshStats:
        """执行一次刷新"""
        stats = RefreshStats()
        stats.start_time = time.time()

        logger.info(f"Starting heartbeat refresh for {len(self.tables)} tables...")

        if self.concurrency > 1:
            # 并发刷新
            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                futures = {
                    executor.submit(self._refresh_table, table, stats): table
                    for table in self.tables
                }
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.warning(f"Failed to refresh {table}: {e}")
        else:
            # 顺序刷新
            for table in self.tables:
                try:
                    self._refresh_table(table, stats)
                except Exception as e:
                    logger.warning(f"Failed to refresh {table}: {e}")

        stats.end_time = time.time()
        return stats

    def print_stats(self, stats: RefreshStats, cycle: int = 0):
        """打印统计信息"""
        cycle_str = f"[Cycle {cycle}] " if cycle > 0 else ""
        logger.info(f"{cycle_str}Refresh completed:")
        logger.info(f"  Duration: {stats.duration():.2f}s")
        logger.info(f"  Refreshed: {stats.refreshed_records}")
        logger.info(f"  Errors: {stats.errors}")
        logger.info(f"  Success Rate: {stats.success_rate():.1f}%")
        logger.info(f"  QPS: {stats.qps():.1f}")

        if stats.tables_processed:
            logger.info("  By Table:")
            for table, count in sorted(stats.tables_processed.items()):
                if count > 0:
                    logger.info(f"    {table}: {count}")


# ============================================================================
# Main Runner
# ============================================================================

class HeartbeatRunner:
    """心跳运行器"""

    def __init__(
        self,
        refresher: HeartbeatRefresher,
        interval: int = DEFAULT_INTERVAL,
        duration: Optional[int] = None,
        once: bool = False,
        quiet: bool = False
    ):
        """
        初始化运行器

        Args:
            refresher: 心跳刷新器
            interval: 刷新间隔（秒）
            duration: 运行时长（秒），None 表示无限运行
            once: 是否只运行一次
            quiet: 静默模式
        """
        self.refresher = refresher
        self.interval = interval
        self.duration = duration
        self.once = once
        self.quiet = quiet

        self._stop_flag = threading.Event()
        self._cycle_count = 0
        self._total_stats = RefreshStats()

    def _signal_handler(self, signum, frame):
        """信号处理"""
        logger.info("\nReceived stop signal, shutting down...")
        self._stop_flag.set()

    def run(self):
        """运行心跳刷新"""
        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("=" * 70)
        logger.info("Heartbeat Refresh - Plan 01: Active Windows")
        logger.info("=" * 70)
        logger.info(f"Configuration:")
        logger.info(f"  SurrealDB URL:    {SURREAL_URL}")
        logger.info(f"  Namespace/DB:     {SURREAL_NS}/{SURREAL_DB}")
        logger.info(f"  Interval:         {self.interval}s")
        logger.info(f"  Duration:         {self.duration}s" if self.duration else "  Duration:         Infinite")
        logger.info(f"  Tables:           {len(self.refresher.tables)}")
        logger.info(f"  Sample Ratio:     {self.refresher.sample_ratio:.1%}")
        logger.info(f"  Batch Size:       {self.refresher.batch_size}")
        logger.info(f"  Concurrency:      {self.refresher.concurrency}")
        logger.info(f"  Once Mode:        {self.once}")
        logger.info("")

        # 检查连接
        if not self.refresher.client.health_check():
            logger.error("Failed to connect to SurrealDB")
            return 1

        start_time = time.time()

        try:
            while not self._stop_flag.is_set():
                self._cycle_count += 1
                cycle_start = time.time()

                # 执行刷新
                stats = self.refresher.refresh_once()

                # 累计统计
                self._total_stats.refreshed_records += stats.refreshed_records
                self._total_stats.errors += stats.errors

                # 打印统计
                if not self.quiet:
                    self.refresher.print_stats(stats, self._cycle_count)

                # 单次模式
                if self.once:
                    break

                # 检查运行时长
                if self.duration:
                    elapsed = time.time() - start_time
                    if elapsed >= self.duration:
                        logger.info(f"Duration limit reached ({self.duration}s)")
                        break

                # 等待下次刷新
                cycle_elapsed = time.time() - cycle_start
                wait_time = max(0, self.interval - cycle_elapsed)
                if wait_time > 0:
                    logger.info(f"Waiting {wait_time:.1f}s until next refresh...")
                    self._stop_flag.wait(wait_time)

        except Exception as e:
            logger.error(f"Error during refresh: {e}")
            return 1

        # 打印总结
        total_time = time.time() - start_time
        logger.info("")
        logger.info("=" * 70)
        logger.info("SUMMARY")
        logger.info("=" * 70)
        logger.info(f"  Total Cycles:     {self._cycle_count}")
        logger.info(f"  Total Time:       {total_time:.2f}s")
        logger.info(f"  Total Refreshed:  {self._total_stats.refreshed_records}")
        logger.info(f"  Total Errors:     {self._total_stats.errors}")
        avg_qps = self._total_stats.refreshed_records / total_time if total_time > 0 else 0
        logger.info(f"  Average QPS:      {avg_qps:.1f}")
        logger.info("=" * 70)

        return 0


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Heartbeat Refresh Script - Plan 01: Active Windows',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 默认模式: 每60秒刷新一次
  python 005.heartbeat_refresh.py

  # 指定刷新间隔为30秒
  python 005.heartbeat_refresh.py --interval 30

  # 只刷新 pod 和 service 表
  python 005.heartbeat_refresh.py --tables pod,service

  # 随机采样刷新50%的资源
  python 005.heartbeat_refresh.py --sample-ratio 0.5

  # 单次刷新模式
  python 005.heartbeat_refresh.py --once

  # 运行1小时后停止
  python 005.heartbeat_refresh.py --duration 3600

  # 4线程并发刷新
  python 005.heartbeat_refresh.py --concurrency 4

  # 包含关系表
  python 005.heartbeat_refresh.py --include-relations
        """
    )

    parser.add_argument('--interval', type=int, default=DEFAULT_INTERVAL,
                        help=f'Refresh interval in seconds (default: {DEFAULT_INTERVAL})')
    parser.add_argument('--tables', type=str, default=None,
                        help='Comma-separated list of tables to refresh (default: all resource tables)')
    parser.add_argument('--include-relations', action='store_true',
                        help='Include relation tables in refresh')
    parser.add_argument('--sample-ratio', type=float, default=DEFAULT_SAMPLE_RATIO,
                        help=f'Sample ratio 0.0-1.0 (default: {DEFAULT_SAMPLE_RATIO})')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                        help=f'Batch size for processing (default: {DEFAULT_BATCH_SIZE})')
    parser.add_argument('--concurrency', type=int, default=1,
                        help='Number of concurrent workers (default: 1)')
    parser.add_argument('--duration', type=int, default=None,
                        help='Total duration in seconds (default: infinite)')
    parser.add_argument('--once', action='store_true',
                        help='Run only once and exit')
    parser.add_argument('--quiet', action='store_true',
                        help='Quiet mode, minimal output')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # 解析表列表
    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(',')]

    # 创建刷新器
    refresher = HeartbeatRefresher(
        tables=tables,
        include_relations=args.include_relations,
        sample_ratio=args.sample_ratio,
        batch_size=args.batch_size,
        concurrency=args.concurrency
    )

    # 创建运行器
    runner = HeartbeatRunner(
        refresher=refresher,
        interval=args.interval,
        duration=args.duration,
        once=args.once,
        quiet=args.quiet
    )

    # 运行
    return runner.run()


if __name__ == '__main__':
    sys.exit(main())
