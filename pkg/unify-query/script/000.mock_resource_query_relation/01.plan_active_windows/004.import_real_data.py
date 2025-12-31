#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import Benchmark Data for Plan 01: Active Windows

为 Plan 01 (Active Windows) 方案导入基准测试数据。
与 00.plan_created_at 不同，此方案使用简单的 UPSERT MERGE 语法，
由 SurrealDB Event 自动管理 active_windows 生命周期。

Write Pattern (无需调用函数):
    UPSERT pod:⟨bcs_cluster_id=X,namespace=N,pod=P⟩ MERGE {
        bcs_cluster_id: "X",
        namespace: "N", 
        pod: "P",
        updated_at: <timestamp_ms>
    };

支持两种数据源：
1. 从 unify-query API 获取真实数据
2. 生成模拟数据（用于基准测试）

Usage:
    python 004.import_benchmark_data.py                    # 完整导入流程（模拟数据）
    python 004.import_benchmark_data.py --init             # 仅初始化 schema
    python 004.import_benchmark_data.py --verify           # 仅验证导入结果
    python 004.import_benchmark_data.py --count 10000      # 指定 Pod 数量
    python 004.import_benchmark_data.py --real             # 从 unify-query 获取真实数据
"""

import json
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests

# Disable SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# Configuration
# ============================================================================

def _load_yaml_config(filename: str) -> Dict[str, Any]:
    """加载 YAML 配置文件"""
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

# SurrealDB 配置 (从 config.yaml 读取)
_surreal_cfg = _config.get('surreal_db', {})
SURREAL_URL = _surreal_cfg.get('url', 'http://localhost:8000')
SURREAL_USER = _surreal_cfg.get('username', 'root')
SURREAL_PASS = _surreal_cfg.get('password', 'root')
SURREAL_NS = _surreal_cfg.get('namespace', 'test')
SURREAL_DB = _surreal_cfg.get('database', 'test')

# Unify-Query API 配置 (从 config.yaml 读取)
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

# 导入配置
TIME_RANGE = 86400          # 查询时间范围（秒）
TOLERANCE_TIME_MS = 600000  # 生命周期容忍时间（毫秒）- 10分钟
BATCH_SIZE = 100            # 批量插入大小


# ============================================================================
# Mock Data Configuration (与 002.mock_full_resource_graph.py 一致)
# ============================================================================

class MockConfig:
    """Mock data generation configuration"""
    # CMDB
    BIZ_ID = "2"
    BIZ_NAME = "bkop"
    SET_ID = "100"
    MODULE_ID = "1001"
    HOST_ID = "10001"
    
    # Kubernetes
    CLUSTER_ID = "BCS-K8S-00002"
    NAMESPACE = "bkop"
    
    # Network
    CLOUD_ID = "0"
    
    # APM
    APM_APP_NAME = "bkop-app"
    APM_SERVICE_NAME = "bkop-service"
    
    # Data Source
    DATA_ID = "100147"
    
    # App Version
    APP_NAME = "bkop-image"
    VERSION = "v1.0.0"
    GIT_REPO = "https://github.com/bkop/app"
    COMMIT_ID = "abc123def456"
    ENVIRONMENT = "production"
    
    # Default counts
    SERVICE_LIST = ["api", "web", "worker", "gateway", "scheduler"]
    NUM_NODES = 10
    NUM_SYSTEMS = 20
    NUM_APM_INSTANCES = 5
    NUM_CONTAINERS_PER_POD = 2


# ============================================================================
# SurrealDB Client for Active Windows Schema
# ============================================================================

class SurrealDBClient:
    """SurrealDB HTTP REST API client for Active Windows schema"""

    def __init__(self):
        self.url = SURREAL_URL
        self.username = SURREAL_USER
        self.password = SURREAL_PASS
        self.namespace = SURREAL_NS
        self.database = SURREAL_DB
        self.session = requests.Session()
        self.session.verify = False
        print(f"SurrealDB client: {self.url}/{self.namespace}/{self.database}")

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query via HTTP REST API"""
        full_sql = f"USE NS {self.namespace} DB {self.database}; {sql}"

        response = self.session.post(
            f"{self.url}/sql",
            headers={
                'Content-Type': 'text/plain; charset=utf-8',
                'Accept': 'application/json'
            },
            auth=(self.username, self.password),
            data=full_sql.encode('utf-8')
        )

        if response.status_code != 200:
            raise Exception(f"HTTP error {response.status_code}: {response.text}")

        results = response.json()

        # Check for SQL errors (skip USE statement result)
        # Ignore "already exists" errors for idempotent schema initialization
        for i, result in enumerate(results):
            if result.get('status') == 'ERR':
                error_detail = result.get('detail') or result.get('result', 'Unknown error')
                # Skip "already exists" errors for tables/functions
                if 'already exists' in str(error_detail).lower():
                    continue
                raise Exception(f"SQL error in statement {i}: {error_detail}")

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
        """
        Build deterministic record ID using SurrealDB's object-based ID format.
        Format: table:⟨key1=value1,key2=value2,...⟩
        """
        # Sort keys for deterministic ID
        sorted_items = sorted(dimensions.items())
        kv_parts = [f"{k}={v}" for k, v in sorted_items]
        kv_str = ",".join(kv_parts)
        return f"{table}:⟨{kv_str}⟩"

    def upsert_resource(
        self,
        table: str,
        dimensions: Dict[str, Any],
        now_ms: int = None
    ) -> Dict[str, Any]:
        """
        Upsert a resource using simple UPSERT MERGE syntax.
        Event will automatically manage active_windows.
        
        Args:
            table: Table name (e.g., 'pod', 'service')
            dimensions: Dimension key-value pairs
            now_ms: Current timestamp in milliseconds
        
        Returns:
            The upserted record
        """
        if now_ms is None:
            now_ms = self.datetime_to_ms()
        
        # Build record ID
        record_id = self._build_record_id(table, dimensions)
        
        # Build SET clause
        set_parts = [f"{k}: {self._escape_value(v)}" for k, v in dimensions.items()]
        set_parts.append(f"updated_at: {now_ms}")
        set_clause = ", ".join(set_parts)
        
        sql = f"UPSERT {record_id} MERGE {{ {set_clause} }};"
        
        try:
            result = self.execute_sql(sql)
            if result and result[0].get('result'):
                records = result[0]['result']
                if isinstance(records, list) and records:
                    return records[0]
                elif isinstance(records, dict):
                    return records
        except Exception as e:
            print(f"  upsert {table} 失败: {e}")
        return {}

    def upsert_relation(
        self,
        relation_table: str,
        from_table: str,
        from_dimensions: Dict[str, Any],
        to_table: str,
        to_dimensions: Dict[str, Any],
        now_ms: int = None
    ) -> Dict[str, Any]:
        """
        Upsert a relation between two resources using fn::upsert_relation function.
        
        Args:
            relation_table: Relation table name (e.g., 'pod_with_service')
            from_table: Source table name
            from_dimensions: Source resource dimensions
            to_table: Target table name
            to_dimensions: Target resource dimensions
            now_ms: Current timestamp in milliseconds
        
        Returns:
            The upserted relation record
        """
        if now_ms is None:
            now_ms = self.datetime_to_ms()
        
        # Build endpoint record IDs
        from_id = self._build_record_id(from_table, from_dimensions)
        to_id = self._build_record_id(to_table, to_dimensions)
        
        # Use fn::upsert_relation function - must use RETURN to get result
        sql = f"RETURN fn::upsert_relation('{relation_table}', {from_id}, {to_id}, {now_ms});"
        
        try:
            result = self.execute_sql(sql)
            if result and result[0].get('result'):
                res = result[0]['result']
                if isinstance(res, dict):
                    return res
                elif isinstance(res, list) and res:
                    return res[0]
        except Exception as e:
            print(f"  upsert relation {relation_table} 失败: {e}")
        return {}

    def upsert_static_relation(
        self,
        relation_table: str,
        from_table: str,
        from_data: Dict[str, Any],
        to_table: str,
        to_data: Dict[str, Any],
        now_ms: int = None
    ) -> Tuple[Dict, Dict, Dict]:
        """
        Upsert a static relation with both endpoints.
        
        Returns:
            Tuple of (from_result, to_result, relation_result)
        """
        if now_ms is None:
            now_ms = self.datetime_to_ms()
        
        # Step 1: Upsert both endpoints
        from_result = self.upsert_resource(from_table, from_data, now_ms)
        to_result = self.upsert_resource(to_table, to_data, now_ms)
        
        # Step 2: Upsert relation
        relation_result = self.upsert_relation(
            relation_table, from_table, from_data, to_table, to_data, now_ms
        )
        
        return (from_result, to_result, relation_result)

    def batch_upsert_resources(
        self,
        table: str,
        resources: List[Dict[str, Any]],
        now_ms: int = None
    ) -> int:
        """
        Batch upsert resources.
        
        Returns:
            Number of successfully upserted records
        """
        if now_ms is None:
            now_ms = self.datetime_to_ms()
        
        success_count = 0
        
        # Build batch SQL
        sql_parts = []
        for dimensions in resources:
            record_id = self._build_record_id(table, dimensions)
            set_parts = [f"{k}: {self._escape_value(v)}" for k, v in dimensions.items()]
            set_parts.append(f"updated_at: {now_ms}")
            set_clause = ", ".join(set_parts)
            sql_parts.append(f"UPSERT {record_id} MERGE {{ {set_clause} }};")
        
        # Execute in batches
        for i in range(0, len(sql_parts), BATCH_SIZE):
            batch_sql = "\n".join(sql_parts[i:i+BATCH_SIZE])
            try:
                self.execute_sql(batch_sql)
                success_count += min(BATCH_SIZE, len(sql_parts) - i)
            except Exception as e:
                print(f"  batch upsert {table} 失败 (batch {i//BATCH_SIZE}): {e}")
        
        return success_count


# ============================================================================
# Schema 初始化
# ============================================================================

def init_schema_from_file(client: SurrealDBClient):
    """从 schema.sql 文件初始化 SurrealDB schema"""
    print("从 schema.sql 初始化 SurrealDB schema...")
    
    if not os.path.exists(_schema_file):
        print(f"错误: schema.sql 文件不存在: {_schema_file}")
        return False
    
    with open(_schema_file, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    # 替换 tolerance_time_ms 占位符
    schema_sql = schema_sql.replace('{tolerance_time_ms}', str(TOLERANCE_TIME_MS))
    
    try:
        client.execute_sql(schema_sql)
        print("Schema 初始化完成")
        return True
    except Exception as e:
        print(f"Schema 初始化失败: {e}")
        return False


# ============================================================================
# 数据生成
# ============================================================================

def generate_mock_pods(num_pods: int) -> List[Dict[str, Any]]:
    """生成模拟 Pod 数据"""
    pods = []
    for i in range(num_pods):
        pods.append({
            'bcs_cluster_id': MockConfig.CLUSTER_ID,
            'namespace': MockConfig.NAMESPACE,
            'pod': f"{MockConfig.BIZ_NAME}-pod-{i:05d}"
        })
    return pods


def generate_mock_services() -> List[Dict[str, Any]]:
    """生成模拟 Service 数据"""
    services = []
    for svc_name in MockConfig.SERVICE_LIST:
        services.append({
            'bcs_cluster_id': MockConfig.CLUSTER_ID,
            'namespace': MockConfig.NAMESPACE,
            'service': f"{MockConfig.BIZ_NAME}-{svc_name}"
        })
    return services


def generate_mock_nodes() -> List[Dict[str, Any]]:
    """生成模拟 Node 数据"""
    nodes = []
    for i in range(MockConfig.NUM_NODES):
        nodes.append({
            'bcs_cluster_id': MockConfig.CLUSTER_ID,
            'node': f"{MockConfig.BIZ_NAME}-node-{i:03d}"
        })
    return nodes


def generate_mock_containers(pods: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """生成模拟 Container 数据"""
    containers = []
    for pod in pods:
        for j in range(MockConfig.NUM_CONTAINERS_PER_POD):
            containers.append({
                'bcs_cluster_id': pod['bcs_cluster_id'],
                'namespace': pod['namespace'],
                'pod': pod['pod'],
                'container': f"container-{j}"
            })
    return containers


def generate_mock_systems() -> List[Dict[str, Any]]:
    """生成模拟 System 数据"""
    systems = []
    for i in range(MockConfig.NUM_SYSTEMS):
        systems.append({
            'bk_cloud_id': MockConfig.CLOUD_ID,
            'bk_target_ip': f"10.0.{i//256}.{i%256}"
        })
    return systems


def generate_mock_deployments() -> List[Dict[str, Any]]:
    """生成模拟 Deployment 数据"""
    deployments = []
    for svc_name in MockConfig.SERVICE_LIST:
        deployments.append({
            'bcs_cluster_id': MockConfig.CLUSTER_ID,
            'namespace': MockConfig.NAMESPACE,
            'deployment': f"{MockConfig.BIZ_NAME}-{svc_name}-deploy"
        })
    return deployments


def generate_mock_replicasets(deployments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """生成模拟 ReplicaSet 数据"""
    replicasets = []
    for deploy in deployments:
        replicasets.append({
            'bcs_cluster_id': deploy['bcs_cluster_id'],
            'namespace': deploy['namespace'],
            'replicaset': f"{deploy['deployment']}-rs-001"
        })
    return replicasets


# ============================================================================
# 数据获取 - 从 unify-query API
# ============================================================================

def query_ts_data(limit: int = 10000) -> List[Dict]:
    """从 unify-query API 获取真实数据"""
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
    
    print(f"查询 unify-query API: limit={limit}")
    
    try:
        resp = requests.post(url, json=data, headers=UNIFY_QUERY_HEADERS, timeout=120)
        if resp.status_code == 200:
            result = resp.json()
            series_list = result.get('series', [])
            
            relations = []
            seen = set()
            
            for series in series_list:
                group_keys = series.get('group_keys', [])
                group_values = series.get('group_values', [])
                
                if len(group_keys) != len(group_values):
                    continue
                
                record = dict(zip(group_keys, group_values))
                key = f"{record.get('bcs_cluster_id', '')}|{record.get('namespace', '')}|{record.get('pod', '')}|{record.get('service', '')}"
                
                if key in seen:
                    continue
                seen.add(key)
                
                if all(k in record for k in ['bcs_cluster_id', 'namespace', 'pod', 'service']):
                    relations.append(record)
            
            print(f"获取到 {len(relations)} 条记录")
            return relations
        else:
            print(f"查询失败: {resp.status_code} - {resp.text[:500]}")
    except Exception as e:
        print(f"查询异常: {e}")
    
    return []


# ============================================================================
# 数据导入
# ============================================================================

def import_mock_data(client: SurrealDBClient, num_pods: int = 10000):
    """导入模拟数据"""
    print(f"\n开始导入模拟数据 (Pod 数量: {num_pods})...")
    
    now_ms = client.datetime_to_ms()
    
    # 1. 生成数据
    print("\n生成模拟数据...")
    pods = generate_mock_pods(num_pods)
    services = generate_mock_services()
    nodes = generate_mock_nodes()
    deployments = generate_mock_deployments()
    replicasets = generate_mock_replicasets(deployments)
    containers = generate_mock_containers(pods[:100])  # 只为前100个Pod生成容器
    systems = generate_mock_systems()
    
    print(f"  Pods: {len(pods)}")
    print(f"  Services: {len(services)}")
    print(f"  Nodes: {len(nodes)}")
    print(f"  Deployments: {len(deployments)}")
    print(f"  ReplicaSets: {len(replicasets)}")
    print(f"  Containers: {len(containers)}")
    print(f"  Systems: {len(systems)}")
    
    # 2. 导入节点资源
    print("\n导入节点资源...")
    
    # Pods (批量)
    print(f"  导入 {len(pods)} 个 Pod...")
    start_time = time.time()
    count = client.batch_upsert_resources('pod', pods, now_ms)
    elapsed = time.time() - start_time
    print(f"    完成: {count} 条, 耗时: {elapsed:.2f}s, QPS: {count/elapsed:.1f}")
    
    # Services
    print(f"  导入 {len(services)} 个 Service...")
    count = client.batch_upsert_resources('service', services, now_ms)
    print(f"    完成: {count} 条")
    
    # Nodes
    print(f"  导入 {len(nodes)} 个 Node...")
    count = client.batch_upsert_resources('node', nodes, now_ms)
    print(f"    完成: {count} 条")
    
    # Deployments
    print(f"  导入 {len(deployments)} 个 Deployment...")
    count = client.batch_upsert_resources('deployment', deployments, now_ms)
    print(f"    完成: {count} 条")
    
    # ReplicaSets
    print(f"  导入 {len(replicasets)} 个 ReplicaSet...")
    count = client.batch_upsert_resources('replicaset', replicasets, now_ms)
    print(f"    完成: {count} 条")
    
    # Containers
    print(f"  导入 {len(containers)} 个 Container...")
    count = client.batch_upsert_resources('container', containers, now_ms)
    print(f"    完成: {count} 条")
    
    # Systems
    print(f"  导入 {len(systems)} 个 System...")
    count = client.batch_upsert_resources('system', systems, now_ms)
    print(f"    完成: {count} 条")
    
    # 3. 导入关系
    print("\n导入关系...")
    
    # pod_with_service: 每个 Pod 关联一个 Service
    print(f"  导入 pod_with_service 关系...")
    start_time = time.time()
    relation_count = 0
    for i, pod in enumerate(pods):
        service = services[i % len(services)]
        client.upsert_static_relation(
            'pod_with_service',
            'pod', pod,
            'service', service,
            now_ms
        )
        relation_count += 1
        if (i + 1) % 1000 == 0:
            print(f"    进度: {i+1}/{len(pods)}")
    elapsed = time.time() - start_time
    print(f"    完成: {relation_count} 条, 耗时: {elapsed:.2f}s")
    
    # node_with_pod: 每个 Pod 分配到一个 Node
    print(f"  导入 node_with_pod 关系...")
    start_time = time.time()
    relation_count = 0
    for i, pod in enumerate(pods):
        node = nodes[i % len(nodes)]
        client.upsert_relation(
            'node_with_pod',
            'node', node,
            'pod', pod,
            now_ms
        )
        relation_count += 1
        if (i + 1) % 1000 == 0:
            print(f"    进度: {i+1}/{len(pods)}")
    elapsed = time.time() - start_time
    print(f"    完成: {relation_count} 条, 耗时: {elapsed:.2f}s")
    
    # deployment_with_replicaset
    print(f"  导入 deployment_with_replicaset 关系...")
    for deploy, rs in zip(deployments, replicasets):
        client.upsert_relation(
            'deployment_with_replicaset',
            'deployment', deploy,
            'replicaset', rs,
            now_ms
        )
    print(f"    完成: {len(deployments)} 条")
    
    # pod_with_replicaset: 每个 Pod 关联到对应的 ReplicaSet
    print(f"  导入 pod_with_replicaset 关系...")
    for i, pod in enumerate(pods):
        rs = replicasets[i % len(replicasets)]
        client.upsert_relation(
            'pod_with_replicaset',
            'pod', pod,
            'replicaset', rs,
            now_ms
        )
    print(f"    完成: {len(pods)} 条")
    
    # container_with_pod
    print(f"  导入 container_with_pod 关系...")
    for container in containers:
        pod_data = {
            'bcs_cluster_id': container['bcs_cluster_id'],
            'namespace': container['namespace'],
            'pod': container['pod']
        }
        client.upsert_relation(
            'container_with_pod',
            'container', container,
            'pod', pod_data,
            now_ms
        )
    print(f"    完成: {len(containers)} 条")
    
    print("\n数据导入完成!")


def import_real_data(client: SurrealDBClient, limit: int = 50000):
    """从 unify-query 导入真实数据"""
    print(f"\n从 unify-query 获取真实数据 (limit={limit})...")
    
    relations = query_ts_data(limit)
    if not relations:
        print("未获取到数据")
        return
    
    print(f"\n开始导入 {len(relations)} 条关系数据...")
    
    now_ms = client.datetime_to_ms()
    success_count = 0
    error_count = 0
    
    for i, r in enumerate(relations):
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
        
        result = client.upsert_static_relation(
            'pod_with_service',
            'pod', pod_data,
            'service', service_data,
            now_ms
        )
        
        if result[2]:  # relation_result
            success_count += 1
        else:
            error_count += 1
        
        if (i + 1) % 500 == 0 or (i + 1) == len(relations):
            print(f"  进度: {i+1}/{len(relations)} (成功: {success_count}, 失败: {error_count})")
    
    print(f"\n导入完成! 成功: {success_count}, 失败: {error_count}")


# ============================================================================
# 验证
# ============================================================================

def verify_import(client: SurrealDBClient):
    """验证导入结果"""
    print("\n验证导入结果...")
    
    tables = ['pod', 'service', 'node', 'deployment', 'replicaset', 'container', 'system',
              'pod_with_service', 'node_with_pod', 'deployment_with_replicaset', 
              'pod_with_replicaset', 'container_with_pod']
    
    for table in tables:
        try:
            sql = f"SELECT count() FROM {table} GROUP ALL;"
            result = client.execute_sql(sql)
            count = 0
            if result and result[0].get('result'):
                res = result[0]['result']
                if isinstance(res, list) and res:
                    count = res[0].get('count', 0)
            print(f"  {table:30s}: {count:>8d}")
        except Exception as e:
            print(f"  {table:30s}: 查询失败 - {e}")
    
    # 示例数据
    print("\n示例 Pod 数据:")
    try:
        sql = "SELECT * FROM pod LIMIT 3;"
        result = client.execute_sql(sql)
        if result and result[0].get('result'):
            for item in result[0]['result'][:3]:
                print(f"  {item}")
    except Exception as e:
        print(f"  查询失败: {e}")
    
    # 示例关系数据
    print("\n示例 pod_with_service 关系:")
    try:
        sql = "SELECT * FROM pod_with_service LIMIT 3;"
        result = client.execute_sql(sql)
        if result and result[0].get('result'):
            for item in result[0]['result'][:3]:
                print(f"  {item}")
    except Exception as e:
        print(f"  查询失败: {e}")


# ============================================================================
# Main
# ============================================================================

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Import benchmark data for Plan 01: Active Windows')
    parser.add_argument('--init', action='store_true', help='仅初始化 schema')
    parser.add_argument('--verify', action='store_true', help='仅验证导入结果')
    parser.add_argument('--real', action='store_true', help='从 unify-query 获取真实数据')
    parser.add_argument('--count', type=int, default=10000, help='Pod 数量 (默认 10000)')
    
    args = parser.parse_args()
    
    client = SurrealDBClient()
    
    if args.init:
        init_schema_from_file(client)
    elif args.verify:
        verify_import(client)
    elif args.real:
        print("=" * 70)
        print("从 unify-query 获取真实数据并导入 SurrealDB (Plan 01: Active Windows)")
        print("=" * 70)
        
        init_schema_from_file(client)
        import_real_data(client)
        verify_import(client)
    else:
        print("=" * 70)
        print(f"导入模拟数据到 SurrealDB (Plan 01: Active Windows)")
        print(f"Pod 数量: {args.count}")
        print("=" * 70)
        
        # 1. 初始化 schema
        init_schema_from_file(client)
        
        # 2. 导入模拟数据
        import_mock_data(client, args.count)
        
        # 3. 验证
        verify_import(client)
