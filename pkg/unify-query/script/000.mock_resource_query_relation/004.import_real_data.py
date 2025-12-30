#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import Real Data from Unify-Query

从 unify-query 接口批量获取真实的 pod_with_service_relation 数据，
导入到 SurrealDB 进行基准测试。

使用与 002.mock_full_resource_graph.py 一致的 upsert 逻辑，
通过 SurrealDB 的 fn::upsert_* 函数自动处理 ID 生成和更新逻辑。

支持两种数据获取方式：
1. /query/ts/info/series - 直接获取所有维度组合（推荐）
2. /query/ts - 通过时序查询获取数据

Usage:
    python 004.import_real_data.py              # 完整导入流程
    python 004.import_real_data.py --init       # 仅初始化 schema (使用 schema.sql)
    python 004.import_real_data.py --verify     # 仅验证导入结果
"""

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List

import requests

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
UNIFY_QUERY_BASE_URL = _unify_cfg.get('url', '').rsplit('/query/ts', 1)[0]  # 获取基础 URL
UNIFY_QUERY_HEADERS = {
    'Content-Type': 'application/json',
    'X-Bkapi-Authorization': json.dumps({
        'bk_app_code': _unify_cfg.get('app_code', ''),
        'bk_app_secret': _unify_cfg.get('app_secret', ''),
        'bk_username': _unify_cfg.get('username', 'admin')
    }),
    'X-Bk-Scope-Space-Uid': _unify_cfg.get('space_uid', '')
}

# 导入配置 (脚本内部常量)
TIME_RANGE = 86400          # 查询时间范围（秒）
TOLERANCE_TIME_MS = 600000  # 生命周期容忍时间（毫秒）


# ============================================================================
# SurrealDB Client (与 002.mock_full_resource_graph.py 保持一致)
# ============================================================================

class SurrealDBClient:
    """SurrealDB HTTP REST API client"""

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
        for i, result in enumerate(results):
            if result.get('status') == 'ERR':
                error_detail = result.get('detail') or result.get('result', 'Unknown error')
                raise Exception(f"SQL error in statement {i}: {error_detail}")

        return results[1:] if len(results) > 1 else results

    def datetime_to_ms(self, dt: datetime) -> int:
        """Convert datetime to milliseconds timestamp"""
        return int(dt.timestamp() * 1000)

    def call_upsert_resource(
        self,
        resource_type: str,
        dimensions: Dict[str, Any],
        now_ms: int,
        tolerance_ms: int
    ) -> Dict[str, Any]:
        """
        调用 fn::upsert_{resource_type} 函数
        """
        # 构建 dimensions 对象字符串
        dim_parts = []
        for k, v in dimensions.items():
            if isinstance(v, (int, float)):
                dim_parts.append(f"{k}: {v}")
            else:
                # 转义单引号
                v_escaped = str(v).replace("'", "\\'")
                dim_parts.append(f"{k}: '{v_escaped}'")
        dim_str = ", ".join(dim_parts)
        
        # 调用资源特定的 upsert 函数
        func_name = f"fn::upsert_{resource_type}"
        sql = f"{func_name}({{ {dim_str} }}, {now_ms}, {tolerance_ms});"
        
        try:
            result = self.execute_sql(sql)
            if result and result[0].get('result'):
                records = result[0]['result']
                if isinstance(records, list) and records:
                    return records[0]
                elif isinstance(records, dict):
                    return records
        except Exception as e:
            print(f"  upsert {resource_type} 失败: {e}")
        return {}

    def call_upsert_relation(
        self,
        relation_table: str,
        from_id: str,
        to_id: str,
        now_ms: int
    ) -> Dict[str, Any]:
        """
        调用 fn::upsert_relation 函数
        """
        sql = f"fn::upsert_relation('{relation_table}', {from_id}, {to_id}, {now_ms});"
        
        try:
            result = self.execute_sql(sql)
            if result and result[0].get('result'):
                records = result[0]['result']
                if isinstance(records, list) and records:
                    return records[0]
                elif isinstance(records, dict):
                    return records
        except Exception as e:
            print(f"  upsert relation {relation_table} 失败: {e}")
        return {}

    def upsert_static_relation(
        self,
        relation_type: str,
        from_type: str,
        from_data: Dict[str, Any],
        to_type: str,
        to_data: Dict[str, Any],
        now_ms: int
    ) -> Dict[str, Any]:
        """
        Upsert 静态关系（与 002.mock_full_resource_graph.py 一致）
        """
        # Step 1: Upsert 两端资源
        from_result = self.call_upsert_resource(from_type, from_data, now_ms, TOLERANCE_TIME_MS)
        to_result = self.call_upsert_resource(to_type, to_data, now_ms, TOLERANCE_TIME_MS)
        
        # Step 2: 获取记录 ID
        from_id = from_result.get('id')
        to_id = to_result.get('id')
        
        if not from_id or not to_id:
            return {}
        
        # Step 3: Upsert 关系
        return self.call_upsert_relation(relation_type, from_id, to_id, now_ms)


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
# 数据获取 - 使用 /query/ts/info/series 接口
# ============================================================================

def query_series(keys: List[str], limit: int = 10000) -> List[Dict]:
    """
    使用 /query/ts/info/series 接口直接获取所有维度组合
    
    这是推荐的方式，可以直接获取所有 pod 和 service 的组合，
    不需要通过过滤器猜测。
    
    Args:
        keys: 要查询的维度列表，如 ['bcs_cluster_id', 'namespace', 'pod', 'service']
        limit: 返回结果数量限制
    
    Returns:
        维度组合列表
    """
    url = f"{UNIFY_QUERY_BASE_URL}/query/ts/info/series"
    
    now = int(time.time())
    data = {
        'data_source': 'bkmonitor',
        'table_id': '',
        'field_name': 'pod_with_service_relation',
        'metric': 'pod_with_service_relation',
        'keys': keys,
        'start': str(now - TIME_RANGE),
        'end': str(now),
        'limit': limit
    }
    
    print(f"查询 series: keys={keys}, limit={limit}")
    print(f"URL: {url}")
    
    try:
        resp = requests.post(url, json=data, headers=UNIFY_QUERY_HEADERS, timeout=120)
        print(f"响应状态: {resp.status_code}")
        
        if resp.status_code == 200:
            result = resp.json()
            # series 接口返回格式: {"measurement": "...", "keys": [...], "series": [[...], [...]]}
            keys_list = result.get('keys', [])
            series_list = result.get('series', [])
            
            # 转换为字典列表
            relations = []
            for values in series_list:
                if len(values) == len(keys_list):
                    record = dict(zip(keys_list, values))
                    relations.append(record)
            
            print(f"获取到 {len(relations)} 条记录")
            return relations
        else:
            print(f"查询失败: {resp.text[:500]}")
    except Exception as e:
        print(f"查询异常: {e}")
    
    return []


def query_ts_data(limit: int = 10000) -> List[Dict]:
    """
    使用 /query/ts 接口获取数据（备用方式）
    
    Args:
        limit: 返回结果数量限制
    
    Returns:
        关系记录列表
    """
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
    
    print(f"查询 ts: limit={limit}")
    
    try:
        resp = requests.post(url, json=data, headers=UNIFY_QUERY_HEADERS, timeout=120)
        if resp.status_code == 200:
            result = resp.json()
            series_list = result.get('series', [])
            
            # 解析 series 数据
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
    except Exception as e:
        print(f"查询异常: {e}")
    
    return []


def fetch_all_data() -> List[Dict]:
    """
    获取所有 pod_with_service_relation 数据
    
    使用 /query/ts 接口获取数据（series 接口需要 table_id，不适用于此场景）
    """
    return query_ts_data(limit=50000)


# ============================================================================
# 数据导入 (使用 upsert 函数，与 002 保持一致)
# ============================================================================

def import_relations(client: SurrealDBClient, relations: List[Dict]):
    """
    使用 upsert 函数导入关系数据到 SurrealDB
    
    与 002.mock_full_resource_graph.py 的 upsert_static_relation 逻辑一致：
    1. 调用 fn::upsert_pod 和 fn::upsert_service 创建/更新资源
    2. 调用 fn::upsert_relation 创建/更新关系
    """
    print(f"开始导入 {len(relations)} 条关系数据...")
    
    now_ms = client.datetime_to_ms(datetime.utcnow())
    
    success_count = 0
    error_count = 0
    
    for i, r in enumerate(relations):
        # Pod 维度 (按 RESOURCE_INDEX_FIELDS 定义)
        pod_data = {
            'bcs_cluster_id': r['bcs_cluster_id'],
            'namespace': r['namespace'],
            'pod': r['pod']
        }
        
        # Service 维度
        service_data = {
            'bcs_cluster_id': r['bcs_cluster_id'],
            'namespace': r['namespace'],
            'service': r['service']
        }
        
        # 使用 upsert_static_relation（与 002 一致）
        result = client.upsert_static_relation(
            relation_type='pod_with_service',
            from_type='pod',
            from_data=pod_data,
            to_type='service',
            to_data=service_data,
            now_ms=now_ms
        )
        
        if result:
            success_count += 1
        else:
            error_count += 1
        
        # 进度输出
        if (i + 1) % 500 == 0 or (i + 1) == len(relations):
            print(f"  进度: {i+1}/{len(relations)} (成功: {success_count}, 失败: {error_count})")
    
    print(f"导入完成! 成功: {success_count}, 失败: {error_count}")


# ============================================================================
# 验证
# ============================================================================

def verify_import(client: SurrealDBClient):
    """验证导入结果"""
    print("\n验证导入结果...")
    
    sql = """
    SELECT count() FROM pod GROUP ALL;
    SELECT count() FROM service GROUP ALL;
    SELECT count() FROM pod_with_service GROUP ALL;
    """
    result = client.execute_sql(sql)
    
    if result and len(result) >= 3:
        pod_count = result[0].get('result', [{}])[0].get('count', 0) if result[0].get('result') else 0
        svc_count = result[1].get('result', [{}])[0].get('count', 0) if result[1].get('result') else 0
        rel_count = result[2].get('result', [{}])[0].get('count', 0) if result[2].get('result') else 0
        print(f"  Pod: {pod_count}")
        print(f"  Service: {svc_count}")
        print(f"  关系: {rel_count}")
    
    # 示例查询
    sql2 = """
    SELECT in.bcs_cluster_id, in.namespace, in.pod, out.service
    FROM pod_with_service LIMIT 5;
    """
    result2 = client.execute_sql(sql2)
    if result2 and result2[0].get('result'):
        print(f"\n示例数据:")
        for item in result2[0]['result'][:5]:
            print(f"  {item}")


# ============================================================================
# Main
# ============================================================================

if __name__ == '__main__':
    import sys
    
    client = SurrealDBClient()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--init':
        init_schema_from_file(client)
    elif len(sys.argv) > 1 and sys.argv[1] == '--verify':
        verify_import(client)
    else:
        print("=" * 70)
        print("从 unify-query 获取真实数据并导入 SurrealDB")
        print("=" * 70)
        
        # 1. 初始化 schema
        init_schema_from_file(client)
        
        # 2. 获取数据
        print("\n" + "=" * 70)
        print("获取数据...")
        print("=" * 70)
        relations = fetch_all_data()
        print(f"共获取 {len(relations)} 条唯一关系")
        
        if not relations:
            print("未获取到数据，退出")
            sys.exit(1)
        
        # 3. 导入数据
        print("\n" + "=" * 70)
        print("导入数据...")
        print("=" * 70)
        import_relations(client, relations)
        
        # 4. 验证
        verify_import(client)
