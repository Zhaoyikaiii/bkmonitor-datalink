#!/usr/bin/env python3
"""
test_multi_hop.py - Test multi-hop queries with the new schema

测试场景：
1. 单层 hop（hop1）- 多个并行关系
2. 两层 hop（hop1 + hop2 嵌套在 target 内）
3. 动态关系双向查询（outbound + inbound）

数据模型：
  pod:nginx-1 
    ├── (node_with_pod) → node:node-1
    │                       └── (node_with_system) → system:10.0.0.1
    ├── (container_with_pod) → container:nginx
    ├── (pod_with_service) → service:nginx-svc
    └── (pod_to_pod outbound) → pod:nginx-2
        └── (pod_to_pod inbound) ← pod:nginx-3
"""

import time
import json
import requests
import urllib3
import yaml
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "config.yaml"

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

class SurrealDBClient:
    def __init__(self, config):
        self.url = config['url'].rstrip('/')
        self.username = config['username']
        self.password = config['password']
        self.namespace = config['namespace']
        self.database = config['database']
        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.verify = False
    
    def execute(self, query, use_db=True):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'text/plain',
            'surreal-ns': self.namespace,
        }
        if use_db:
            headers['surreal-db'] = self.database
        
        response = self.session.post(
            f"{self.url}/sql",
            headers=headers,
            data=query.encode('utf-8')
        )
        response.raise_for_status()
        return response.json()

def setup_test_data(client, now_ms):
    """创建测试数据"""
    print("\n1. 创建测试实体和关系...")
    
    setup_sql = f"""
    -- ============================================
    -- 实体数据
    -- ============================================
    
    -- Pods
    UPSERT pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001', namespace: 'default', pod: 'nginx-1', updated_at: {now_ms}
    }};
    UPSERT pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-2⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001', namespace: 'default', pod: 'nginx-2', updated_at: {now_ms}
    }};
    UPSERT pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-3⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001', namespace: 'default', pod: 'nginx-3', updated_at: {now_ms}
    }};
    
    -- Node
    UPSERT node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001', node: 'node-1', updated_at: {now_ms}
    }};
    
    -- Container
    UPSERT container:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1,container=nginx⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001', namespace: 'default', pod: 'nginx-1', container: 'nginx', updated_at: {now_ms}
    }};
    
    -- Service
    UPSERT service:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,service=nginx-svc⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001', namespace: 'default', service: 'nginx-svc', updated_at: {now_ms}
    }};
    
    -- System
    UPSERT system:⟨bk_cloud_id=0,bk_target_ip=10.0.0.1⟩ MERGE {{
        bk_cloud_id: '0', bk_target_ip: '10.0.0.1', updated_at: {now_ms}
    }};
    
    -- ============================================
    -- 关系数据（静态关系）
    -- ============================================
    
    -- node_with_pod: node-1 -> nginx-1
    UPSERT node_with_pod:⟨node-1|nginx-1⟩ MERGE {{
        source_id: node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩,
        target_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩,
        updated_at: {now_ms}
    }};
    
    -- container_with_pod: container -> nginx-1
    UPSERT container_with_pod:⟨nginx-container|nginx-1⟩ MERGE {{
        source_id: container:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1,container=nginx⟩,
        target_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩,
        updated_at: {now_ms}
    }};
    
    -- pod_with_service: nginx-1 -> nginx-svc
    UPSERT pod_with_service:⟨nginx-1|nginx-svc⟩ MERGE {{
        source_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩,
        target_id: service:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,service=nginx-svc⟩,
        updated_at: {now_ms}
    }};
    
    -- node_with_system: node-1 -> system (用于 hop2 测试)
    UPSERT node_with_system:⟨node-1|system-1⟩ MERGE {{
        source_id: node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩,
        target_id: system:⟨bk_cloud_id=0,bk_target_ip=10.0.0.1⟩,
        updated_at: {now_ms}
    }};
    
    -- ============================================
    -- 关系数据（动态关系 - 用于双向查询测试）
    -- ============================================
    
    -- pod_to_pod: nginx-1 -> nginx-2 (outbound from nginx-1's perspective)
    UPSERT pod_to_pod:⟨nginx-1|nginx-2⟩ MERGE {{
        source_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩,
        target_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-2⟩,
        updated_at: {now_ms}
    }};
    
    -- pod_to_pod: nginx-3 -> nginx-1 (inbound to nginx-1's perspective)
    UPSERT pod_to_pod:⟨nginx-3|nginx-1⟩ MERGE {{
        source_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-3⟩,
        target_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩,
        updated_at: {now_ms}
    }};
    """
    
    result = client.execute(setup_sql)
    errors = [r for r in result if r.get('status') == 'ERR']
    if errors:
        print(f"Errors: {errors}")
        return False
    print("✓ 测试数据创建成功")
    return True


def test_hop1_parallel_relations(client, start, end):
    """测试单层 hop 多个并行关系"""
    print("\n" + "=" * 60)
    print("测试 1: 单层 hop（hop1）多个并行关系")
    print("=" * 60)
    print("查询 pod:nginx-1 的所有静态关系：node_with_pod, container_with_pod, pod_with_service")
    
    query = f"""
    LET $start = {start};
    LET $end = {end};
    
    SELECT 
        'pod' AS entity_type,
        <string>id AS entity_id,
        {{ bcs_cluster_id: bcs_cluster_id, namespace: namespace, pod: pod }} AS entity_data,
        (SELECT * FROM pod_liveness_record 
         WHERE pod_id = $parent.id AND $end >= period_start AND $start <= period_end) AS liveness,
        {{
            -- 并行关系 1: node_with_pod (反向：pod 是 target)
            node_with_pod: (SELECT 
                1 AS hop,
                'node_with_pod' AS relation_type,
                'static' AS relation_category,
                <string>id AS relation_id,
                (SELECT * FROM node_with_pod_liveness_record 
                 WHERE relation_id = $parent.id AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                {{
                    entity_type: 'node',
                    entity_id: <string>source_id,
                    entity_data: {{ bcs_cluster_id: source_id.bcs_cluster_id, node: source_id.node }},
                    liveness: (SELECT * FROM node_liveness_record 
                               WHERE node_id = $parent.source_id AND $end >= period_start AND $start <= period_end)
                }} AS target
            FROM node_with_pod WHERE target_id = $parent.id),
            
            -- 并行关系 2: container_with_pod (反向：pod 是 target)
            container_with_pod: (SELECT 
                1 AS hop,
                'container_with_pod' AS relation_type,
                'static' AS relation_category,
                <string>id AS relation_id,
                (SELECT * FROM container_with_pod_liveness_record 
                 WHERE relation_id = $parent.id AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                {{
                    entity_type: 'container',
                    entity_id: <string>source_id,
                    entity_data: {{ bcs_cluster_id: source_id.bcs_cluster_id, namespace: source_id.namespace, pod: source_id.pod, container: source_id.container }},
                    liveness: (SELECT * FROM container_liveness_record 
                               WHERE container_id = $parent.source_id AND $end >= period_start AND $start <= period_end)
                }} AS target
            FROM container_with_pod WHERE target_id = $parent.id),
            
            -- 并行关系 3: pod_with_service (正向：pod 是 source)
            pod_with_service: (SELECT 
                1 AS hop,
                'pod_with_service' AS relation_type,
                'static' AS relation_category,
                <string>id AS relation_id,
                (SELECT * FROM pod_with_service_liveness_record 
                 WHERE relation_id = $parent.id AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                {{
                    entity_type: 'service',
                    entity_id: <string>target_id,
                    entity_data: {{ bcs_cluster_id: target_id.bcs_cluster_id, namespace: target_id.namespace, service: target_id.service }},
                    liveness: (SELECT * FROM service_liveness_record 
                               WHERE service_id = $parent.target_id AND $end >= period_start AND $start <= period_end)
                }} AS target
            FROM pod_with_service WHERE source_id = $parent.id)
        }} AS hop1
    FROM pod WHERE id = pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩;
    """
    
    result = client.execute(query)
    select_result = [r for r in result if r.get('result') is not None][-1]
    
    if select_result.get('status') == 'OK':
        print("✓ 查询成功!")
        data = select_result['result'][0]
        hop1 = data.get('hop1', {})
        print(f"\n  hop1 包含 {len(hop1)} 个并行关系:")
        for rel_type, relations in hop1.items():
            print(f"    - {rel_type}: {len(relations)} 条记录")
            for r in relations:
                target = r.get('target', {})
                print(f"      → {target.get('entity_type')}: {target.get('entity_id', '').split(':')[-1][:30]}...")
        return True
    else:
        print(f"✗ 查询失败: {select_result}")
        return False


def test_hop2_nested(client, start, end):
    """测试两层 hop（hop2 嵌套在 hop1 的 target 内）"""
    print("\n" + "=" * 60)
    print("测试 2: 两层 hop（hop2 嵌套在 target 内）")
    print("=" * 60)
    print("查询路径: pod:nginx-1 → (node_with_pod) → node:node-1 → (node_with_system) → system")
    
    query = f"""
    LET $start = {start};
    LET $end = {end};
    
    SELECT 
        'pod' AS entity_type,
        <string>id AS entity_id,
        {{ bcs_cluster_id: bcs_cluster_id, namespace: namespace, pod: pod }} AS entity_data,
        {{
            -- hop1: node_with_pod
            node_with_pod: (SELECT 
                1 AS hop,
                'node_with_pod' AS relation_type,
                <string>id AS relation_id,
                {{
                    entity_type: 'node',
                    entity_id: <string>source_id,
                    entity_data: {{ bcs_cluster_id: source_id.bcs_cluster_id, node: source_id.node }},
                    liveness: (SELECT * FROM node_liveness_record 
                               WHERE node_id = $parent.source_id AND $end >= period_start AND $start <= period_end),
                    
                    -- hop2 嵌套在 target 内部
                    hop2: {{
                        -- 从 node 继续遍历到 system
                        node_with_system: (SELECT 
                            2 AS hop,
                            'node_with_system' AS relation_type,
                            <string>id AS relation_id,
                            (SELECT * FROM node_with_system_liveness_record 
                             WHERE relation_id = $parent.id AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                            {{
                                entity_type: 'system',
                                entity_id: <string>target_id,
                                entity_data: {{ bk_cloud_id: target_id.bk_cloud_id, bk_target_ip: target_id.bk_target_ip }},
                                liveness: (SELECT * FROM system_liveness_record 
                                           WHERE system_id = $parent.target_id AND $end >= period_start AND $start <= period_end)
                            }} AS target
                        FROM node_with_system WHERE source_id = $parent.source_id)
                    }}
                }} AS target
            FROM node_with_pod WHERE target_id = $parent.id)
        }} AS hop1
    FROM pod WHERE id = pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩;
    """
    
    result = client.execute(query)
    select_result = [r for r in result if r.get('result') is not None][-1]
    
    if select_result.get('status') == 'OK':
        print("✓ 查询成功!")
        data = select_result['result'][0]
        
        # 遍历结果验证嵌套结构
        hop1 = data.get('hop1', {})
        node_with_pod = hop1.get('node_with_pod', [])
        
        if node_with_pod:
            print(f"\n  hop1.node_with_pod: {len(node_with_pod)} 条记录")
            for rel in node_with_pod:
                target = rel.get('target', {})
                print(f"    → node: {target.get('entity_data', {}).get('node')}")
                
                # 检查 hop2
                hop2 = target.get('hop2', {})
                node_with_system = hop2.get('node_with_system', [])
                if node_with_system:
                    print(f"      hop2.node_with_system: {len(node_with_system)} 条记录")
                    for rel2 in node_with_system:
                        target2 = rel2.get('target', {})
                        print(f"        → system: {target2.get('entity_data', {}).get('bk_target_ip')}")
                else:
                    print("      hop2.node_with_system: 无记录")
        return True
    else:
        print(f"✗ 查询失败: {select_result}")
        return False


def test_dynamic_relation_bidirectional(client, start, end):
    """测试动态关系双向查询（outbound + inbound）"""
    print("\n" + "=" * 60)
    print("测试 3: 动态关系双向查询（direction=both）")
    print("=" * 60)
    print("查询 pod:nginx-1 的 pod_to_pod 关系（同时查询 outbound 和 inbound）")
    print("  - outbound: nginx-1 → nginx-2")
    print("  - inbound: nginx-3 → nginx-1")
    
    query = f"""
    LET $start = {start};
    LET $end = {end};
    
    SELECT 
        'pod' AS entity_type,
        <string>id AS entity_id,
        {{ bcs_cluster_id: bcs_cluster_id, namespace: namespace, pod: pod }} AS entity_data,
        {{
            -- 动态关系 outbound: 当前 pod 是 source
            pod_to_pod_outbound: (SELECT 
                1 AS hop,
                'pod_to_pod' AS relation_type,
                'dynamic' AS relation_category,
                'outbound' AS direction,
                <string>id AS relation_id,
                (SELECT * FROM pod_to_pod_liveness_record 
                 WHERE relation_id = $parent.id AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                {{
                    entity_type: 'pod',
                    entity_id: <string>target_id,
                    entity_data: {{ bcs_cluster_id: target_id.bcs_cluster_id, namespace: target_id.namespace, pod: target_id.pod }},
                    liveness: (SELECT * FROM pod_liveness_record 
                               WHERE pod_id = $parent.target_id AND $end >= period_start AND $start <= period_end)
                }} AS target
            FROM pod_to_pod WHERE source_id = $parent.id),
            
            -- 动态关系 inbound: 当前 pod 是 target
            pod_to_pod_inbound: (SELECT 
                1 AS hop,
                'pod_to_pod' AS relation_type,
                'dynamic' AS relation_category,
                'inbound' AS direction,
                <string>id AS relation_id,
                (SELECT * FROM pod_to_pod_liveness_record 
                 WHERE relation_id = $parent.id AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                {{
                    entity_type: 'pod',
                    entity_id: <string>source_id,
                    entity_data: {{ bcs_cluster_id: source_id.bcs_cluster_id, namespace: source_id.namespace, pod: source_id.pod }},
                    liveness: (SELECT * FROM pod_liveness_record 
                               WHERE pod_id = $parent.source_id AND $end >= period_start AND $start <= period_end)
                }} AS target
            FROM pod_to_pod WHERE target_id = $parent.id)
        }} AS hop1
    FROM pod WHERE id = pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩;
    """
    
    result = client.execute(query)
    select_result = [r for r in result if r.get('result') is not None][-1]
    
    if select_result.get('status') == 'OK':
        print("✓ 查询成功!")
        data = select_result['result'][0]
        hop1 = data.get('hop1', {})
        
        # 检查 outbound
        outbound = hop1.get('pod_to_pod_outbound', [])
        print(f"\n  pod_to_pod_outbound: {len(outbound)} 条记录")
        for rel in outbound:
            target = rel.get('target', {})
            print(f"    → (outbound) pod: {target.get('entity_data', {}).get('pod')}")
        
        # 检查 inbound
        inbound = hop1.get('pod_to_pod_inbound', [])
        print(f"\n  pod_to_pod_inbound: {len(inbound)} 条记录")
        for rel in inbound:
            target = rel.get('target', {})
            print(f"    ← (inbound) pod: {target.get('entity_data', {}).get('pod')}")
        
        return True
    else:
        print(f"✗ 查询失败: {select_result}")
        return False


def test_combined_multi_hop_parallel(client, start, end):
    """测试组合场景：多层 hop + 并行关系 + 动态关系"""
    print("\n" + "=" * 60)
    print("测试 4: 组合场景 - 多层 hop + 并行关系 + 动态关系")
    print("=" * 60)
    print("完整查询 pod:nginx-1 的所有关系（max_hops=2）")
    
    query = f"""
    LET $start = {start};
    LET $end = {end};
    
    SELECT 
        'pod' AS entity_type,
        <string>id AS entity_id,
        {{
            -- 静态关系：node_with_pod (带 hop2)
            node_with_pod: (SELECT 
                1 AS hop,
                'node_with_pod' AS relation_type,
                <string>id AS relation_id,
                {{
                    entity_type: 'node',
                    entity_id: <string>source_id,
                    entity_data: {{ bcs_cluster_id: source_id.bcs_cluster_id, node: source_id.node }},
                    hop2: {{
                        node_with_system: (SELECT 
                            2 AS hop,
                            'node_with_system' AS relation_type,
                            <string>id AS relation_id,
                            {{
                                entity_type: 'system',
                                entity_id: <string>target_id,
                                entity_data: {{ bk_cloud_id: target_id.bk_cloud_id, bk_target_ip: target_id.bk_target_ip }}
                            }} AS target
                        FROM node_with_system WHERE source_id = $parent.source_id)
                    }}
                }} AS target
            FROM node_with_pod WHERE target_id = $parent.id),
            
            -- 静态关系：container_with_pod
            container_with_pod: (SELECT 
                1 AS hop,
                'container_with_pod' AS relation_type,
                <string>id AS relation_id,
                {{
                    entity_type: 'container',
                    entity_id: <string>source_id,
                    entity_data: {{ bcs_cluster_id: source_id.bcs_cluster_id, namespace: source_id.namespace, pod: source_id.pod, container: source_id.container }}
                }} AS target
            FROM container_with_pod WHERE target_id = $parent.id),
            
            -- 静态关系：pod_with_service
            pod_with_service: (SELECT 
                1 AS hop,
                'pod_with_service' AS relation_type,
                <string>id AS relation_id,
                {{
                    entity_type: 'service',
                    entity_id: <string>target_id,
                    entity_data: {{ bcs_cluster_id: target_id.bcs_cluster_id, namespace: target_id.namespace, service: target_id.service }}
                }} AS target
            FROM pod_with_service WHERE source_id = $parent.id),
            
            -- 动态关系：pod_to_pod (双向)
            pod_to_pod_outbound: (SELECT 
                1 AS hop,
                'pod_to_pod' AS relation_type,
                'outbound' AS direction,
                <string>id AS relation_id,
                {{
                    entity_type: 'pod',
                    entity_id: <string>target_id,
                    entity_data: {{ bcs_cluster_id: target_id.bcs_cluster_id, namespace: target_id.namespace, pod: target_id.pod }}
                }} AS target
            FROM pod_to_pod WHERE source_id = $parent.id),
            
            pod_to_pod_inbound: (SELECT 
                1 AS hop,
                'pod_to_pod' AS relation_type,
                'inbound' AS direction,
                <string>id AS relation_id,
                {{
                    entity_type: 'pod',
                    entity_id: <string>source_id,
                    entity_data: {{ bcs_cluster_id: source_id.bcs_cluster_id, namespace: source_id.namespace, pod: source_id.pod }}
                }} AS target
            FROM pod_to_pod WHERE target_id = $parent.id)
        }} AS hop1
    FROM pod WHERE id = pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩;
    """
    
    result = client.execute(query)
    select_result = [r for r in result if r.get('result') is not None][-1]
    
    if select_result.get('status') == 'OK':
        print("✓ 查询成功!")
        data = select_result['result'][0]
        hop1 = data.get('hop1', {})
        
        print(f"\n  hop1 包含 {len(hop1)} 个关系类型:")
        
        total_hop1_relations = 0
        total_hop2_relations = 0
        
        for rel_type, relations in hop1.items():
            print(f"\n    {rel_type}: {len(relations)} 条记录")
            total_hop1_relations += len(relations)
            
            for rel in relations:
                target = rel.get('target', {})
                entity_data = target.get('entity_data', {})
                
                # 获取显示名称
                display_name = (entity_data.get('pod') or 
                               entity_data.get('node') or 
                               entity_data.get('container') or 
                               entity_data.get('service') or 
                               entity_data.get('bk_target_ip') or 
                               '?')
                direction = rel.get('direction', '')
                dir_str = f" ({direction})" if direction else ""
                print(f"      → {target.get('entity_type')}: {display_name}{dir_str}")
                
                # 检查 hop2
                hop2 = target.get('hop2', {})
                if hop2:
                    for rel2_type, relations2 in hop2.items():
                        print(f"        hop2.{rel2_type}: {len(relations2)} 条记录")
                        total_hop2_relations += len(relations2)
                        for rel2 in relations2:
                            target2 = rel2.get('target', {})
                            entity_data2 = target2.get('entity_data', {})
                            display_name2 = (entity_data2.get('bk_target_ip') or 
                                           entity_data2.get('pod') or 
                                           '?')
                            print(f"          → {target2.get('entity_type')}: {display_name2}")
        
        print(f"\n  总计: hop1={total_hop1_relations} 条, hop2={total_hop2_relations} 条")
        return True
    else:
        print(f"✗ 查询失败: {select_result}")
        return False


def main():
    config = load_config()
    client = SurrealDBClient(config['surreal_db'])
    
    now_ms = int(time.time() * 1000)
    start = now_ms - 3600000  # 1 hour ago
    end = now_ms + 3600000    # 1 hour later
    
    print("=" * 60)
    print("多层 Hop 和并行关系测试")
    print("=" * 60)
    
    # 创建测试数据
    if not setup_test_data(client, now_ms):
        return 1
    
    # 等待 EVENT 触发器完成
    import time as t
    t.sleep(0.5)
    
    results = []
    
    # 测试 1: 单层 hop 多个并行关系
    results.append(("hop1 并行关系", test_hop1_parallel_relations(client, start, end)))
    
    # 测试 2: 两层 hop（嵌套）
    results.append(("hop2 嵌套", test_hop2_nested(client, start, end)))
    
    # 测试 3: 动态关系双向查询
    results.append(("动态关系双向", test_dynamic_relation_bidirectional(client, start, end)))
    
    # 测试 4: 组合场景
    results.append(("组合场景", test_combined_multi_hop_parallel(client, start, end)))
    
    # 输出总结
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✓ 通过" if passed else "✗ 失败"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False
    
    print("\n" + ("所有测试通过！" if all_passed else "部分测试失败"))
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
