#!/usr/bin/env python3
"""
test_new_schema.py - Test the new schema with source_id/target_id fields

This script:
1. Initializes the database with new schema
2. Creates test entities and relations
3. Runs a sample query to verify the schema works
"""

import time
import requests
import urllib3
import yaml
from pathlib import Path

# Disable SSL warnings
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

def main():
    config = load_config()
    client = SurrealDBClient(config['surreal_db'])
    
    now_ms = int(time.time() * 1000)
    
    print("=" * 60)
    print("Testing new schema with source_id/target_id")
    print("=" * 60)
    
    # 1. Create test entities
    print("\n1. Creating test entities...")
    
    entities_sql = f"""
    -- Create pods
    UPSERT pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001',
        namespace: 'default',
        pod: 'nginx-1',
        updated_at: {now_ms}
    }};
    
    UPSERT pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-2⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001',
        namespace: 'default',
        pod: 'nginx-2',
        updated_at: {now_ms}
    }};
    
    -- Create node
    UPSERT node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001',
        node: 'node-1',
        updated_at: {now_ms}
    }};
    
    -- Create container
    UPSERT container:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1,container=nginx⟩ MERGE {{
        bcs_cluster_id: 'BCS-K8S-001',
        namespace: 'default',
        pod: 'nginx-1',
        container: 'nginx',
        updated_at: {now_ms}
    }};
    
    -- Create system
    UPSERT system:⟨bk_cloud_id=0,bk_target_ip=10.0.0.1⟩ MERGE {{
        bk_cloud_id: '0',
        bk_target_ip: '10.0.0.1',
        updated_at: {now_ms}
    }};
    """
    
    result = client.execute(entities_sql)
    errors = [r for r in result if r.get('status') == 'ERR']
    if errors:
        print(f"Errors creating entities: {errors}")
        return 1
    print("✓ Entities created successfully")
    
    # 2. Create relations with source_id/target_id
    print("\n2. Creating relations with source_id/target_id...")
    
    relations_sql = f"""
    -- node_with_pod: node-1 -> nginx-1
    UPSERT node_with_pod:⟨bcs_cluster_id=BCS-K8S-001,node=node-1|bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩ MERGE {{
        source_id: node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩,
        target_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩,
        updated_at: {now_ms}
    }};
    
    -- node_with_pod: node-1 -> nginx-2
    UPSERT node_with_pod:⟨bcs_cluster_id=BCS-K8S-001,node=node-1|bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-2⟩ MERGE {{
        source_id: node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩,
        target_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-2⟩,
        updated_at: {now_ms}
    }};
    
    -- container_with_pod: container -> nginx-1
    UPSERT container_with_pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1,container=nginx|bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩ MERGE {{
        source_id: container:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1,container=nginx⟩,
        target_id: pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩,
        updated_at: {now_ms}
    }};
    
    -- node_with_system: node-1 -> system
    UPSERT node_with_system:⟨bcs_cluster_id=BCS-K8S-001,node=node-1|bk_cloud_id=0,bk_target_ip=10.0.0.1⟩ MERGE {{
        source_id: node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩,
        target_id: system:⟨bk_cloud_id=0,bk_target_ip=10.0.0.1⟩,
        updated_at: {now_ms}
    }};
    """
    
    result = client.execute(relations_sql)
    errors = [r for r in result if r.get('status') == 'ERR']
    if errors:
        print(f"Errors creating relations: {errors}")
        return 1
    print("✓ Relations created successfully")
    
    # 3. Verify data
    print("\n3. Verifying data...")
    
    verify_sql = """
    SELECT count() as cnt FROM pod GROUP ALL;
    SELECT count() as cnt FROM node GROUP ALL;
    SELECT count() as cnt FROM node_with_pod GROUP ALL;
    SELECT count() as cnt FROM pod_liveness_record GROUP ALL;
    SELECT count() as cnt FROM node_with_pod_liveness_record GROUP ALL;
    """
    
    result = client.execute(verify_sql)
    print("Data counts:")
    tables = ['pod', 'node', 'node_with_pod', 'pod_liveness_record', 'node_with_pod_liveness_record']
    for i, table in enumerate(tables):
        if result[i].get('status') == 'OK':
            cnt = result[i].get('result', [{}])[0].get('cnt', 0)
            print(f"  - {table}: {cnt}")
    
    # 4. Test query with source_id/target_id
    print("\n4. Testing query with new schema...")
    
    start = now_ms - 3600000
    end = now_ms + 3600000
    
    query_sql = f"""
    LET $start = {start};
    LET $end = {end};
    
    SELECT 
        'pod' AS entity_type,
        <string>id AS entity_id,
        {{
            bcs_cluster_id: bcs_cluster_id,
            namespace: namespace,
            pod: pod
        }} AS entity_data,
        (SELECT * FROM pod_liveness_record 
         WHERE pod_id = $parent.id 
         AND $end >= period_start AND $start <= period_end) AS liveness,
        {{
            node_with_pod: (SELECT 
                1 AS hop,
                'node_with_pod' AS relation_type,
                <string>id AS relation_id,
                (SELECT * FROM node_with_pod_liveness_record 
                 WHERE relation_id = $parent.id 
                 AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                {{
                    entity_type: 'node',
                    entity_id: <string>source_id,
                    entity_data: source_id.*,
                    liveness: (SELECT * FROM node_liveness_record 
                               WHERE node_id = $parent.source_id 
                               AND $end >= period_start AND $start <= period_end)
                }} AS target
            FROM node_with_pod WHERE target_id = $parent.id 
               AND (SELECT count() FROM node_with_pod_liveness_record 
                    WHERE relation_id = $parent.id 
                    AND $end >= period_start AND $start <= period_end 
                    GROUP ALL)[0].count > 0),
            
            container_with_pod: (SELECT 
                1 AS hop,
                'container_with_pod' AS relation_type,
                <string>id AS relation_id,
                (SELECT * FROM container_with_pod_liveness_record 
                 WHERE relation_id = $parent.id 
                 AND $end >= period_start AND $start <= period_end) AS relation_liveness,
                {{
                    entity_type: 'container',
                    entity_id: <string>source_id,
                    entity_data: source_id.*,
                    liveness: (SELECT * FROM container_liveness_record 
                               WHERE container_id = $parent.source_id 
                               AND $end >= period_start AND $start <= period_end)
                }} AS target
            FROM container_with_pod WHERE target_id = $parent.id 
               AND (SELECT count() FROM container_with_pod_liveness_record 
                    WHERE relation_id = $parent.id 
                    AND $end >= period_start AND $start <= period_end 
                    GROUP ALL)[0].count > 0)
        }} AS hop1
    FROM pod WHERE id = pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=nginx-1⟩;
    """
    
    result = client.execute(query_sql)
    
    # Find the SELECT result (last one)
    select_result = None
    for r in result:
        if r.get('status') == 'OK' and r.get('result'):
            select_result = r
    
    if select_result and select_result.get('status') == 'OK':
        print("✓ Query executed successfully!")
        import json
        print("\nQuery result:")
        print(json.dumps(select_result.get('result', []), indent=2, default=str))
    else:
        print(f"Query failed: {result}")
        return 1
    
    print("\n" + "=" * 60)
    print("All tests passed! New schema is working correctly.")
    print("=" * 60)
    
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
