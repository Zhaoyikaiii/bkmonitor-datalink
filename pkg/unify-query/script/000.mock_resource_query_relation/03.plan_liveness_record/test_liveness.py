#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Liveness Record - Plan 03

This script tests the liveness record mechanism for resources and relations:
1. Test pod resource liveness (create, renewal, expiry)
2. Test node resource liveness
3. Test node_with_pod relation liveness
4. Verify ID format follows document specification

ID Format (per document):
- Resource: {resource_type}:⟨key1=value1,key2=value2,...⟩
- Relation: {relation_table}:⟨from_keys|to_keys⟩

Usage:
    python test_liveness.py --init-schema    # Initialize schema first
    python test_liveness.py                  # Run tests
"""

import argparse
import logging
import os
import sys
from typing import Any, Dict, List

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# Logging Configuration
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration Loading
# ============================================================================

def _load_yaml_config(filename: str) -> Dict[str, Any]:
    """Load YAML configuration file"""
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required. Install with: pip install pyyaml")
    
    if not os.path.exists(filename):
        return {}
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = yaml.safe_load(f)
            return content if content is not None else {}
    except Exception as e:
        logger.warning(f"Failed to load {filename}: {e}")
        return {}


_script_dir = os.path.dirname(os.path.abspath(__file__))
_config_file = os.path.join(_script_dir, 'config.yaml')
_config = _load_yaml_config(_config_file) if os.path.exists(_config_file) else {}

# SurrealDB Configuration
_surreal_cfg = _config.get('surreal_db', {})
SURREAL_URL = _surreal_cfg.get('url', 'http://localhost:8000')
SURREAL_USER = _surreal_cfg.get('username', 'root')
SURREAL_PASS = _surreal_cfg.get('password', 'root')
SURREAL_NS = _surreal_cfg.get('namespace', 'test')
SURREAL_DB = _surreal_cfg.get('database', 'test')

# Tolerance time in seconds (default: 300 = 5 minutes)
TOLERANCE_SEC = _config.get('mock', {}).get('tolerance_time_ms', 300)

# Schema file
SCHEMA_FILE = os.path.join(_script_dir, 'schema.sql')


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
        logger.info(f"SurrealDB client: {self.url}/{self.namespace}/{self.database}")

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
        for i, result in enumerate(results):
            if result.get('status') == 'ERR':
                error_detail = result.get('detail') or result.get('result', 'Unknown error')
                if 'already exists' in str(error_detail).lower():
                    continue
                raise Exception(f"SQL error in statement {i}: {error_detail}")

        return results[1:] if len(results) > 1 else results

    def init_schema(self, tolerance_sec: int = TOLERANCE_SEC) -> None:
        """Initialize database schema from external SQL file"""
        if not os.path.exists(SCHEMA_FILE):
            raise FileNotFoundError(f"Schema file not found: {SCHEMA_FILE}")
        
        with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # Replace placeholder with actual tolerance value
        schema_sql = schema_sql.replace('{tolerance_time_ms}', str(tolerance_sec))
        
        # Smart split: handle statements with {} blocks correctly
        statements = self._split_sql_statements(schema_sql)
        
        logger.info(f"Executing {len(statements)} schema statements with tolerance={tolerance_sec}s...")
        
        for i, stmt in enumerate(statements):
            if not stmt or stmt.startswith('--'):
                continue
            try:
                self.execute_sql(stmt)
            except Exception as e:
                if 'already exists' not in str(e).lower():
                    logger.warning(f"Statement {i} warning: {e}")
        
        logger.info("Schema initialization completed")

    def _split_sql_statements(self, sql: str) -> list:
        """Split SQL statements, handling {} blocks correctly"""
        statements = []
        current = []
        brace_depth = 0
        
        for line in sql.split('\n'):
            stripped = line.strip()
            
            # Skip empty lines and comments
            if not stripped or stripped.startswith('--'):
                continue
            
            # Count braces
            brace_depth += stripped.count('{') - stripped.count('}')
            current.append(line)
            
            # If we're at brace depth 0 and line ends with ;, it's end of statement
            if brace_depth == 0 and stripped.endswith(';'):
                stmt = '\n'.join(current).strip()
                if stmt:
                    statements.append(stmt.rstrip(';'))
                current = []
        
        # Handle any remaining content
        if current:
            stmt = '\n'.join(current).strip()
            if stmt:
                statements.append(stmt.rstrip(';'))
        
        return statements

    # ========================================
    # Resource Operations
    # ========================================

    def upsert_pod(self, bcs_cluster_id: str, namespace: str, pod: str, 
                   updated_at: int) -> Dict[str, Any]:
        """Upsert pod with specified updated_at timestamp
        
        ID format: pod:⟨bcs_cluster_id=X,namespace=N,pod=P⟩
        """
        pod_id = f"bcs_cluster_id={bcs_cluster_id},namespace={namespace},pod={pod}"
        sql = f'''
        UPSERT pod:⟨{pod_id}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            namespace: "{namespace}",
            pod: "{pod}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_node(self, bcs_cluster_id: str, node: str, 
                    updated_at: int) -> Dict[str, Any]:
        """Upsert node with specified updated_at timestamp
        
        ID format: node:⟨bcs_cluster_id=X,node=N⟩
        """
        node_id = f"bcs_cluster_id={bcs_cluster_id},node={node}"
        sql = f'''
        UPSERT node:⟨{node_id}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            node: "{node}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_relation(self, relation_table: str, from_id: str, to_id: str, 
                        updated_at: int) -> Dict[str, Any]:
        """Upsert relation using fn::upsert_relation
        
        Relation ID format: {relation_table}:⟨from_keys|to_keys⟩
        """
        sql = f'''
        fn::upsert_relation("{relation_table}", {from_id}, {to_id}, {updated_at});
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    # ========================================
    # Query Operations
    # ========================================

    def get_records(self, table: str) -> List[Dict]:
        """Get all records from a table"""
        results = self.execute_sql(f"SELECT * FROM {table};")
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def get_liveness_records(self, resource_type: str, resource_id: str = None) -> List[Dict]:
        """Get liveness records for a resource type"""
        liveness_table = f"{resource_type}_liveness_record"
        id_field = f"{resource_type}_id"
        
        if resource_id:
            sql = f"SELECT * FROM {liveness_table} WHERE {id_field} = {resource_id} ORDER BY created_at ASC;"
        else:
            sql = f"SELECT * FROM {liveness_table} ORDER BY created_at ASC;"
        
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def get_relation_liveness_records(self, table: str, relation_id: str = None) -> List[Dict]:
        """Get relation liveness records"""
        liveness_table = f"{table}_liveness_record"
        if relation_id:
            sql = f"SELECT * FROM {liveness_table} WHERE relation_id = {relation_id} ORDER BY created_at ASC;"
        else:
            sql = f"SELECT * FROM {liveness_table} ORDER BY created_at ASC;"
        
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def clear_test_data(self) -> None:
        """Clear test data"""
        tables = [
            'node_with_pod_liveness_record', 'node_with_pod',
            'pod_liveness_record', 'pod',
            'node_liveness_record', 'node'
        ]
        for table in tables:
            try:
                self.execute_sql(f"DELETE FROM {table};")
            except:
                pass
        logger.info("Test data cleared")


# ============================================================================
# Test Cases
# ============================================================================

class TestLiveness:
    """Test cases for liveness record"""

    def __init__(self, client: SurrealDBClient):
        self.client = client
        self.passed = 0
        self.failed = 0

    def assert_equal(self, actual, expected, msg: str):
        """Assert equality and log result"""
        if actual == expected:
            logger.info(f"  ✓ PASS: {msg}")
            self.passed += 1
            return True
        else:
            logger.error(f"  ✗ FAIL: {msg}")
            logger.error(f"    Expected: {expected}")
            logger.error(f"    Actual: {actual}")
            self.failed += 1
            return False

    def assert_true(self, condition: bool, msg: str):
        """Assert condition is true"""
        if condition:
            logger.info(f"  ✓ PASS: {msg}")
            self.passed += 1
            return True
        else:
            logger.error(f"  ✗ FAIL: {msg}")
            self.failed += 1
            return False

    def run_all_tests(self):
        """Run all test cases"""
        logger.info("=" * 60)
        logger.info("Starting Liveness Record Tests")
        logger.info("=" * 60)

        # Clear existing test data
        self.client.clear_test_data()

        # Test timestamps
        T0 = 1766462400  # 2025-12-23 12:00:00
        T1 = 1766462699  # 2025-12-23 12:04:59 (T0 + 299s)
        T2 = 1766463000  # 2025-12-23 12:10:00 (T1 + 301s)
        T3 = 1766463180  # 2025-12-23 12:13:00 (T2 + 180s)

        # Test Pod Liveness
        self.test_pod_create(T0)
        self.test_pod_renewal(T0, T1)
        self.test_pod_expired(T1, T2)
        self.test_pod_renewal_after_expiry(T2, T3)
        self.test_pod_id_format()

        # Test Node Liveness
        self.test_node_create(T0)
        self.test_node_renewal(T0, T1)

        # Test Relation Liveness
        self.test_relation_create(T0)
        self.test_relation_renewal(T0, T1)
        self.test_relation_expired(T1, T2)
        self.test_relation_id_format()

        # Range Query Test
        self.test_range_query(T0, T1, T2, T3)

        # Summary
        logger.info("=" * 60)
        logger.info(f"Test Summary: {self.passed} passed, {self.failed} failed")
        logger.info("=" * 60)

        return self.failed == 0

    # ========================================
    # Pod Tests
    # ========================================

    def test_pod_create(self, t0: int):
        """Test T0: Create pod"""
        logger.info("\n--- Test: Pod Create ---")
        
        self.client.upsert_pod("BCS-K8S-001", "default", "pod-0", t0)
        
        pods = self.client.get_records("pod")
        self.assert_equal(len(pods), 1, "Should have 1 pod")
        
        if pods:
            pod = pods[0]
            self.assert_equal(pod.get('updated_at'), t0, "Pod updated_at should be T0")
            self.assert_equal(pod.get('created_at'), t0, "Pod created_at should be T0")
        
        liveness = self.client.get_liveness_records("pod")
        self.assert_equal(len(liveness), 1, "Should have 1 liveness record")
        
        if liveness:
            record = liveness[0]
            self.assert_equal(record.get('period_start'), t0, "period_start should be T0")
            self.assert_equal(record.get('period_end'), t0, "period_end should be T0")
            self.assert_equal(record.get('is_active'), True, "is_active should be True")

    def test_pod_renewal(self, t0: int, t1: int):
        """Test T1: Pod renewal within tolerance"""
        logger.info(f"\n--- Test: Pod Renewal (delta: {t1-t0}s, tolerance: {TOLERANCE_SEC}s) ---")
        
        self.client.upsert_pod("BCS-K8S-001", "default", "pod-0", t1)
        
        pods = self.client.get_records("pod")
        if pods:
            pod = pods[0]
            self.assert_equal(pod.get('updated_at'), t1, "Pod updated_at should be T1")
            self.assert_equal(pod.get('created_at'), t0, "Pod created_at should still be T0")
        
        liveness = self.client.get_liveness_records("pod")
        self.assert_equal(len(liveness), 1, "Should still have 1 liveness record")
        
        if liveness:
            record = liveness[0]
            self.assert_equal(record.get('period_start'), t0, "period_start should still be T0")
            self.assert_equal(record.get('period_end'), t1, "period_end should be extended to T1")
            self.assert_equal(record.get('is_active'), True, "is_active should still be True")

    def test_pod_expired(self, t1: int, t2: int):
        """Test T2: Pod expired (beyond tolerance)"""
        logger.info(f"\n--- Test: Pod Expired (delta: {t2-t1}s, tolerance: {TOLERANCE_SEC}s) ---")
        
        self.client.upsert_pod("BCS-K8S-001", "default", "pod-0", t2)
        
        liveness = self.client.get_liveness_records("pod")
        self.assert_equal(len(liveness), 2, "Should have 2 liveness records")
        
        if len(liveness) >= 2:
            sorted_records = sorted(liveness, key=lambda r: r.get('created_at', 0))
            old_record = sorted_records[0]
            new_record = sorted_records[1]
            
            self.assert_equal(old_record.get('is_active'), False, "Old record should be inactive")
            self.assert_equal(old_record.get('period_end'), t1, "Old record period_end should be T1")
            
            self.assert_equal(new_record.get('is_active'), True, "New record should be active")
            self.assert_equal(new_record.get('period_start'), t2, "New record period_start should be T2")
            self.assert_equal(new_record.get('period_end'), t2, "New record period_end should be T2")

    def test_pod_renewal_after_expiry(self, t2: int, t3: int):
        """Test T3: Pod renewal after expiry"""
        logger.info(f"\n--- Test: Pod Renewal After Expiry (delta: {t3-t2}s) ---")
        
        self.client.upsert_pod("BCS-K8S-001", "default", "pod-0", t3)
        
        liveness = self.client.get_liveness_records("pod")
        self.assert_equal(len(liveness), 2, "Should still have 2 liveness records")
        
        if len(liveness) >= 2:
            sorted_records = sorted(liveness, key=lambda r: r.get('created_at', 0))
            new_record = sorted_records[1]
            
            self.assert_equal(new_record.get('is_active'), True, "New record should still be active")
            self.assert_equal(new_record.get('period_start'), t2, "New record period_start should still be T2")
            self.assert_equal(new_record.get('period_end'), t3, "New record period_end should be extended to T3")

    def test_pod_id_format(self):
        """Test pod ID format"""
        logger.info("\n--- Test: Pod ID Format ---")
        
        pods = self.client.get_records("pod")
        if pods:
            pod_id = str(pods[0].get('id', ''))
            logger.info(f"Pod ID: {pod_id}")
            
            self.assert_true(pod_id.startswith('pod:'), "ID should start with 'pod:'")
            self.assert_true('bcs_cluster_id=BCS-K8S-001' in pod_id, "ID should contain bcs_cluster_id")
            self.assert_true('namespace=default' in pod_id, "ID should contain namespace")
            self.assert_true('pod=pod-0' in pod_id, "ID should contain pod")

    # ========================================
    # Node Tests
    # ========================================

    def test_node_create(self, t0: int):
        """Test: Create node"""
        logger.info("\n--- Test: Node Create ---")
        
        self.client.upsert_node("BCS-K8S-001", "node-0", t0)
        
        nodes = self.client.get_records("node")
        self.assert_equal(len(nodes), 1, "Should have 1 node")
        
        liveness = self.client.get_liveness_records("node")
        self.assert_equal(len(liveness), 1, "Should have 1 node liveness record")

    def test_node_renewal(self, t0: int, t1: int):
        """Test: Node renewal"""
        logger.info("\n--- Test: Node Renewal ---")
        
        self.client.upsert_node("BCS-K8S-001", "node-0", t1)
        
        liveness = self.client.get_liveness_records("node")
        self.assert_equal(len(liveness), 1, "Should still have 1 node liveness record")
        
        if liveness:
            record = liveness[0]
            self.assert_equal(record.get('period_end'), t1, "Node period_end should be extended to T1")

    # ========================================
    # Relation Tests
    # ========================================

    def test_relation_create(self, t0: int):
        """Test: Create relation"""
        logger.info("\n--- Test: Relation Create ---")
        
        node_id = "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-0⟩"
        pod_id = "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
        self.client.upsert_relation("node_with_pod", node_id, pod_id, t0)
        
        relations = self.client.get_records("node_with_pod")
        self.assert_equal(len(relations), 1, "Should have 1 relation")
        
        liveness = self.client.get_relation_liveness_records("node_with_pod")
        self.assert_equal(len(liveness), 1, "Should have 1 relation liveness record")

    def test_relation_renewal(self, t0: int, t1: int):
        """Test: Relation renewal"""
        logger.info("\n--- Test: Relation Renewal ---")
        
        node_id = "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-0⟩"
        pod_id = "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
        self.client.upsert_relation("node_with_pod", node_id, pod_id, t1)
        
        liveness = self.client.get_relation_liveness_records("node_with_pod")
        self.assert_equal(len(liveness), 1, "Should still have 1 relation liveness record")
        
        if liveness:
            record = liveness[0]
            self.assert_equal(record.get('period_end'), t1, "Relation period_end should be extended to T1")

    def test_relation_expired(self, t1: int, t2: int):
        """Test: Relation expired"""
        logger.info("\n--- Test: Relation Expired ---")
        
        node_id = "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-0⟩"
        pod_id = "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
        self.client.upsert_relation("node_with_pod", node_id, pod_id, t2)
        
        liveness = self.client.get_relation_liveness_records("node_with_pod")
        self.assert_equal(len(liveness), 2, "Should have 2 relation liveness records")

    def test_relation_id_format(self):
        """Test relation ID format"""
        logger.info("\n--- Test: Relation ID Format ---")
        
        relations = self.client.get_records("node_with_pod")
        if relations:
            rel_id = str(relations[0].get('id', ''))
            logger.info(f"Relation ID: {rel_id}")
            
            self.assert_true(rel_id.startswith('node_with_pod:'), "ID should start with 'node_with_pod:'")
            self.assert_true('|' in rel_id, "ID should contain '|' separator")
            self.assert_true('bcs_cluster_id=BCS-K8S-001' in rel_id, "ID should contain bcs_cluster_id")
            self.assert_true('node=node-0' in rel_id, "ID should contain node")
            self.assert_true('pod=pod-0' in rel_id, "ID should contain pod")

    # ========================================
    # Range Query Test
    # ========================================

    def test_range_query(self, t0: int, t1: int, t2: int, t3: int):
        """Test range query for liveness"""
        logger.info("\n--- Test: Range Query ---")
        
        # Query at T0+150s (should be in first period)
        query_time_1 = t0 + 150
        sql = f'''
        SELECT * FROM pod_liveness_record 
        WHERE {query_time_1} >= period_start AND {query_time_1} <= period_end;
        '''
        results = self.client.execute_sql(sql)
        count_1 = len(results[0].get('result', [])) if results else 0
        self.assert_equal(count_1, 1, f"Query at T0+150s should find 1 record")
        
        # Query at T1+150s (gap between periods)
        query_time_2 = t1 + 150
        sql = f'''
        SELECT * FROM pod_liveness_record 
        WHERE {query_time_2} >= period_start AND {query_time_2} <= period_end;
        '''
        results = self.client.execute_sql(sql)
        count_2 = len(results[0].get('result', [])) if results else 0
        self.assert_equal(count_2, 0, f"Query at T1+150s (gap) should find 0 records")
        
        # Query at T2+90s (should be in second period)
        query_time_3 = t2 + 90
        sql = f'''
        SELECT * FROM pod_liveness_record 
        WHERE {query_time_3} >= period_start AND {query_time_3} <= period_end;
        '''
        results = self.client.execute_sql(sql)
        count_3 = len(results[0].get('result', [])) if results else 0
        self.assert_equal(count_3, 1, f"Query at T2+90s should find 1 record")


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Test Liveness Record - Plan 03'
    )
    parser.add_argument(
        '--init-schema', action='store_true',
        help='Initialize database schema before running tests'
    )
    parser.add_argument(
        '--debug', action='store_true',
        help='Enable debug logging'
    )
    parser.add_argument(
        '--tolerance', type=int, default=TOLERANCE_SEC,
        help=f'Tolerance time in seconds (default: {TOLERANCE_SEC})'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize client
    client = SurrealDBClient()
    
    # Initialize schema if requested
    if args.init_schema:
        logger.info("Initializing schema...")
        client.init_schema(args.tolerance)
    
    # Run tests
    tester = TestLiveness(client)
    success = tester.run_all_tests()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
