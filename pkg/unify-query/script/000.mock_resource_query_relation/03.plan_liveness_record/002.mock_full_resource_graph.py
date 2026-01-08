#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mock Full Resource Graph to SurrealDB - Plan 03: Liveness Record

This script generates mock resource association data for all resource types.
Unlike Plan 01 (active_windows), this plan uses:
- Separate liveness record tables (pod_liveness_record, etc.)
- SurrealDB Events to automatically manage liveness records
- Clean separation of business data and lifecycle tracking

Key Differences from Plan 01:
- Plan 01: Uses active_windows array embedded in resource table
- Plan 03: Uses separate liveness_record tables for lifecycle tracking

Write Pattern:
    UPSERT pod:⟨bcs_cluster_id=X,namespace=N,pod=P⟩ MERGE {
        bcs_cluster_id: "X",
        namespace: "N", 
        pod: "P",
        updated_at: <timestamp_ms>
    };

The Event will automatically:
- Create pod_liveness_record on first insert
- Extend period_end if within tolerance
- Close old record and create new if beyond tolerance

Usage:
    # Initialize schema (first time or to reset)
    python 002.mock_full_resource_graph.py --init-schema
    
    # Generate mock data
    python 002.mock_full_resource_graph.py
    
    # Enable debug logging
    python 002.mock_full_resource_graph.py --debug

Configuration:
    Connection settings are managed in config.yaml
"""

import argparse
import logging
import os
import random
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Tuple

import requests

# Disable SSL warnings for self-signed certificates
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


# Load configuration
_config_file = os.path.join(os.path.dirname(__file__), 'config.yaml')
_config = _load_yaml_config(_config_file) if os.path.exists(_config_file) else {}


# ============================================================================
# Configuration Constants
# ============================================================================

class SurrealDBConfig:
    URL = _config.get('surreal_db', {}).get('url', 'http://localhost:8000')
    USERNAME = _config.get('surreal_db', {}).get('username', 'root')
    PASSWORD = _config.get('surreal_db', {}).get('password', 'root')
    NAMESPACE = _config.get('surreal_db', {}).get('namespace', 'test')
    DATABASE = _config.get('surreal_db', {}).get('database', 'test')


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
    RESULT_TABLE_ID = "2_bkmonitor_bkop_2"
    
    # Network
    CLOUD_ID = "0"
    TARGET_IP = "10.0.0.1"
    
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
    
    # Counts
    SERVICE_LIST = ["api", "web", "worker"]
    NUM_PODS = 10
    NUM_CONTAINERS = 20
    NUM_DEPLOYMENTS = 3
    NUM_NODES = 3
    NUM_SYSTEMS = 5
    NUM_APM_INSTANCES = 3
    
    # Traffic
    POD_TO_POD_TRAFFIC_PROBABILITY = 0.4
    POD_TO_SYSTEM_TRAFFIC_PROBABILITY = 0.3
    SERVICE_TO_SERVICE_TRAFFIC_PROBABILITY = 0.5
    
    # Lifecycle Management
    TOLERANCE_TIME_MS = _config.get('mock', {}).get('tolerance_time_ms', 300000)  # 5 minutes default


# ============================================================================
# Enums - All Resource Types
# ============================================================================

class ResourceType(Enum):
    # Kubernetes Resources
    POD = "pod"
    NODE = "node"
    CONTAINER = "container"
    DEPLOYMENT = "deployment"
    REPLICASET = "replicaset"
    STATEFULSET = "statefulset"
    DAEMONSET = "daemonset"
    JOB = "job"
    SERVICE = "service"
    INGRESS = "ingress"
    CLUSTER = "cluster"
    NAMESPACE = "namespace"
    
    # Network Resources
    SYSTEM = "system"
    K8S_ADDRESS = "k8s_address"
    DOMAIN = "domain"
    
    # APM Resources
    APM_SERVICE = "apm_service"
    APM_SERVICE_INSTANCE = "apm_service_instance"
    
    # Data Source Resources
    DATASOURCE = "datasource"
    BKLOGCONFIG = "bklogconfig"
    
    # CMDB Resources
    BIZ = "biz"
    SET = "set"
    MODULE = "module"
    HOST = "host"
    
    # App Version Resources
    APP_VERSION = "app_version"
    GIT_COMMIT = "git_commit"
    ENVIRONMENT = "environment"
    
    # Metric
    METRIC = "metric"


# ============================================================================
# Enums - All Relation Types
# ============================================================================

class RelationType(Enum):
    # Kubernetes Static Relations
    NODE_WITH_SYSTEM = "node_with_system"
    NODE_WITH_POD = "node_with_pod"
    JOB_WITH_POD = "job_with_pod"
    POD_WITH_REPLICASET = "pod_with_replicaset"
    POD_WITH_STATEFULSET = "pod_with_statefulset"
    DAEMONSET_WITH_POD = "daemonset_with_pod"
    DEPLOYMENT_WITH_REPLICASET = "deployment_with_replicaset"
    POD_WITH_SERVICE = "pod_with_service"
    INGRESS_WITH_SERVICE = "ingress_with_service"
    
    # Network Static Relations
    K8S_ADDRESS_WITH_SERVICE = "k8s_address_with_service"
    DOMAIN_WITH_SERVICE = "domain_with_service"
    
    # APM Static Relations
    APM_SERVICE_INSTANCE_WITH_POD = "apm_service_instance_with_pod"
    APM_SERVICE_INSTANCE_WITH_SYSTEM = "apm_service_instance_with_system"
    APM_SERVICE_WITH_APM_SERVICE_INSTANCE = "apm_service_with_apm_service_instance"
    
    # Container Static Relations
    CONTAINER_WITH_POD = "container_with_pod"
    
    # Data Source Static Relations
    DATASOURCE_WITH_POD = "datasource_with_pod"
    DATASOURCE_WITH_NODE = "datasource_with_node"
    BKLOGCONFIG_WITH_DATASOURCE = "bklogconfig_with_datasource"
    
    # CMDB Static Relations
    BIZ_WITH_SET = "biz_with_set"
    MODULE_WITH_SET = "module_with_set"
    HOST_WITH_MODULE = "host_with_module"
    HOST_WITH_SYSTEM = "host_with_system"
    
    # App Version Static Relations
    APP_VERSION_WITH_CONTAINER = "app_version_with_container"
    APP_VERSION_WITH_SYSTEM = "app_version_with_system"
    CONTAINER_WITH_ENVIRONMENT = "container_with_environment"
    ENVIRONMENT_WITH_SYSTEM = "environment_with_system"
    APP_VERSION_WITH_GIT_COMMIT = "app_version_with_git_commit"
    
    # Dynamic Relations
    POD_TO_POD = "pod_to_pod"
    POD_TO_SYSTEM = "pod_to_system"
    SYSTEM_TO_POD = "system_to_pod"
    SYSTEM_TO_SYSTEM = "system_to_system"
    SERVICE_TO_SERVICE = "service_to_service"
    
    # Metric Relations
    NODE_HAS_METRIC = "node_has_metric"
    RELATION_HAS_METRIC = "relation_has_metric"


# ============================================================================
# Schema Definition
# ============================================================================

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.sql')


def load_schema_sql(tolerance_time_ms: int = None) -> str:
    """Load schema SQL from external file and replace tolerance_time placeholder."""
    if tolerance_time_ms is None:
        tolerance_time_ms = MockConfig.TOLERANCE_TIME_MS
    
    if not os.path.exists(SCHEMA_FILE):
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_FILE}")
    
    with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    # Replace {tolerance_time_ms} placeholder with actual value
    schema_sql = schema_sql.replace('{tolerance_time_ms}', str(tolerance_time_ms))
    
    logger.info(f"Loaded schema from {SCHEMA_FILE} with tolerance_time_ms={tolerance_time_ms}")
    return schema_sql


# ============================================================================
# Storage Client - Liveness Record Version
# ============================================================================

class SurrealDBClient:
    """
    SurrealDB HTTP REST API client for Liveness Record schema.
    
    Key difference from Plan 01:
    - Uses simple UPSERT MERGE syntax
    - Events automatically manage liveness_record tables
    - Separate tables for lifecycle tracking
    """

    def __init__(
            self,
            url: str = SurrealDBConfig.URL,
            username: str = SurrealDBConfig.USERNAME,
            password: str = SurrealDBConfig.PASSWORD,
            namespace: str = SurrealDBConfig.NAMESPACE,
            database: str = SurrealDBConfig.DATABASE
    ):
        self.url = url
        self.username = username
        self.password = password
        self.namespace = namespace
        self.database = database
        self.session = requests.Session()
        self.session.verify = False
        logger.info(f"SurrealDB client initialized: {url}/{namespace}/{database}")

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
                if 'already exists' in str(error_detail).lower():
                    continue
                raise Exception(f"SQL error in statement {i}: {error_detail}")

        return results[1:] if len(results) > 1 else results

    def datetime_to_ms(self, dt: datetime = None) -> int:
        """Convert datetime to milliseconds timestamp"""
        if dt is None:
            dt = datetime.utcnow()
        return int(dt.timestamp() * 1000)

    def init_schema(self, tolerance_time_ms: int = None) -> None:
        """Initialize database schema from external SQL file"""
        schema_sql = load_schema_sql(tolerance_time_ms)
        
        # Smart split: handle statements with {} blocks correctly
        statements = self._split_sql_statements(schema_sql)
        
        logger.info(f"Executing {len(statements)} schema statements...")
        
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

    # ========================================================================
    # Resource UPSERT Methods (Liveness Record Version)
    # ========================================================================

    def upsert_pod(self, bcs_cluster_id: str, namespace: str, pod: str, 
                   updated_at: int = None) -> Dict[str, Any]:
        """Upsert pod - Event will manage liveness record"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        UPSERT pod:⟨bcs_cluster_id={bcs_cluster_id},namespace={namespace},pod={pod}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            namespace: "{namespace}",
            pod: "{pod}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_node(self, bcs_cluster_id: str, node: str, 
                    updated_at: int = None) -> Dict[str, Any]:
        """Upsert node - Event will manage liveness record"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        UPSERT node:⟨bcs_cluster_id={bcs_cluster_id},node={node}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            node: "{node}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_container(self, bcs_cluster_id: str, namespace: str, pod: str, 
                         container: str, updated_at: int = None) -> Dict[str, Any]:
        """Upsert container - Event will manage liveness record"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        UPSERT container:⟨bcs_cluster_id={bcs_cluster_id},namespace={namespace},pod={pod},container={container}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            namespace: "{namespace}",
            pod: "{pod}",
            container: "{container}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_deployment(self, bcs_cluster_id: str, namespace: str, deployment: str,
                          updated_at: int = None) -> Dict[str, Any]:
        """Upsert deployment - Event will manage liveness record"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        UPSERT deployment:⟨bcs_cluster_id={bcs_cluster_id},namespace={namespace},deployment={deployment}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            namespace: "{namespace}",
            deployment: "{deployment}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_replicaset(self, bcs_cluster_id: str, namespace: str, replicaset: str,
                          updated_at: int = None) -> Dict[str, Any]:
        """Upsert replicaset - Event will manage liveness record"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        UPSERT replicaset:⟨bcs_cluster_id={bcs_cluster_id},namespace={namespace},replicaset={replicaset}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            namespace: "{namespace}",
            replicaset: "{replicaset}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_service(self, bcs_cluster_id: str, namespace: str, service: str,
                       updated_at: int = None) -> Dict[str, Any]:
        """Upsert service - Event will manage liveness record"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        UPSERT service:⟨bcs_cluster_id={bcs_cluster_id},namespace={namespace},service={service}⟩ MERGE {{
            bcs_cluster_id: "{bcs_cluster_id}",
            namespace: "{namespace}",
            service: "{service}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    def upsert_system(self, bk_target_ip: str, bk_cloud_id: str,
                      updated_at: int = None) -> Dict[str, Any]:
        """Upsert system - Event will manage liveness record
        
        ID format: system:⟨bk_cloud_id=X,bk_target_ip=Y⟩
        """
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        UPSERT system:⟨bk_cloud_id={bk_cloud_id},bk_target_ip={bk_target_ip}⟩ MERGE {{
            bk_target_ip: "{bk_target_ip}",
            bk_cloud_id: "{bk_cloud_id}",
            updated_at: {updated_at}
        }};
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    # ========================================================================
    # Relation UPSERT Methods
    # ========================================================================

    def upsert_relation(self, relation_type: str, from_id: str, to_id: str,
                        updated_at: int = None) -> Dict[str, Any]:
        """Upsert relation using fn::upsert_relation function"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        sql = f'''
        fn::upsert_relation("{relation_type}", {from_id}, {to_id}, {updated_at});
        '''
        results = self.execute_sql(sql)
        return results[0] if results else {}

    # ========================================================================
    # Liveness Query Methods
    # ========================================================================

    def check_liveness_range_exists(self, table_suffix: str, record_id: str,
                                    start_time: int, end_time: int) -> bool:
        """Check if a record has liveness in time range"""
        sql = f'''
        fn::check_liveness_range_exists("{table_suffix}", {record_id}, {start_time}, {end_time});
        '''
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return False

    def is_alive_at(self, table_suffix: str, record_id: str, check_time: int) -> bool:
        """Check if a record is alive at a specific time"""
        sql = f'''
        fn::is_alive_at("{table_suffix}", {record_id}, {check_time});
        '''
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return False

    def get_liveness_records(self, table_suffix: str, record_id: str,
                             start_time: int, end_time: int) -> List[Dict]:
        """Get liveness records for a resource in time range"""
        sql = f'''
        fn::get_liveness_records("{table_suffix}", {record_id}, {start_time}, {end_time});
        '''
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def query_alive_resources(self, resource_type: str, check_time: int,
                              conditions: str = "") -> List[Dict]:
        """Query resources that are alive at a specific time"""
        where_clause = f"fn::is_alive_at(\"{resource_type}\", id, {check_time})"
        if conditions:
            where_clause = f"{conditions} AND {where_clause}"
        
        sql = f'''
        SELECT * FROM {resource_type} WHERE {where_clause};
        '''
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []


# ============================================================================
# Mock Data Generator
# ============================================================================

class MockDataGenerator:
    """Generate mock resource data for testing"""

    def __init__(self, client: SurrealDBClient):
        self.client = client
        self.pods = []
        self.nodes = []
        self.containers = []
        self.deployments = []
        self.replicasets = []
        self.services = []
        self.systems = []

    def generate_all(self, updated_at: int = None) -> Dict[str, int]:
        """Generate all mock data and return counts"""
        if updated_at is None:
            updated_at = self.client.datetime_to_ms()
        
        counts = {}
        
        # Generate nodes
        logger.info("Generating nodes...")
        for i in range(MockConfig.NUM_NODES):
            node_name = f"node-{i}"
            self.client.upsert_node(MockConfig.CLUSTER_ID, node_name, updated_at)
            self.nodes.append({
                'bcs_cluster_id': MockConfig.CLUSTER_ID,
                'node': node_name
            })
        counts['nodes'] = len(self.nodes)
        
        # Generate deployments
        logger.info("Generating deployments...")
        for i in range(MockConfig.NUM_DEPLOYMENTS):
            deployment_name = f"deployment-{i}"
            self.client.upsert_deployment(
                MockConfig.CLUSTER_ID, MockConfig.NAMESPACE, deployment_name, updated_at
            )
            self.deployments.append({
                'bcs_cluster_id': MockConfig.CLUSTER_ID,
                'namespace': MockConfig.NAMESPACE,
                'deployment': deployment_name
            })
        counts['deployments'] = len(self.deployments)
        
        # Generate replicasets
        logger.info("Generating replicasets...")
        for deployment in self.deployments:
            replicaset_name = f"{deployment['deployment']}-rs"
            self.client.upsert_replicaset(
                MockConfig.CLUSTER_ID, MockConfig.NAMESPACE, replicaset_name, updated_at
            )
            self.replicasets.append({
                'bcs_cluster_id': MockConfig.CLUSTER_ID,
                'namespace': MockConfig.NAMESPACE,
                'replicaset': replicaset_name,
                'deployment': deployment['deployment']
            })
        counts['replicasets'] = len(self.replicasets)
        
        # Generate pods
        logger.info("Generating pods...")
        for i in range(MockConfig.NUM_PODS):
            pod_name = f"pod-{i}"
            self.client.upsert_pod(
                MockConfig.CLUSTER_ID, MockConfig.NAMESPACE, pod_name, updated_at
            )
            # Assign to a random replicaset
            rs = random.choice(self.replicasets) if self.replicasets else None
            self.pods.append({
                'bcs_cluster_id': MockConfig.CLUSTER_ID,
                'namespace': MockConfig.NAMESPACE,
                'pod': pod_name,
                'replicaset': rs['replicaset'] if rs else None
            })
        counts['pods'] = len(self.pods)
        
        # Generate containers
        logger.info("Generating containers...")
        for i in range(MockConfig.NUM_CONTAINERS):
            pod = random.choice(self.pods)
            container_name = f"container-{i}"
            self.client.upsert_container(
                pod['bcs_cluster_id'], pod['namespace'], pod['pod'], 
                container_name, updated_at
            )
            self.containers.append({
                'bcs_cluster_id': pod['bcs_cluster_id'],
                'namespace': pod['namespace'],
                'pod': pod['pod'],
                'container': container_name
            })
        counts['containers'] = len(self.containers)
        
        # Generate services
        logger.info("Generating services...")
        for service_name in MockConfig.SERVICE_LIST:
            self.client.upsert_service(
                MockConfig.CLUSTER_ID, MockConfig.NAMESPACE, service_name, updated_at
            )
            self.services.append({
                'bcs_cluster_id': MockConfig.CLUSTER_ID,
                'namespace': MockConfig.NAMESPACE,
                'service': service_name
            })
        counts['services'] = len(self.services)
        
        # Generate systems
        logger.info("Generating systems...")
        for i in range(MockConfig.NUM_SYSTEMS):
            ip = f"10.0.0.{i+1}"
            self.client.upsert_system(ip, MockConfig.CLOUD_ID, updated_at)
            self.systems.append({
                'bk_target_ip': ip,
                'bk_cloud_id': MockConfig.CLOUD_ID
            })
        counts['systems'] = len(self.systems)
        
        logger.info(f"Mock data generation completed: {counts}")
        return counts


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Mock Full Resource Graph - Plan 03: Liveness Record'
    )
    parser.add_argument(
        '--init-schema', action='store_true',
        help='Initialize database schema before generating data'
    )
    parser.add_argument(
        '--debug', action='store_true',
        help='Enable debug logging'
    )
    parser.add_argument(
        '--tolerance-ms', type=int, default=MockConfig.TOLERANCE_TIME_MS,
        help=f'Tolerance time in milliseconds (default: {MockConfig.TOLERANCE_TIME_MS})'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize client
    client = SurrealDBClient()
    
    # Initialize schema if requested
    if args.init_schema:
        logger.info("Initializing schema...")
        client.init_schema(args.tolerance_ms)
    
    # Generate mock data
    logger.info("Generating mock data...")
    generator = MockDataGenerator(client)
    counts = generator.generate_all()
    
    logger.info("=" * 60)
    logger.info("Mock data generation completed!")
    logger.info(f"Generated resources: {counts}")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
