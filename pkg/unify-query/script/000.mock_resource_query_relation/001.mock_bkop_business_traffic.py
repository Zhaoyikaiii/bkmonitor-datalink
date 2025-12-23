#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mock BKOP Business 2 Traffic to SurrealDB with Metrics

This script generates mock resource association data for BKOP Business 2,
including static relations and dynamic traffic with metrics (flow_total, flow_seconds, flow_error).

Key Features:
    - Uses SurrealDB RELATION type tables for proper graph traversal
    - Uses document-defined ID format for nodes and relations
    - Supports both native SurrealDB and BKBase backends
    - Idempotent: can be run multiple times without data conflicts

ID Format (per documentation):
    - Node ID: {resource_type}:{key1}={value1},{key2}={value2},...
    - Static Relation ID: {res1}_with_{res2}:{res1_kv}|{res2_kv} (res1 < res2 alphabetically)
    - Dynamic Relation ID: {src}_to_{dst}:{src_kv}|{dst_kv}

Usage:
    # Initialize schema (first time or to reset)
    python 001.mock_bkop_business_traffic.py --backend native --init-schema
    
    # Use native SurrealDB (default)
    python 001.mock_bkop_business_traffic.py --backend native
    
    # Enable debug logging
    python 001.mock_bkop_business_traffic.py --backend=native --debug

Configuration:
    Connection settings are managed in config.yaml
"""

import abc
import argparse
import json
import logging
import os
import random
import sys
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional, Tuple

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

def _parse_backend_from_args() -> str:
    """Parse --backend argument from command line"""
    for i, arg in enumerate(sys.argv):
        if arg == '--backend' and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        elif arg.startswith('--backend='):
            return arg.split('=', 1)[1]
    return 'native'


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

class StorageBackend(Enum):
    NATIVE = "native"
    BKBASE = "bkbase"


class SurrealDBConfig:
    URL = _config.get('surreal_db', {}).get('url', 'http://localhost:8000')
    USERNAME = _config.get('surreal_db', {}).get('username', 'root')
    PASSWORD = _config.get('surreal_db', {}).get('password', 'root')
    NAMESPACE = _config.get('surreal_db', {}).get('namespace', 'test')
    DATABASE = _config.get('surreal_db', {}).get('database', 'test')


class BKBaseConfig:
    API_URL = _config.get('bkbase', {}).get('api_url', '')
    USERNAME = _config.get('bkbase', {}).get('username', '')
    APP_CODE = _config.get('bkbase', {}).get('app_code', '')
    APP_SECRET = _config.get('bkbase', {}).get('app_secret', '')
    RESULT_TABLE_ID = _config.get('bkbase', {}).get('result_table_id', '')


class MockConfig:
    """Mock data generation configuration"""
    BIZ_ID = "2"
    BIZ_NAME = "bkop"
    CLUSTER_ID = "BCS-K8S-00002"
    NAMESPACE = "bkop"
    RESULT_TABLE_ID = "2_bkmonitor_bkop_2"
    
    SERVICE_LIST = ["api", "web", "worker"]
    NUM_PODS = 10
    NUM_DEPLOYMENTS = 3
    NUM_NODES = 3
    
    POD_TO_POD_TRAFFIC_PROBABILITY = 0.4
    
    FLOW_TOTAL_RANGE = (10, 1000)
    FLOW_SECONDS_RANGE = (0.01, 2.0)
    FLOW_ERROR_RATE_RANGE = (0.0, 0.1)
    
    DEFAULT_TIME_BACK_HOURS = 1
    START_TIME = datetime.now().replace(tzinfo=None) - timedelta(hours=DEFAULT_TIME_BACK_HOURS)
    END_TIME = datetime.now().replace(tzinfo=None)
    METRIC_TIME_POINTS = 12


# ============================================================================
# Enums
# ============================================================================

class ResourceType(Enum):
    POD = "pod"
    NODE = "node"
    SERVICE = "service"
    DEPLOYMENT = "deployment"
    REPLICASET = "replicaset"
    NAMESPACE = "namespace"
    CLUSTER = "cluster"
    BIZ = "biz"
    METRIC = "metric"


class RelationType(Enum):
    # Static relations (bidirectional)
    NODE_WITH_POD = "node_with_pod"
    POD_WITH_SERVICE = "pod_with_service"
    DEPLOYMENT_WITH_REPLICASET = "deployment_with_replicaset"
    POD_WITH_REPLICASET = "pod_with_replicaset"
    # Dynamic relations (directional)
    POD_TO_POD = "pod_to_pod"
    # Metric relations
    RELATION_HAS_METRIC = "relation_has_metric"


class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


# ============================================================================
# Resource Index Fields Definition (per documentation section 2)
# ============================================================================

RESOURCE_INDEX_FIELDS = {
    ResourceType.POD: ["bcs_cluster_id", "namespace", "pod"],
    ResourceType.NODE: ["bcs_cluster_id", "node"],
    ResourceType.SERVICE: ["bcs_cluster_id", "namespace", "service"],
    ResourceType.DEPLOYMENT: ["bcs_cluster_id", "deployment", "namespace"],  # sorted alphabetically
    ResourceType.REPLICASET: ["bcs_cluster_id", "namespace", "replicaset"],
    ResourceType.NAMESPACE: ["bcs_cluster_id", "namespace"],
    ResourceType.CLUSTER: ["bcs_cluster_id"],
    ResourceType.BIZ: ["bk_biz_id"],
    ResourceType.METRIC: ["metric_name"],
}


# ============================================================================
# ID Generation Utilities (per documentation section 4)
# ============================================================================

class IDGenerator:
    """ID generator following documentation section 4 rules"""

    @staticmethod
    def generate_node_id(resource_type: ResourceType, data: Dict[str, Any]) -> str:
        """
        Generate node ID per section 4.1
        Format: {resource_type}:{key1}={value1},{key2}={value2},...
        Keys are sorted alphabetically
        """
        index_fields = RESOURCE_INDEX_FIELDS.get(resource_type, [])
        sorted_keys = sorted(index_fields)
        pairs = [f"{key}={data.get(key, '')}" for key in sorted_keys]
        return f"{resource_type.value}:{','.join(pairs)}"

    @staticmethod
    def generate_kv_string(resource_type: ResourceType, data: Dict[str, Any]) -> str:
        """Generate key=value string for a resource (keys sorted)"""
        index_fields = RESOURCE_INDEX_FIELDS.get(resource_type, [])
        sorted_keys = sorted(index_fields)
        pairs = [f"{key}={data.get(key, '')}" for key in sorted_keys]
        return ','.join(pairs)

    @staticmethod
    def generate_static_relation_id(
            relation_type: RelationType,
            type1: ResourceType,
            data1: Dict[str, Any],
            type2: ResourceType,
            data2: Dict[str, Any]
    ) -> str:
        """
        Generate static (bidirectional) relation ID per section 4.2.1
        Format: {res1}_with_{res2}:{res1_kv}|{res2_kv}
        Where res1 < res2 alphabetically
        """
        kv1 = IDGenerator.generate_kv_string(type1, data1)
        kv2 = IDGenerator.generate_kv_string(type2, data2)
        
        # Ensure res1 < res2 alphabetically
        if type1.value < type2.value:
            return f"{relation_type.value}:{kv1}|{kv2}"
        else:
            return f"{relation_type.value}:{kv2}|{kv1}"

    @staticmethod
    def generate_dynamic_relation_id(
            relation_type: RelationType,
            source_type: ResourceType,
            source_data: Dict[str, Any],
            target_type: ResourceType,
            target_data: Dict[str, Any]
    ) -> str:
        """
        Generate dynamic (directional) relation ID per section 4.2.2
        Format: {src}_to_{dst}:{src_kv}|{dst_kv}
        """
        source_kv = IDGenerator.generate_kv_string(source_type, source_data)
        target_kv = IDGenerator.generate_kv_string(target_type, target_data)
        return f"{relation_type.value}:{source_kv}|{target_kv}"


# ============================================================================
# Schema Definition (per documentation section 6)
# ============================================================================

SCHEMA_SQL = """
-- ============================================
-- Drop existing tables (for clean reset)
-- ============================================
REMOVE TABLE IF EXISTS pod;
REMOVE TABLE IF EXISTS node;
REMOVE TABLE IF EXISTS service;
REMOVE TABLE IF EXISTS deployment;
REMOVE TABLE IF EXISTS replicaset;
REMOVE TABLE IF EXISTS namespace;
REMOVE TABLE IF EXISTS cluster;
REMOVE TABLE IF EXISTS biz;
REMOVE TABLE IF EXISTS metric;

REMOVE TABLE IF EXISTS node_with_pod;
REMOVE TABLE IF EXISTS pod_with_service;
REMOVE TABLE IF EXISTS deployment_with_replicaset;
REMOVE TABLE IF EXISTS pod_with_replicaset;
REMOVE TABLE IF EXISTS pod_to_pod;
REMOVE TABLE IF EXISTS relation_has_metric;

-- ============================================
-- Node Tables (per documentation section 2)
-- ============================================

-- Pod node table
DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD created_at ON pod TYPE datetime;
DEFINE FIELD updated_at ON pod TYPE datetime;
DEFINE INDEX idx_pod_key ON pod FIELDS bcs_cluster_id, namespace, pod UNIQUE;

-- Node node table
DEFINE TABLE node SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON node TYPE string;
DEFINE FIELD node ON node TYPE string;
DEFINE FIELD created_at ON node TYPE datetime;
DEFINE FIELD updated_at ON node TYPE datetime;
DEFINE INDEX idx_node_key ON node FIELDS bcs_cluster_id, node UNIQUE;

-- Service node table
DEFINE TABLE service SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON service TYPE string;
DEFINE FIELD namespace ON service TYPE string;
DEFINE FIELD service ON service TYPE string;
DEFINE FIELD created_at ON service TYPE datetime;
DEFINE FIELD updated_at ON service TYPE datetime;
DEFINE INDEX idx_service_key ON service FIELDS bcs_cluster_id, namespace, service UNIQUE;

-- Deployment node table
DEFINE TABLE deployment SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON deployment TYPE string;
DEFINE FIELD namespace ON deployment TYPE string;
DEFINE FIELD deployment ON deployment TYPE string;
DEFINE FIELD created_at ON deployment TYPE datetime;
DEFINE FIELD updated_at ON deployment TYPE datetime;
DEFINE INDEX idx_deployment_key ON deployment FIELDS bcs_cluster_id, namespace, deployment UNIQUE;

-- ReplicaSet node table
DEFINE TABLE replicaset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON replicaset TYPE string;
DEFINE FIELD namespace ON replicaset TYPE string;
DEFINE FIELD replicaset ON replicaset TYPE string;
DEFINE FIELD created_at ON replicaset TYPE datetime;
DEFINE FIELD updated_at ON replicaset TYPE datetime;
DEFINE INDEX idx_replicaset_key ON replicaset FIELDS bcs_cluster_id, namespace, replicaset UNIQUE;

-- Namespace node table
DEFINE TABLE namespace SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON namespace TYPE string;
DEFINE FIELD namespace ON namespace TYPE string;
DEFINE FIELD created_at ON namespace TYPE datetime;
DEFINE FIELD updated_at ON namespace TYPE datetime;
DEFINE INDEX idx_namespace_key ON namespace FIELDS bcs_cluster_id, namespace UNIQUE;

-- Cluster node table
DEFINE TABLE cluster SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON cluster TYPE string;
DEFINE FIELD created_at ON cluster TYPE datetime;
DEFINE FIELD updated_at ON cluster TYPE datetime;
DEFINE INDEX idx_cluster_key ON cluster FIELDS bcs_cluster_id UNIQUE;

-- Biz node table
DEFINE TABLE biz SCHEMAFULL;
DEFINE FIELD bk_biz_id ON biz TYPE string;
DEFINE FIELD created_at ON biz TYPE datetime;
DEFINE FIELD updated_at ON biz TYPE datetime;
DEFINE INDEX idx_biz_key ON biz FIELDS bk_biz_id UNIQUE;

-- Metric node table
DEFINE TABLE metric SCHEMAFULL;
DEFINE FIELD metric_name ON metric TYPE string;
DEFINE FIELD metric_type ON metric TYPE string;
DEFINE FIELD unit ON metric TYPE string;
DEFINE FIELD description ON metric TYPE string;
DEFINE FIELD created_at ON metric TYPE datetime;
DEFINE FIELD updated_at ON metric TYPE datetime;
DEFINE INDEX idx_metric_key ON metric FIELDS metric_name UNIQUE;

-- ============================================
-- Relation Tables (TYPE RELATION for graph traversal)
-- Per documentation section 6.2
-- ============================================

-- Static relation: node <-> pod (bidirectional)
DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION IN node OUT pod;
DEFINE FIELD created_at ON node_with_pod TYPE datetime;
DEFINE FIELD updated_at ON node_with_pod TYPE datetime;

-- Static relation: pod <-> service (bidirectional)
DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION IN pod OUT service;
DEFINE FIELD created_at ON pod_with_service TYPE datetime;
DEFINE FIELD updated_at ON pod_with_service TYPE datetime;

-- Static relation: deployment <-> replicaset (bidirectional)
DEFINE TABLE deployment_with_replicaset SCHEMAFULL TYPE RELATION IN deployment OUT replicaset;
DEFINE FIELD created_at ON deployment_with_replicaset TYPE datetime;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE datetime;

-- Static relation: pod <-> replicaset (bidirectional)
DEFINE TABLE pod_with_replicaset SCHEMAFULL TYPE RELATION IN pod OUT replicaset;
DEFINE FIELD created_at ON pod_with_replicaset TYPE datetime;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE datetime;

-- Dynamic relation: pod -> pod (directional)
DEFINE TABLE pod_to_pod SCHEMAFULL TYPE RELATION IN pod OUT pod;
DEFINE FIELD created_at ON pod_to_pod TYPE datetime;
DEFINE FIELD updated_at ON pod_to_pod TYPE datetime;

-- Metric relation: relation -> metric
DEFINE TABLE relation_has_metric SCHEMAFULL TYPE RELATION IN pod_to_pod OUT metric;
DEFINE FIELD result_table_id ON relation_has_metric TYPE string;
DEFINE FIELD created_at ON relation_has_metric TYPE datetime;
DEFINE FIELD updated_at ON relation_has_metric TYPE datetime;
"""


# ============================================================================
# Storage Client
# ============================================================================

class SurrealDBClient:
    """SurrealDB HTTP REST API client"""

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
        for i, result in enumerate(results):
            if result.get('status') == 'ERR':
                error_detail = result.get('detail') or result.get('result', 'Unknown error')
                raise Exception(f"SQL error in statement {i}: {error_detail}")

        return results[1:] if len(results) > 1 else results

    def format_datetime(self, dt: datetime) -> str:
        """Format datetime for SurrealDB"""
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def init_schema(self):
        """Initialize database schema"""
        logger.info("Initializing database schema...")
        
        # Execute schema SQL line by line to handle errors better
        statements = [s.strip() for s in SCHEMA_SQL.split(';') if s.strip()]
        
        for stmt in statements:
            if not stmt or stmt.startswith('--'):
                continue
            try:
                self.execute_sql(stmt + ';')
            except Exception as e:
                logger.warning(f"Schema statement warning: {e}")
        
        logger.info("✓ Database schema initialized")

    def upsert_node(
            self,
            resource_type: ResourceType,
            data: Dict[str, Any],
            created_at: datetime,
            updated_at: datetime
    ) -> Dict[str, Any]:
        """Upsert a node using document-defined ID format"""
        node_id = IDGenerator.generate_node_id(resource_type, data)
        
        # Build SET clause
        set_parts = []
        for key, value in data.items():
            if isinstance(value, (int, float)):
                set_parts.append(f"{key} = {value}")
            else:
                set_parts.append(f"{key} = '{value}'")
        
        # Add timestamps with idempotent logic
        set_parts.append(f"created_at = created_at OR type::datetime('{self.format_datetime(created_at)}')")
        set_parts.append(f"updated_at = type::datetime('{self.format_datetime(updated_at)}')")
        
        set_clause = ', '.join(set_parts)
        
        # Use record ID in backticks to handle special characters
        sql = f"UPSERT {resource_type.value}:`{node_id}` SET {set_clause};"
        
        result = self.execute_sql(sql)
        return result[0].get('result', []) if result else []

    def batch_upsert_nodes(
            self,
            resource_type: ResourceType,
            nodes: List[Dict[str, Any]],
            created_at: datetime,
            updated_at: datetime
    ) -> Dict[str, Any]:
        """Batch upsert nodes"""
        if not nodes:
            return {}

        logger.debug(f"Batch upserting {len(nodes)} {resource_type.value} nodes")

        statements = []
        for data in nodes:
            node_id = IDGenerator.generate_node_id(resource_type, data)
            
            set_parts = []
            for key, value in data.items():
                if isinstance(value, (int, float)):
                    set_parts.append(f"{key} = {value}")
                else:
                    set_parts.append(f"{key} = '{value}'")
            
            set_parts.append(f"created_at = created_at OR type::datetime('{self.format_datetime(created_at)}')")
            set_parts.append(f"updated_at = type::datetime('{self.format_datetime(updated_at)}')")
            
            set_clause = ', '.join(set_parts)
            statements.append(f"UPSERT {resource_type.value}:`{node_id}` SET {set_clause};")

        sql = "BEGIN TRANSACTION; " + " ".join(statements) + " COMMIT TRANSACTION;"
        results = self.execute_sql(sql)
        logger.info(f"✓ Batch upserted {len(nodes)} {resource_type.value} nodes")
        return results

    def upsert_static_relation(
            self,
            relation_type: RelationType,
            from_type: ResourceType,
            from_data: Dict[str, Any],
            to_type: ResourceType,
            to_data: Dict[str, Any],
            created_at: datetime,
            updated_at: datetime
    ) -> Dict[str, Any]:
        """
        Upsert a static (bidirectional) relation using RELATE with document-defined ID
        """
        from_id = IDGenerator.generate_node_id(from_type, from_data)
        to_id = IDGenerator.generate_node_id(to_type, to_data)
        relation_id = IDGenerator.generate_static_relation_id(
            relation_type, from_type, from_data, to_type, to_data
        )

        sql = f"""
        RELATE {from_type.value}:`{from_id}`->{relation_type.value}:`{relation_id}`->{to_type.value}:`{to_id}` SET
            created_at = created_at OR type::datetime('{self.format_datetime(created_at)}'),
            updated_at = type::datetime('{self.format_datetime(updated_at)}');
        """

        result = self.execute_sql(sql)
        return result[0].get('result', []) if result else []

    def upsert_dynamic_relation(
            self,
            relation_type: RelationType,
            source_type: ResourceType,
            source_data: Dict[str, Any],
            target_type: ResourceType,
            target_data: Dict[str, Any],
            created_at: datetime,
            updated_at: datetime
    ) -> Dict[str, Any]:
        """
        Upsert a dynamic (directional) relation using RELATE with document-defined ID
        """
        source_id = IDGenerator.generate_node_id(source_type, source_data)
        target_id = IDGenerator.generate_node_id(target_type, target_data)
        relation_id = IDGenerator.generate_dynamic_relation_id(
            relation_type, source_type, source_data, target_type, target_data
        )

        sql = f"""
        RELATE {source_type.value}:`{source_id}`->{relation_type.value}:`{relation_id}`->{target_type.value}:`{target_id}` SET
            created_at = created_at OR type::datetime('{self.format_datetime(created_at)}'),
            updated_at = type::datetime('{self.format_datetime(updated_at)}');
        """

        result = self.execute_sql(sql)
        return result[0].get('result', []) if result else []


# ============================================================================
# Mock Data Generator
# ============================================================================

class MockGenerator:
    """Generate mock data"""

    def __init__(self, client: SurrealDBClient):
        self.client = client
        self.resources: Dict[ResourceType, List[Dict[str, Any]]] = {}
        self.current_time = MockConfig.END_TIME
        self.traffic_relations: List[Tuple[Dict, Dict, str]] = []  # (source, target, relation_id)

    def random_time_in_range(self) -> datetime:
        """Generate random time within configured range"""
        delta = MockConfig.END_TIME - MockConfig.START_TIME
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return MockConfig.START_TIME + timedelta(seconds=random_seconds)

    def create_biz(self):
        """Create business node"""
        data = {"bk_biz_id": MockConfig.BIZ_ID}
        self.client.upsert_node(ResourceType.BIZ, data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.BIZ] = [data]
        logger.info(f"✓ Created biz: {MockConfig.BIZ_NAME} (id={MockConfig.BIZ_ID})")

    def create_cluster(self):
        """Create cluster node"""
        data = {"bcs_cluster_id": MockConfig.CLUSTER_ID}
        self.client.upsert_node(ResourceType.CLUSTER, data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.CLUSTER] = [data]
        logger.info(f"✓ Created cluster: {MockConfig.CLUSTER_ID}")

    def create_namespace(self):
        """Create namespace node"""
        data = {"bcs_cluster_id": MockConfig.CLUSTER_ID, "namespace": MockConfig.NAMESPACE}
        self.client.upsert_node(ResourceType.NAMESPACE, data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.NAMESPACE] = [data]
        logger.info(f"✓ Created namespace: {MockConfig.NAMESPACE}")

    def create_nodes(self):
        """Create node resources"""
        logger.info(f"Creating {MockConfig.NUM_NODES} nodes...")
        nodes = []
        for i in range(MockConfig.NUM_NODES):
            nodes.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "node": f"{MockConfig.BIZ_NAME}-node-{i}"
            })
        self.client.batch_upsert_nodes(ResourceType.NODE, nodes, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.NODE] = nodes

    def create_pods(self):
        """Create pod resources"""
        logger.info(f"Creating {MockConfig.NUM_PODS} pods...")
        pods = []
        for i in range(MockConfig.NUM_PODS):
            pods.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "pod": f"{MockConfig.BIZ_NAME}-pod-{i:03d}"
            })
        self.client.batch_upsert_nodes(ResourceType.POD, pods, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.POD] = pods

    def create_services(self):
        """Create service resources"""
        logger.info(f"Creating {len(MockConfig.SERVICE_LIST)} services...")
        services = []
        for svc_name in MockConfig.SERVICE_LIST:
            services.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "service": f"{MockConfig.BIZ_NAME}-{svc_name}"
            })
        self.client.batch_upsert_nodes(ResourceType.SERVICE, services, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.SERVICE] = services

    def create_deployments(self):
        """Create deployment resources"""
        logger.info(f"Creating {MockConfig.NUM_DEPLOYMENTS} deployments...")
        deployments = []
        for i in range(MockConfig.NUM_DEPLOYMENTS):
            deployments.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "deployment": f"{MockConfig.BIZ_NAME}-{MockConfig.SERVICE_LIST[i]}-deploy"
            })
        self.client.batch_upsert_nodes(ResourceType.DEPLOYMENT, deployments, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.DEPLOYMENT] = deployments

    def create_static_relations(self):
        """Create static relations"""
        logger.info("Creating static relations...")
        
        # Deployment -> ReplicaSet -> Pod chain
        self._create_deployment_chain()
        
        # Node with Pod
        self._create_node_with_pod()
        
        # Pod with Service
        self._create_pod_with_service()

    def _create_deployment_chain(self):
        """Create Deployment -> ReplicaSet -> Pod chain"""
        logger.info("  Creating deployment chain relations...")
        
        deployments = self.resources.get(ResourceType.DEPLOYMENT, [])
        pods = self.resources.get(ResourceType.POD, [])
        
        if not deployments:
            return
        
        replicasets = []
        pods_per_deployment = len(pods) // len(deployments)
        
        for i, deploy in enumerate(deployments):
            # Create ReplicaSet
            rs_data = {
                "bcs_cluster_id": deploy["bcs_cluster_id"],
                "namespace": deploy["namespace"],
                "replicaset": f"{deploy['deployment']}-rs-001"
            }
            self.client.upsert_node(ResourceType.REPLICASET, rs_data, self.random_time_in_range(), self.current_time)
            replicasets.append(rs_data)
            
            # Deployment -> ReplicaSet
            self.client.upsert_static_relation(
                RelationType.DEPLOYMENT_WITH_REPLICASET,
                ResourceType.DEPLOYMENT, deploy,
                ResourceType.REPLICASET, rs_data,
                self.random_time_in_range(), self.current_time
            )
            
            # Pod -> ReplicaSet
            start_idx = i * pods_per_deployment
            end_idx = start_idx + pods_per_deployment if i < len(deployments) - 1 else len(pods)
            for pod in pods[start_idx:end_idx]:
                self.client.upsert_static_relation(
                    RelationType.POD_WITH_REPLICASET,
                    ResourceType.POD, pod,
                    ResourceType.REPLICASET, rs_data,
                    self.random_time_in_range(), self.current_time
                )
        
        self.resources[ResourceType.REPLICASET] = replicasets
        logger.info(f"  ✓ Created {len(replicasets)} replicasets and relations")

    def _create_node_with_pod(self):
        """Create node_with_pod relations"""
        nodes = self.resources.get(ResourceType.NODE, [])
        pods = self.resources.get(ResourceType.POD, [])
        
        count = 0
        for i, pod in enumerate(pods):
            node = nodes[i % len(nodes)]
            self.client.upsert_static_relation(
                RelationType.NODE_WITH_POD,
                ResourceType.NODE, node,
                ResourceType.POD, pod,
                self.random_time_in_range(), self.current_time
            )
            count += 1
        logger.info(f"  ✓ Created {count} node_with_pod relations")

    def _create_pod_with_service(self):
        """Create pod_with_service relations"""
        services = self.resources.get(ResourceType.SERVICE, [])
        pods = self.resources.get(ResourceType.POD, [])
        
        count = 0
        pods_per_service = len(pods) // len(services)
        
        for i, service in enumerate(services):
            start_idx = i * pods_per_service
            end_idx = start_idx + pods_per_service if i < len(services) - 1 else len(pods)
            
            for pod in pods[start_idx:end_idx]:
                self.client.upsert_static_relation(
                    RelationType.POD_WITH_SERVICE,
                    ResourceType.POD, pod,
                    ResourceType.SERVICE, service,
                    self.random_time_in_range(), self.current_time
                )
                count += 1
        logger.info(f"  ✓ Created {count} pod_with_service relations")

    def create_dynamic_relations(self):
        """Create dynamic pod_to_pod traffic relations"""
        logger.info("Creating pod_to_pod traffic relations...")
        
        pods = self.resources.get(ResourceType.POD, [])
        
        count = 0
        for source_pod in pods:
            if random.random() < MockConfig.POD_TO_POD_TRAFFIC_PROBABILITY:
                target_candidates = [p for p in pods if p != source_pod]
                if target_candidates:
                    target_pod = random.choice(target_candidates)
                    
                    self.client.upsert_dynamic_relation(
                        RelationType.POD_TO_POD,
                        ResourceType.POD, source_pod,
                        ResourceType.POD, target_pod,
                        self.random_time_in_range(), self.current_time
                    )
                    
                    relation_id = IDGenerator.generate_dynamic_relation_id(
                        RelationType.POD_TO_POD,
                        ResourceType.POD, source_pod,
                        ResourceType.POD, target_pod
                    )
                    self.traffic_relations.append((source_pod, target_pod, relation_id))
                    count += 1
        
        logger.info(f"✓ Created {count} pod_to_pod traffic relations")

    def create_metrics_metadata(self):
        """Create metric nodes"""
        logger.info("Creating metric metadata...")
        
        metrics = [
            {"metric_name": "pod_to_pod_flow_total", "metric_type": "counter", "unit": "count", "description": "Pod到Pod的流量访问量"},
            {"metric_name": "pod_to_pod_flow_seconds", "metric_type": "gauge", "unit": "seconds", "description": "Pod到Pod的流量访问耗时"},
            {"metric_name": "pod_to_pod_flow_error", "metric_type": "counter", "unit": "count", "description": "Pod到Pod的流量错误数"},
        ]
        
        for metric_data in metrics:
            self.client.upsert_node(ResourceType.METRIC, metric_data, self.random_time_in_range(), self.current_time)
        
        self.resources[ResourceType.METRIC] = metrics
        logger.info(f"✓ Created {len(metrics)} metric definitions")

    def create_relation_has_metric(self):
        """Create relation_has_metric associations"""
        logger.info("Creating relation_has_metric associations...")
        
        metrics = self.resources.get(ResourceType.METRIC, [])
        
        count = 0
        for source_pod, target_pod, relation_id in self.traffic_relations:
            for metric_data in metrics:
                metric_id = IDGenerator.generate_node_id(ResourceType.METRIC, metric_data)
                result_table_id = f"{MockConfig.RESULT_TABLE_ID}_{metric_data['metric_name']}"
                
                sql = f"""
                RELATE pod_to_pod:`{relation_id}`->relation_has_metric->metric:`{metric_id}` SET
                    result_table_id = '{result_table_id}',
                    created_at = type::datetime('{self.client.format_datetime(self.current_time)}'),
                    updated_at = type::datetime('{self.client.format_datetime(self.current_time)}');
                """
                
                try:
                    self.client.execute_sql(sql)
                    count += 1
                except Exception as e:
                    logger.warning(f"  ⚠ Failed to create relation_has_metric: {e}")
        
        logger.info(f"✓ Created {count} relation_has_metric associations")

    def generate_all(self):
        """Generate all mock data"""
        logger.info("\n" + "=" * 70)
        logger.info("Starting BKOP Business 2 Mock Data Generation")
        logger.info("=" * 70 + "\n")
        
        self.create_biz()
        self.create_cluster()
        self.create_namespace()
        self.create_nodes()
        self.create_pods()
        self.create_services()
        self.create_deployments()
        
        self.create_static_relations()
        self.create_dynamic_relations()
        
        self.create_metrics_metadata()
        self.create_relation_has_metric()
        
        logger.info("\n" + "=" * 70)
        logger.info("Mock Data Generation Completed!")
        logger.info("=" * 70)
        self._print_summary()

    def _print_summary(self):
        """Print generation summary"""
        logger.info("\n📊 Summary:")
        logger.info("-" * 70)
        logger.info(f"  Business ID: {MockConfig.BIZ_ID} ({MockConfig.BIZ_NAME})")
        logger.info(f"  Cluster: {MockConfig.CLUSTER_ID}")
        logger.info(f"  Namespace: {MockConfig.NAMESPACE}")
        logger.info("-" * 70)
        for resource_type, items in self.resources.items():
            logger.info(f"  {resource_type.value:20s}: {len(items):5d} items")
        logger.info(f"  {'traffic_relations':20s}: {len(self.traffic_relations):5d} items")
        logger.info("-" * 70)


# ============================================================================
# Main Function
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Mock BKOP Business 2 Traffic to SurrealDB')
    parser.add_argument('--backend', type=str, default='native', choices=['native', 'bkbase'])
    parser.add_argument('--init-schema', action='store_true', help='Initialize database schema (drops existing tables)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=" * 70)
    logger.info(" Mock BKOP Business 2 Traffic to SurrealDB")
    logger.info("=" * 70)
    logger.info(f"\nConfiguration:")
    logger.info(f"  Storage Backend: {args.backend}")
    logger.info(f"  SurrealDB URL: {SurrealDBConfig.URL}")
    logger.info(f"  Namespace: {SurrealDBConfig.NAMESPACE}")
    logger.info(f"  Database: {SurrealDBConfig.DATABASE}")
    logger.info(f"  Init Schema: {args.init_schema}")
    logger.info("")

    try:
        client = SurrealDBClient()
        
        # Test connection
        logger.info("Testing connection...")
        client.execute_sql("INFO FOR DB;")
        logger.info("✓ Connection successful!\n")
        
        # Initialize schema if requested
        if args.init_schema:
            client.init_schema()
        
        # Generate data
        generator = MockGenerator(client)
        generator.generate_all()
        
        logger.info("\n✓ Done!")
        return 0

    except Exception as e:
        logger.error(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
