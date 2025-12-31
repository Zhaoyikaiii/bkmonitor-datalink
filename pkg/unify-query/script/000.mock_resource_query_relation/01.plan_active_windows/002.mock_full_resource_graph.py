#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mock Full Resource Graph to SurrealDB - Plan 01: Active Windows

This script generates mock resource association data for all resource types.
Unlike Plan 00 (created_at), this plan uses:
- Simple UPSERT MERGE syntax for writes
- SurrealDB Events to automatically manage active_windows lifecycle
- No explicit fn::upsert_{resource_type} function calls needed

Key Differences from Plan 00:
- Plan 00: Uses fn::upsert_pod/fn::upsert_service functions with created_at lifecycle
- Plan 01: Uses simple UPSERT MERGE, Event automatically manages active_windows

Write Pattern:
    UPSERT pod:⟨bcs_cluster_id=X,namespace=N,pod=P⟩ MERGE {
        bcs_cluster_id: "X",
        namespace: "N", 
        pod: "P",
        updated_at: <timestamp_ms>
    };

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
    TOLERANCE_TIME_MS = _config.get('mock', {}).get('tolerance_time_ms', 600000)  # 10 minutes default


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
# Storage Client - Active Windows Version
# ============================================================================

class SurrealDBClient:
    """
    SurrealDB HTTP REST API client for Active Windows schema.
    
    Key difference from Plan 00:
    - Uses simple UPSERT MERGE syntax
    - Events automatically manage active_windows lifecycle
    - No need to call fn::upsert_{resource_type} functions
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

    def _escape_value(self, v: Any) -> str:
        """Escape value for SurrealDB"""
        if isinstance(v, (int, float)):
            return str(v)
        else:
            v_escaped = str(v).replace("'", "\\'")
            return f"'{v_escaped}'"

    def _build_record_id(self, table: str, dimensions: Dict[str, Any]) -> str:
        """Build deterministic record ID using SurrealDB's object-based ID format."""
        sorted_items = sorted(dimensions.items())
        kv_parts = [f"{k}={v}" for k, v in sorted_items]
        kv_str = ",".join(kv_parts)
        return f"{table}:⟨{kv_str}⟩"

    def init_schema(self, tolerance_time_ms: int = None):
        """Initialize database schema from schema.sql file."""
        logger.info("Initializing database schema...")
        
        schema_sql = load_schema_sql(tolerance_time_ms)
        
        # Split schema into individual statements and execute separately to avoid transaction conflicts
        statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
        
        for i, stmt in enumerate(statements):
            try:
                self.execute_sql(stmt + ';')
                logger.debug(f"Executed schema statement {i+1}/{len(statements)}")
            except Exception as e:
                if 'already exists' in str(e).lower():
                    logger.debug(f"Statement {i+1} skipped (already exists)")
                    continue
                # Retry once on transaction conflict
                if 'transaction' in str(e).lower() or 'conflict' in str(e).lower():
                    import time
                    time.sleep(0.1)
                    try:
                        self.execute_sql(stmt + ';')
                        logger.debug(f"Executed schema statement {i+1}/{len(statements)} (retry)")
                        continue
                    except Exception as e2:
                        if 'already exists' in str(e2).lower():
                            continue
                        logger.warning(f"Statement {i+1} failed after retry: {e2}")
                        continue
                logger.warning(f"Statement {i+1} failed: {e}")
                continue
        
        logger.info("Database schema initialized successfully")

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
        
        record_id = self._build_record_id(table, dimensions)
        
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
            logger.warning(f"  upsert {table} failed: {e}")
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
        Upsert a relation using fn::upsert_relation function.
        
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
        
        from_id = self._build_record_id(from_table, from_dimensions)
        to_id = self._build_record_id(to_table, to_dimensions)
        
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
            logger.warning(f"  upsert relation {relation_table} failed: {e}")
        return {}

    def upsert_node(
            self,
            resource_type: ResourceType,
            data: Dict[str, Any],
            updated_at: datetime
    ) -> Tuple[str, int, str]:
        """
        Upsert a node using simple UPSERT MERGE.
        
        Returns:
            Tuple of (action, updated_at_ms, node_id)
        """
        current_time_ms = self.datetime_to_ms(updated_at)
        
        result = self.upsert_resource(resource_type.value, data, current_time_ms)
        
        node_id = str(result.get('id', ''))
        action = 'upsert'
        
        return (action, current_time_ms, node_id)

    def batch_upsert_nodes(
            self,
            resource_type: ResourceType,
            nodes: List[Dict[str, Any]],
            updated_at: datetime
    ) -> List[Tuple[str, int, str]]:
        """Batch upsert nodes."""
        if not nodes:
            return []

        results = []
        current_time_ms = self.datetime_to_ms(updated_at)
        
        # Build batch SQL
        sql_parts = []
        for dimensions in nodes:
            record_id = self._build_record_id(resource_type.value, dimensions)
            set_parts = [f"{k}: {self._escape_value(v)}" for k, v in dimensions.items()]
            set_parts.append(f"updated_at: {current_time_ms}")
            set_clause = ", ".join(set_parts)
            sql_parts.append(f"UPSERT {record_id} MERGE {{ {set_clause} }};")
        
        # Execute in batches
        batch_size = 100
        for i in range(0, len(sql_parts), batch_size):
            batch_sql = "\n".join(sql_parts[i:i+batch_size])
            try:
                self.execute_sql(batch_sql)
            except Exception as e:
                logger.warning(f"  batch upsert {resource_type.value} failed (batch {i//batch_size}): {e}")
        
        logger.info(f"  Processed {len(nodes)} {resource_type.value} nodes")
        return [(f'upsert', current_time_ms, '') for _ in nodes]

    def upsert_static_relation(
            self,
            relation_type: RelationType,
            from_type: ResourceType,
            from_data: Dict[str, Any],
            to_type: ResourceType,
            to_data: Dict[str, Any],
            updated_at: datetime
    ) -> Dict[str, Any]:
        """Upsert a static (bidirectional) relation."""
        current_time_ms = self.datetime_to_ms(updated_at)
        
        # Step 1: Upsert both endpoint resources
        self.upsert_resource(from_type.value, from_data, current_time_ms)
        self.upsert_resource(to_type.value, to_data, current_time_ms)
        
        # Step 2: Upsert the relation
        return self.upsert_relation(
            relation_type.value,
            from_type.value, from_data,
            to_type.value, to_data,
            current_time_ms
        )

    def upsert_dynamic_relation(
            self,
            relation_type: RelationType,
            source_type: ResourceType,
            source_data: Dict[str, Any],
            target_type: ResourceType,
            target_data: Dict[str, Any],
            updated_at: datetime
    ) -> Dict[str, Any]:
        """Upsert a dynamic (directional) relation."""
        current_time_ms = self.datetime_to_ms(updated_at)
        
        # Step 1: Upsert both endpoint resources
        self.upsert_resource(source_type.value, source_data, current_time_ms)
        self.upsert_resource(target_type.value, target_data, current_time_ms)
        
        # Step 2: Upsert the relation
        return self.upsert_relation(
            relation_type.value,
            source_type.value, source_data,
            target_type.value, target_data,
            current_time_ms
        )


# ============================================================================
# Mock Data Generator
# ============================================================================

class FullMockGenerator:
    """Generate complete mock data for all resource types"""

    def __init__(self, client: SurrealDBClient):
        self.client = client
        self.resources: Dict[ResourceType, List[Dict[str, Any]]] = {}
        self.current_time = datetime.utcnow().replace(tzinfo=None)
        self.current_time_ms = self.client.datetime_to_ms(self.current_time)
        self.traffic_relations: List[Tuple[Dict, Dict, RelationType]] = []

    # =========================================================================
    # CMDB Resources
    # =========================================================================
    
    def create_cmdb_resources(self):
        """Create CMDB resources: Biz, Set, Module, Host"""
        logger.info("Creating CMDB resources...")
        
        biz_data = {"bk_biz_id": MockConfig.BIZ_ID}
        self.client.upsert_node(ResourceType.BIZ, biz_data, self.current_time)
        self.resources[ResourceType.BIZ] = [biz_data]
        
        set_data = {"bk_set_id": MockConfig.SET_ID}
        self.client.upsert_node(ResourceType.SET, set_data, self.current_time)
        self.resources[ResourceType.SET] = [set_data]
        
        module_data = {"bk_module_id": MockConfig.MODULE_ID}
        self.client.upsert_node(ResourceType.MODULE, module_data, self.current_time)
        self.resources[ResourceType.MODULE] = [module_data]
        
        host_data = {"bk_host_id": MockConfig.HOST_ID}
        self.client.upsert_node(ResourceType.HOST, host_data, self.current_time)
        self.resources[ResourceType.HOST] = [host_data]
        
        logger.info("  Created Biz, Set, Module, Host")

    def create_cmdb_relations(self):
        """Create CMDB relations"""
        logger.info("Creating CMDB relations...")
        
        biz_data = self.resources[ResourceType.BIZ][0]
        set_data = self.resources[ResourceType.SET][0]
        module_data = self.resources[ResourceType.MODULE][0]
        host_data = self.resources[ResourceType.HOST][0]
        system_list = self.resources.get(ResourceType.SYSTEM, [])
        system_data = system_list[0] if system_list else {}
        
        self.client.upsert_static_relation(
            RelationType.BIZ_WITH_SET,
            ResourceType.BIZ, biz_data,
            ResourceType.SET, set_data,
            self.current_time
        )
        
        self.client.upsert_static_relation(
            RelationType.MODULE_WITH_SET,
            ResourceType.MODULE, module_data,
            ResourceType.SET, set_data,
            self.current_time
        )
        
        self.client.upsert_static_relation(
            RelationType.HOST_WITH_MODULE,
            ResourceType.HOST, host_data,
            ResourceType.MODULE, module_data,
            self.current_time
        )
        
        if system_data:
            self.client.upsert_static_relation(
                RelationType.HOST_WITH_SYSTEM,
                ResourceType.HOST, host_data,
                ResourceType.SYSTEM, system_data,
                self.current_time
            )
        
        logger.info("  Created CMDB relations")

    # =========================================================================
    # Kubernetes Resources
    # =========================================================================
    
    def create_k8s_resources(self):
        """Create Kubernetes resources"""
        logger.info("Creating Kubernetes resources...")
        
        # Cluster
        cluster_data = {"bcs_cluster_id": MockConfig.CLUSTER_ID}
        self.client.upsert_node(ResourceType.CLUSTER, cluster_data, self.current_time)
        self.resources[ResourceType.CLUSTER] = [cluster_data]
        
        # Namespace
        ns_data = {"bcs_cluster_id": MockConfig.CLUSTER_ID, "namespace": MockConfig.NAMESPACE}
        self.client.upsert_node(ResourceType.NAMESPACE, ns_data, self.current_time)
        self.resources[ResourceType.NAMESPACE] = [ns_data]
        
        # Nodes
        nodes = []
        for i in range(MockConfig.NUM_NODES):
            nodes.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "node": f"{MockConfig.BIZ_NAME}-node-{i}"
            })
        self.client.batch_upsert_nodes(ResourceType.NODE, nodes, self.current_time)
        self.resources[ResourceType.NODE] = nodes
        
        # Pods
        pods = []
        for i in range(MockConfig.NUM_PODS):
            pods.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "pod": f"{MockConfig.BIZ_NAME}-pod-{i:03d}"
            })
        self.client.batch_upsert_nodes(ResourceType.POD, pods, self.current_time)
        self.resources[ResourceType.POD] = pods
        
        # Containers
        containers = []
        for i in range(MockConfig.NUM_CONTAINERS):
            pod_idx = i % len(pods)
            containers.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "pod": pods[pod_idx]["pod"],
                "container": f"container-{i:03d}"
            })
        self.client.batch_upsert_nodes(ResourceType.CONTAINER, containers, self.current_time)
        self.resources[ResourceType.CONTAINER] = containers
        
        # Services
        services = []
        for svc_name in MockConfig.SERVICE_LIST:
            services.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "service": f"{MockConfig.BIZ_NAME}-{svc_name}"
            })
        self.client.batch_upsert_nodes(ResourceType.SERVICE, services, self.current_time)
        self.resources[ResourceType.SERVICE] = services
        
        # Deployments
        deployments = []
        for svc_name in MockConfig.SERVICE_LIST:
            deployments.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "deployment": f"{MockConfig.BIZ_NAME}-{svc_name}-deploy"
            })
        self.client.batch_upsert_nodes(ResourceType.DEPLOYMENT, deployments, self.current_time)
        self.resources[ResourceType.DEPLOYMENT] = deployments
        
        # ReplicaSets
        replicasets = []
        for deploy in deployments:
            replicasets.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "replicaset": f"{deploy['deployment']}-rs-001"
            })
        self.client.batch_upsert_nodes(ResourceType.REPLICASET, replicasets, self.current_time)
        self.resources[ResourceType.REPLICASET] = replicasets
        
        # StatefulSet
        statefulset_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "statefulset": f"{MockConfig.BIZ_NAME}-statefulset"
        }
        self.client.upsert_node(ResourceType.STATEFULSET, statefulset_data, self.current_time)
        self.resources[ResourceType.STATEFULSET] = [statefulset_data]
        
        # DaemonSet
        daemonset_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "daemonset": f"{MockConfig.BIZ_NAME}-daemonset"
        }
        self.client.upsert_node(ResourceType.DAEMONSET, daemonset_data, self.current_time)
        self.resources[ResourceType.DAEMONSET] = [daemonset_data]
        
        # Job
        job_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "job": f"{MockConfig.BIZ_NAME}-job"
        }
        self.client.upsert_node(ResourceType.JOB, job_data, self.current_time)
        self.resources[ResourceType.JOB] = [job_data]
        
        # Ingress
        ingress_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "ingress": f"{MockConfig.BIZ_NAME}-ingress"
        }
        self.client.upsert_node(ResourceType.INGRESS, ingress_data, self.current_time)
        self.resources[ResourceType.INGRESS] = [ingress_data]
        
        logger.info("  Created all Kubernetes resources")

    def create_k8s_relations(self):
        """Create Kubernetes static relations"""
        logger.info("Creating Kubernetes relations...")
        
        pods = self.resources.get(ResourceType.POD, [])
        nodes = self.resources.get(ResourceType.NODE, [])
        services = self.resources.get(ResourceType.SERVICE, [])
        deployments = self.resources.get(ResourceType.DEPLOYMENT, [])
        replicasets = self.resources.get(ResourceType.REPLICASET, [])
        containers = self.resources.get(ResourceType.CONTAINER, [])
        systems = self.resources.get(ResourceType.SYSTEM, [])
        
        # node_with_pod
        for i, pod_data in enumerate(pods):
            node_data = nodes[i % len(nodes)]
            self.client.upsert_static_relation(
                RelationType.NODE_WITH_POD,
                ResourceType.NODE, node_data,
                ResourceType.POD, pod_data,
                self.current_time
            )
        
        # node_with_system
        for node_data in nodes:
            if systems:
                system_data = systems[0]
                self.client.upsert_static_relation(
                    RelationType.NODE_WITH_SYSTEM,
                    ResourceType.NODE, node_data,
                    ResourceType.SYSTEM, system_data,
                    self.current_time
                )
        
        # pod_with_service
        pods_per_service = len(pods) // len(services) if services else 0
        for i, service_data in enumerate(services):
            start_idx = i * pods_per_service
            end_idx = start_idx + pods_per_service if i < len(services) - 1 else len(pods)
            for pod_data in pods[start_idx:end_idx]:
                self.client.upsert_static_relation(
                    RelationType.POD_WITH_SERVICE,
                    ResourceType.POD, pod_data,
                    ResourceType.SERVICE, service_data,
                    self.current_time
                )
        
        # deployment_with_replicaset and pod_with_replicaset
        pods_per_rs = len(pods) // len(replicasets) if replicasets else 0
        for i, (deploy_data, rs_data) in enumerate(zip(deployments, replicasets)):
            self.client.upsert_static_relation(
                RelationType.DEPLOYMENT_WITH_REPLICASET,
                ResourceType.DEPLOYMENT, deploy_data,
                ResourceType.REPLICASET, rs_data,
                self.current_time
            )
            
            start_idx = i * pods_per_rs
            end_idx = start_idx + pods_per_rs if i < len(replicasets) - 1 else len(pods)
            for pod_data in pods[start_idx:end_idx]:
                self.client.upsert_static_relation(
                    RelationType.POD_WITH_REPLICASET,
                    ResourceType.POD, pod_data,
                    ResourceType.REPLICASET, rs_data,
                    self.current_time
                )
        
        # container_with_pod
        for container_data in containers:
            matching_pod = None
            for pod_data in pods:
                if (pod_data["bcs_cluster_id"] == container_data["bcs_cluster_id"] and
                    pod_data["namespace"] == container_data["namespace"] and
                    pod_data["pod"] == container_data["pod"]):
                    matching_pod = pod_data
                    break
            
            if matching_pod:
                self.client.upsert_static_relation(
                    RelationType.CONTAINER_WITH_POD,
                    ResourceType.CONTAINER, container_data,
                    ResourceType.POD, matching_pod,
                    self.current_time
                )
        
        # ingress_with_service
        ingress_list = self.resources.get(ResourceType.INGRESS, [])
        if ingress_list and services:
            self.client.upsert_static_relation(
                RelationType.INGRESS_WITH_SERVICE,
                ResourceType.INGRESS, ingress_list[0],
                ResourceType.SERVICE, services[0],
                self.current_time
            )
        
        # job_with_pod
        job_list = self.resources.get(ResourceType.JOB, [])
        if job_list and pods:
            self.client.upsert_static_relation(
                RelationType.JOB_WITH_POD,
                ResourceType.JOB, job_list[0],
                ResourceType.POD, pods[0],
                self.current_time
            )
        
        # pod_with_statefulset
        statefulset_list = self.resources.get(ResourceType.STATEFULSET, [])
        if statefulset_list and pods:
            self.client.upsert_static_relation(
                RelationType.POD_WITH_STATEFULSET,
                ResourceType.POD, pods[0],
                ResourceType.STATEFULSET, statefulset_list[0],
                self.current_time
            )
        
        # daemonset_with_pod
        daemonset_list = self.resources.get(ResourceType.DAEMONSET, [])
        if daemonset_list and pods:
            pod_data = pods[1] if len(pods) > 1 else pods[0]
            self.client.upsert_static_relation(
                RelationType.DAEMONSET_WITH_POD,
                ResourceType.DAEMONSET, daemonset_list[0],
                ResourceType.POD, pod_data,
                self.current_time
            )
        
        logger.info("  Created all Kubernetes relations")

    # =========================================================================
    # Network Resources
    # =========================================================================
    
    def create_network_resources(self):
        """Create Network resources: System, K8s Address, Domain"""
        logger.info("Creating Network resources...")
        
        systems = []
        for i in range(MockConfig.NUM_SYSTEMS):
            systems.append({
                "bk_cloud_id": MockConfig.CLOUD_ID,
                "bk_target_ip": f"10.0.0.{i+1}"
            })
        self.client.batch_upsert_nodes(ResourceType.SYSTEM, systems, self.current_time)
        self.resources[ResourceType.SYSTEM] = systems
        
        k8s_address_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "address": "10.0.0.100"
        }
        self.client.upsert_node(ResourceType.K8S_ADDRESS, k8s_address_data, self.current_time)
        self.resources[ResourceType.K8S_ADDRESS] = [k8s_address_data]
        
        domain_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "domain": f"{MockConfig.BIZ_NAME}.example.com"
        }
        self.client.upsert_node(ResourceType.DOMAIN, domain_data, self.current_time)
        self.resources[ResourceType.DOMAIN] = [domain_data]
        
        logger.info("  Created Network resources")

    def create_network_relations(self):
        """Create Network relations"""
        logger.info("Creating Network relations...")
        
        services = self.resources.get(ResourceType.SERVICE, [])
        k8s_address_list = self.resources.get(ResourceType.K8S_ADDRESS, [])
        domain_list = self.resources.get(ResourceType.DOMAIN, [])
        
        if k8s_address_list and services:
            self.client.upsert_static_relation(
                RelationType.K8S_ADDRESS_WITH_SERVICE,
                ResourceType.K8S_ADDRESS, k8s_address_list[0],
                ResourceType.SERVICE, services[0],
                self.current_time
            )
        
        if domain_list and services:
            self.client.upsert_static_relation(
                RelationType.DOMAIN_WITH_SERVICE,
                ResourceType.DOMAIN, domain_list[0],
                ResourceType.SERVICE, services[0],
                self.current_time
            )
        
        logger.info("  Created Network relations")

    # =========================================================================
    # APM Resources
    # =========================================================================
    
    def create_apm_resources(self):
        """Create APM resources"""
        logger.info("Creating APM resources...")
        
        apm_service_data = {
            "apm_application_name": MockConfig.APM_APP_NAME,
            "apm_service_name": MockConfig.APM_SERVICE_NAME
        }
        self.client.upsert_node(ResourceType.APM_SERVICE, apm_service_data, self.current_time)
        self.resources[ResourceType.APM_SERVICE] = [apm_service_data]
        
        apm_instances = []
        for i in range(MockConfig.NUM_APM_INSTANCES):
            apm_instances.append({
                "apm_application_name": MockConfig.APM_APP_NAME,
                "apm_service_name": MockConfig.APM_SERVICE_NAME,
                "apm_service_instance_name": f"{MockConfig.APM_SERVICE_NAME}-instance-{i}"
            })
        self.client.batch_upsert_nodes(ResourceType.APM_SERVICE_INSTANCE, apm_instances, self.current_time)
        self.resources[ResourceType.APM_SERVICE_INSTANCE] = apm_instances
        
        logger.info("  Created APM resources")

    def create_apm_relations(self):
        """Create APM relations"""
        logger.info("Creating APM relations...")
        
        apm_service_list = self.resources.get(ResourceType.APM_SERVICE, [])
        apm_instances = self.resources.get(ResourceType.APM_SERVICE_INSTANCE, [])
        pods = self.resources.get(ResourceType.POD, [])
        systems = self.resources.get(ResourceType.SYSTEM, [])
        
        if not apm_service_list:
            logger.info("  No APM service found, skipping APM relations")
            return
        
        apm_service_data = apm_service_list[0]
        
        for i, instance_data in enumerate(apm_instances):
            self.client.upsert_static_relation(
                RelationType.APM_SERVICE_WITH_APM_SERVICE_INSTANCE,
                ResourceType.APM_SERVICE, apm_service_data,
                ResourceType.APM_SERVICE_INSTANCE, instance_data,
                self.current_time
            )
            
            if pods:
                pod_data = pods[i % len(pods)]
                self.client.upsert_static_relation(
                    RelationType.APM_SERVICE_INSTANCE_WITH_POD,
                    ResourceType.APM_SERVICE_INSTANCE, instance_data,
                    ResourceType.POD, pod_data,
                    self.current_time
                )
            
            if systems:
                system_data = systems[i % len(systems)]
                self.client.upsert_static_relation(
                    RelationType.APM_SERVICE_INSTANCE_WITH_SYSTEM,
                    ResourceType.APM_SERVICE_INSTANCE, instance_data,
                    ResourceType.SYSTEM, system_data,
                    self.current_time
                )
        
        logger.info("  Created APM relations")

    # =========================================================================
    # Data Source Resources
    # =========================================================================
    
    def create_datasource_resources(self):
        """Create Data Source resources"""
        logger.info("Creating Data Source resources...")
        
        datasource_data = {"bk_data_id": MockConfig.DATA_ID}
        self.client.upsert_node(ResourceType.DATASOURCE, datasource_data, self.current_time)
        self.resources[ResourceType.DATASOURCE] = [datasource_data]
        
        bklogconfig_data = {
            "bklogconfig_namespace": MockConfig.NAMESPACE,
            "bklogconfig_name": f"{MockConfig.BIZ_NAME}-logconfig"
        }
        self.client.upsert_node(ResourceType.BKLOGCONFIG, bklogconfig_data, self.current_time)
        self.resources[ResourceType.BKLOGCONFIG] = [bklogconfig_data]
        
        logger.info("  Created Data Source resources")

    def create_datasource_relations(self):
        """Create Data Source relations"""
        logger.info("Creating Data Source relations...")
        
        datasource_list = self.resources.get(ResourceType.DATASOURCE, [])
        bklogconfig_list = self.resources.get(ResourceType.BKLOGCONFIG, [])
        pods = self.resources.get(ResourceType.POD, [])
        nodes = self.resources.get(ResourceType.NODE, [])
        
        if not datasource_list:
            logger.info("  No datasource found, skipping datasource relations")
            return
            
        datasource_data = datasource_list[0]
        
        if pods:
            self.client.upsert_static_relation(
                RelationType.DATASOURCE_WITH_POD,
                ResourceType.DATASOURCE, datasource_data,
                ResourceType.POD, pods[0],
                self.current_time
            )
        
        if nodes:
            self.client.upsert_static_relation(
                RelationType.DATASOURCE_WITH_NODE,
                ResourceType.DATASOURCE, datasource_data,
                ResourceType.NODE, nodes[0],
                self.current_time
            )
        
        if bklogconfig_list:
            self.client.upsert_static_relation(
                RelationType.BKLOGCONFIG_WITH_DATASOURCE,
                ResourceType.BKLOGCONFIG, bklogconfig_list[0],
                ResourceType.DATASOURCE, datasource_data,
                self.current_time
            )
        
        logger.info("  Created Data Source relations")

    # =========================================================================
    # App Version Resources
    # =========================================================================
    
    def create_app_version_resources(self):
        """Create App Version resources"""
        logger.info("Creating App Version resources...")
        
        app_version_data = {
            "app_name": MockConfig.APP_NAME,
            "version": MockConfig.VERSION
        }
        self.client.upsert_node(ResourceType.APP_VERSION, app_version_data, self.current_time)
        self.resources[ResourceType.APP_VERSION] = [app_version_data]
        
        git_commit_data = {
            "git_repo": MockConfig.GIT_REPO,
            "commit_id": MockConfig.COMMIT_ID
        }
        self.client.upsert_node(ResourceType.GIT_COMMIT, git_commit_data, self.current_time)
        self.resources[ResourceType.GIT_COMMIT] = [git_commit_data]
        
        env_data = {"environment": MockConfig.ENVIRONMENT}
        self.client.upsert_node(ResourceType.ENVIRONMENT, env_data, self.current_time)
        self.resources[ResourceType.ENVIRONMENT] = [env_data]
        
        logger.info("  Created App Version resources")

    def create_app_version_relations(self):
        """Create App Version relations"""
        logger.info("Creating App Version relations...")
        
        app_version_list = self.resources.get(ResourceType.APP_VERSION, [])
        git_commit_list = self.resources.get(ResourceType.GIT_COMMIT, [])
        env_list = self.resources.get(ResourceType.ENVIRONMENT, [])
        containers = self.resources.get(ResourceType.CONTAINER, [])
        systems = self.resources.get(ResourceType.SYSTEM, [])
        
        if not app_version_list:
            logger.info("  No app version found, skipping app version relations")
            return
        
        app_version_data = app_version_list[0]
        
        if git_commit_list:
            self.client.upsert_static_relation(
                RelationType.APP_VERSION_WITH_GIT_COMMIT,
                ResourceType.APP_VERSION, app_version_data,
                ResourceType.GIT_COMMIT, git_commit_list[0],
                self.current_time
            )
        
        if containers:
            self.client.upsert_static_relation(
                RelationType.APP_VERSION_WITH_CONTAINER,
                ResourceType.APP_VERSION, app_version_data,
                ResourceType.CONTAINER, containers[0],
                self.current_time
            )
        
        if systems:
            self.client.upsert_static_relation(
                RelationType.APP_VERSION_WITH_SYSTEM,
                ResourceType.APP_VERSION, app_version_data,
                ResourceType.SYSTEM, systems[0],
                self.current_time
            )
        
        if containers and env_list:
            self.client.upsert_static_relation(
                RelationType.CONTAINER_WITH_ENVIRONMENT,
                ResourceType.CONTAINER, containers[0],
                ResourceType.ENVIRONMENT, env_list[0],
                self.current_time
            )
        
        if env_list and systems:
            self.client.upsert_static_relation(
                RelationType.ENVIRONMENT_WITH_SYSTEM,
                ResourceType.ENVIRONMENT, env_list[0],
                ResourceType.SYSTEM, systems[0],
                self.current_time
            )
        
        logger.info("  Created App Version relations")

    # =========================================================================
    # Dynamic Traffic Relations
    # =========================================================================
    
    def create_dynamic_relations(self):
        """Create all dynamic traffic relations"""
        logger.info("Creating dynamic traffic relations...")
        
        pods = self.resources.get(ResourceType.POD, [])
        systems = self.resources.get(ResourceType.SYSTEM, [])
        services = self.resources.get(ResourceType.SERVICE, [])
        
        # pod_to_pod
        count = 0
        for source_pod_data in pods:
            if random.random() < MockConfig.POD_TO_POD_TRAFFIC_PROBABILITY:
                target_candidates = [p for p in pods if p != source_pod_data]
                if target_candidates:
                    target_pod_data = random.choice(target_candidates)
                    self.client.upsert_dynamic_relation(
                        RelationType.POD_TO_POD,
                        ResourceType.POD, source_pod_data,
                        ResourceType.POD, target_pod_data,
                        self.current_time
                    )
                    self.traffic_relations.append((source_pod_data, target_pod_data, RelationType.POD_TO_POD))
                    count += 1
        logger.info(f"    Created {count} pod_to_pod relations")
        
        # pod_to_system
        count = 0
        for pod_data in pods:
            if random.random() < MockConfig.POD_TO_SYSTEM_TRAFFIC_PROBABILITY:
                if systems:
                    target_system_data = random.choice(systems)
                    self.client.upsert_dynamic_relation(
                        RelationType.POD_TO_SYSTEM,
                        ResourceType.POD, pod_data,
                        ResourceType.SYSTEM, target_system_data,
                        self.current_time
                    )
                    count += 1
        logger.info(f"    Created {count} pod_to_system relations")
        
        # system_to_pod
        count = 0
        for system_data in systems:
            if random.random() < MockConfig.POD_TO_SYSTEM_TRAFFIC_PROBABILITY:
                if pods:
                    target_pod_data = random.choice(pods)
                    self.client.upsert_dynamic_relation(
                        RelationType.SYSTEM_TO_POD,
                        ResourceType.SYSTEM, system_data,
                        ResourceType.POD, target_pod_data,
                        self.current_time
                    )
                    count += 1
        logger.info(f"    Created {count} system_to_pod relations")
        
        # system_to_system
        count = 0
        for source_system_data in systems:
            if random.random() < MockConfig.POD_TO_SYSTEM_TRAFFIC_PROBABILITY:
                target_candidates = [s for s in systems if s != source_system_data]
                if target_candidates:
                    target_system_data = random.choice(target_candidates)
                    self.client.upsert_dynamic_relation(
                        RelationType.SYSTEM_TO_SYSTEM,
                        ResourceType.SYSTEM, source_system_data,
                        ResourceType.SYSTEM, target_system_data,
                        self.current_time
                    )
                    count += 1
        logger.info(f"    Created {count} system_to_system relations")
        
        # service_to_service
        count = 0
        for source_service_data in services:
            if random.random() < MockConfig.SERVICE_TO_SERVICE_TRAFFIC_PROBABILITY:
                target_candidates = [s for s in services if s != source_service_data]
                if target_candidates:
                    target_service_data = random.choice(target_candidates)
                    self.client.upsert_dynamic_relation(
                        RelationType.SERVICE_TO_SERVICE,
                        ResourceType.SERVICE, source_service_data,
                        ResourceType.SERVICE, target_service_data,
                        self.current_time
                    )
                    count += 1
        logger.info(f"    Created {count} service_to_service relations")

    # =========================================================================
    # Metrics
    # =========================================================================
    
    def create_metrics(self):
        """Create metric nodes and associations"""
        logger.info("Creating metrics...")
        
        metrics = [
            {"metric_name": "pod_to_pod_flow_total"},
            {"metric_name": "pod_to_pod_flow_seconds"},
            {"metric_name": "pod_to_pod_flow_error"},
            {"metric_name": "cpu_usage"},
            {"metric_name": "memory_usage"},
        ]
        
        for metric_data in metrics:
            self.client.upsert_node(ResourceType.METRIC, metric_data, self.current_time)
        
        self.resources[ResourceType.METRIC] = metrics
        logger.info(f"  Created {len(metrics)} metric definitions")

    # =========================================================================
    # Main Generation
    # =========================================================================
    
    def generate_all(self):
        """Generate all mock data"""
        logger.info("\n" + "=" * 70)
        logger.info("Starting Full Resource Graph Mock Data Generation")
        logger.info("Plan 01: Active Windows (Event-based lifecycle)")
        logger.info("=" * 70 + "\n")
        
        # Create resources in dependency order
        self.create_cmdb_resources()
        self.create_network_resources()
        self.create_k8s_resources()
        self.create_apm_resources()
        self.create_datasource_resources()
        self.create_app_version_resources()
        
        # Create static relations
        self.create_cmdb_relations()
        self.create_network_relations()
        self.create_k8s_relations()
        self.create_apm_relations()
        self.create_datasource_relations()
        self.create_app_version_relations()
        
        # Create dynamic relations
        self.create_dynamic_relations()
        
        # Create metrics
        self.create_metrics()
        
        logger.info("\n" + "=" * 70)
        logger.info("Mock Data Generation Completed!")
        logger.info("=" * 70)
        self._print_summary()

    def _print_summary(self):
        """Print generation summary"""
        logger.info("\nSummary:")
        logger.info("-" * 70)
        logger.info(f"  Business ID: {MockConfig.BIZ_ID} ({MockConfig.BIZ_NAME})")
        logger.info(f"  Cluster: {MockConfig.CLUSTER_ID}")
        logger.info(f"  Namespace: {MockConfig.NAMESPACE}")
        logger.info("-" * 70)
        logger.info("  Resource Counts:")
        for resource_type, items in sorted(self.resources.items(), key=lambda x: x[0].value):
            logger.info(f"    {resource_type.value:30s}: {len(items):5d} items")
        logger.info(f"    {'traffic_relations':30s}: {len(self.traffic_relations):5d} items")
        logger.info("-" * 70)


# ============================================================================
# Main Function
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Mock Full Resource Graph - Plan 01: Active Windows')
    parser.add_argument('--init-schema', action='store_true', help='Initialize database schema (drops existing tables)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=" * 70)
    logger.info(" Mock Full Resource Graph - Plan 01: Active Windows")
    logger.info("=" * 70)
    logger.info(f"\nConfiguration:")
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
        logger.info("Connection successful!\n")
        
        # Initialize schema if requested
        if args.init_schema:
            client.init_schema()
        
        # Generate data
        generator = FullMockGenerator(client)
        generator.generate_all()
        
        logger.info("\nDone!")
        return 0

    except Exception as e:
        logger.error(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
