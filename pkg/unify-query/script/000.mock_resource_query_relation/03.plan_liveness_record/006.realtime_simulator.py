#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Realtime Simulator - Plan 03: Liveness Record

This script simulates a real production environment by:
1. Importing real data from unify-query every 5 minutes
2. Heartbeat refresh every 2 minutes (60% of resources)
3. Generating pod-to-pod traffic events with configurable probability
4. Running continuously in the background

Default Behavior:
- Data import interval: 5 minutes (from unify-query)
- Heartbeat interval: 2 minutes
- Heartbeat refresh ratio: 60% of resources
- Pod-to-pod traffic probability: 30%
- Pod-to-system traffic probability: 20%

Usage:
    # Run with default settings
    python 006.realtime_simulator.py
    
    # Run with custom intervals
    python 006.realtime_simulator.py --import-interval 300 --heartbeat-interval 120
    
    # Run with custom refresh ratio
    python 006.realtime_simulator.py --refresh-ratio 0.6
    
    # Initialize schema first
    python 006.realtime_simulator.py --init-schema
    
    # Run in foreground with debug logging
    python 006.realtime_simulator.py --debug --foreground

Configuration:
    Connection settings are managed in config.yaml
"""

import argparse
import logging
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple

import requests

# Disable SSL warnings for self-signed certificates
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# Logging Configuration
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s',
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


class UnifyQueryConfig:
    URL = _config.get('unify_query', {}).get('url', '')
    APP_CODE = _config.get('unify_query', {}).get('app_code', '')
    APP_SECRET = _config.get('unify_query', {}).get('app_secret', '')
    USERNAME = _config.get('unify_query', {}).get('username', '')
    SPACE_UID = _config.get('unify_query', {}).get('space_uid', '')


class SimulatorConfig:
    """Simulator configuration with defaults"""
    # Time intervals (in seconds)
    IMPORT_INTERVAL = 300  # 5 minutes
    HEARTBEAT_INTERVAL = 120  # 2 minutes
    
    # Refresh ratio
    REFRESH_RATIO = 0.6  # 60% of resources
    
    # Traffic probabilities
    POD_TO_POD_PROBABILITY = 0.3  # 30% chance of pod-to-pod traffic
    POD_TO_SYSTEM_PROBABILITY = 0.2  # 20% chance of pod-to-system traffic
    SERVICE_TO_SERVICE_PROBABILITY = 0.25  # 25% chance of service-to-service traffic
    
    # Liveness tolerance
    TOLERANCE_TIME_MS = _config.get('mock', {}).get('tolerance_time_ms', 300000)
    
    # Default cluster/namespace for mock data
    DEFAULT_CLUSTER_ID = "BCS-K8S-00002"
    DEFAULT_NAMESPACE = "bkop"
    DEFAULT_CLOUD_ID = "0"


# ============================================================================
# Schema Loading
# ============================================================================

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.sql')


def load_schema_sql(tolerance_time_ms: int = None) -> str:
    """Load schema SQL from external file and replace tolerance_time placeholder."""
    if tolerance_time_ms is None:
        tolerance_time_ms = SimulatorConfig.TOLERANCE_TIME_MS
    
    if not os.path.exists(SCHEMA_FILE):
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_FILE}")
    
    with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    schema_sql = schema_sql.replace('{tolerance_time_ms}', str(tolerance_time_ms))
    
    logger.info(f"Loaded schema from {SCHEMA_FILE} with tolerance_time_ms={tolerance_time_ms}")
    return schema_sql


# ============================================================================
# SurrealDB Client
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
        self._lock = threading.Lock()
        logger.info(f"SurrealDB client initialized: {url}/{namespace}/{database}")

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query via HTTP REST API (thread-safe)"""
        with self._lock:
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
            if not stripped or stripped.startswith('--'):
                continue
            brace_depth += stripped.count('{') - stripped.count('}')
            current.append(line)
            if brace_depth == 0 and stripped.endswith(';'):
                stmt = '\n'.join(current).strip()
                if stmt:
                    statements.append(stmt.rstrip(';'))
                current = []
        
        if current:
            stmt = '\n'.join(current).strip()
            if stmt:
                statements.append(stmt.rstrip(';'))
        
        return statements

    # ========================================================================
    # Resource UPSERT Methods
    # ========================================================================

    def upsert_pod(self, bcs_cluster_id: str, namespace: str, pod: str, 
                   updated_at: int = None) -> Dict[str, Any]:
        """Upsert pod"""
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
        """Upsert node"""
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
        """Upsert container"""
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
        """Upsert deployment"""
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
        """Upsert replicaset"""
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
        """Upsert service"""
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
        """Upsert system"""
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

    def upsert_pod_to_pod(self, from_cluster: str, from_ns: str, from_pod: str,
                          to_cluster: str, to_ns: str, to_pod: str,
                          updated_at: int = None) -> Dict[str, Any]:
        """Upsert pod_to_pod relation"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        from_id = f'pod:⟨bcs_cluster_id={from_cluster},namespace={from_ns},pod={from_pod}⟩'
        to_id = f'pod:⟨bcs_cluster_id={to_cluster},namespace={to_ns},pod={to_pod}⟩'
        
        return self.upsert_relation("pod_to_pod", from_id, to_id, updated_at)

    def upsert_pod_to_system(self, cluster: str, ns: str, pod: str,
                             cloud_id: str, ip: str,
                             updated_at: int = None) -> Dict[str, Any]:
        """Upsert pod_to_system relation"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        from_id = f'pod:⟨bcs_cluster_id={cluster},namespace={ns},pod={pod}⟩'
        to_id = f'system:⟨bk_cloud_id={cloud_id},bk_target_ip={ip}⟩'
        
        return self.upsert_relation("pod_to_system", from_id, to_id, updated_at)

    def upsert_node_with_pod(self, cluster: str, node: str, ns: str, pod: str,
                             updated_at: int = None) -> Dict[str, Any]:
        """Upsert node_with_pod relation"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        from_id = f'node:⟨bcs_cluster_id={cluster},node={node}⟩'
        to_id = f'pod:⟨bcs_cluster_id={cluster},namespace={ns},pod={pod}⟩'
        
        return self.upsert_relation("node_with_pod", from_id, to_id, updated_at)

    def upsert_container_with_pod(self, cluster: str, ns: str, pod: str, container: str,
                                  updated_at: int = None) -> Dict[str, Any]:
        """Upsert container_with_pod relation"""
        if updated_at is None:
            updated_at = self.datetime_to_ms()
        
        from_id = f'container:⟨bcs_cluster_id={cluster},namespace={ns},pod={pod},container={container}⟩'
        to_id = f'pod:⟨bcs_cluster_id={cluster},namespace={ns},pod={pod}⟩'
        
        return self.upsert_relation("container_with_pod", from_id, to_id, updated_at)

    # ========================================================================
    # Query Methods
    # ========================================================================

    def get_all_pods(self) -> List[Dict]:
        """Get all pods from database"""
        sql = "SELECT * FROM pod;"
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def get_all_nodes(self) -> List[Dict]:
        """Get all nodes from database"""
        sql = "SELECT * FROM node;"
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def get_all_containers(self) -> List[Dict]:
        """Get all containers from database"""
        sql = "SELECT * FROM container;"
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def get_all_services(self) -> List[Dict]:
        """Get all services from database"""
        sql = "SELECT * FROM service;"
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def get_all_systems(self) -> List[Dict]:
        """Get all systems from database"""
        sql = "SELECT * FROM system;"
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []

    def get_all_relations(self, relation_type: str) -> List[Dict]:
        """Get all relations of a specific type"""
        sql = f"SELECT * FROM {relation_type};"
        results = self.execute_sql(sql)
        if results and results[0].get('result') is not None:
            return results[0]['result']
        return []


# ============================================================================
# Unify-Query Client
# ============================================================================

class UnifyQueryClient:
    """Client for fetching real data from unify-query API"""

    def __init__(self):
        self.url = UnifyQueryConfig.URL
        self.app_code = UnifyQueryConfig.APP_CODE
        self.app_secret = UnifyQueryConfig.APP_SECRET
        self.username = UnifyQueryConfig.USERNAME
        self.space_uid = UnifyQueryConfig.SPACE_UID
        self.session = requests.Session()
        self.session.verify = False
        
        # Extract base URL for query_list API
        if self.url:
            self.base_url = self.url.rsplit('/query/ts', 1)[0]
        else:
            self.base_url = ''

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        import json
        return {
            'Content-Type': 'application/json',
            'X-Bkapi-Authorization': json.dumps({
                'bk_app_code': self.app_code,
                'bk_app_secret': self.app_secret,
                'bk_username': self.username
            })
        }

    def query_ts(self, field_name: str, dimensions: List[str], limit: int = 10000) -> Dict[str, Any]:
        """Execute query using query_list format"""
        if not self.base_url:
            logger.warning("Unify-Query URL not configured, skipping query")
            return {}
        
        url = f"{self.base_url}/query/ts"
        now = int(time.time())
        
        payload = {
            'query_list': [{
                'data_source': 'bkmonitor',
                'field_name': field_name,
                'is_regexp': False,
                'function': [{
                    'method': 'count',
                    'dimensions': dimensions
                }],
                'time_aggregation': {},
                'reference_name': 'a',
                'limit': limit,
                'conditions': {'field_list': []}
            }],
            'metric_merge': 'a',
            'start_time': str(now - 300),
            'end_time': str(now),
            'step': '60s',
            'timezone': 'Asia/Shanghai',
            'instant': True,
            'space_uid': self.space_uid
        }
        
        try:
            response = self.session.post(
                url,
                headers=self._get_headers(),
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                logger.error(f"Unify-Query error: {response.status_code} - {response.text[:200]}")
                return {}
            
            return response.json()
        except Exception as e:
            logger.error(f"Unify-Query request failed: {e}")
            return {}

    def _parse_series(self, result: Dict, required_keys: List[str]) -> List[Dict]:
        """Parse series from query result"""
        records = []
        seen = set()
        
        series_list = result.get('series', [])
        for series in series_list:
            group_keys = series.get('group_keys', [])
            group_values = series.get('group_values', [])
            
            if len(group_keys) != len(group_values):
                continue
            
            record = dict(zip(group_keys, group_values))
            
            # Create unique key
            key = '|'.join(str(record.get(k, '')) for k in required_keys)
            if key in seen:
                continue
            seen.add(key)
            
            # Check all required keys exist
            if all(k in record and record[k] for k in required_keys):
                records.append(record)
        
        return records

    def fetch_pods(self) -> List[Dict]:
        """Fetch pod information from unify-query using pod_with_service_relation"""
        result = self.query_ts(
            field_name='pod_with_service_relation',
            dimensions=['bcs_cluster_id', 'namespace', 'pod', 'service'],
            limit=10000
        )
        
        if not result:
            return []
        
        records = self._parse_series(result, ['bcs_cluster_id', 'namespace', 'pod'])
        
        # Deduplicate pods (same pod may have multiple services)
        pods = []
        seen = set()
        for r in records:
            key = f"{r['bcs_cluster_id']}|{r['namespace']}|{r['pod']}"
            if key not in seen:
                seen.add(key)
                pods.append({
                    'bcs_cluster_id': r['bcs_cluster_id'],
                    'namespace': r['namespace'],
                    'pod': r['pod']
                })
        
        return pods

    def fetch_nodes(self) -> List[Dict]:
        """Fetch node information from unify-query"""
        # Try using node_with_system_relation or similar metric
        result = self.query_ts(
            field_name='node_with_system_relation',
            dimensions=['bcs_cluster_id', 'node'],
            limit=1000
        )
        
        if not result or not result.get('series'):
            # Fallback: try container_cpu_usage_seconds_total to get nodes
            result = self.query_ts(
                field_name='container_cpu_usage_seconds_total',
                dimensions=['bcs_cluster_id', 'node'],
                limit=1000
            )
        
        if not result:
            return []
        
        return self._parse_series(result, ['bcs_cluster_id', 'node'])

    def fetch_services(self) -> List[Dict]:
        """Fetch service information from unify-query"""
        result = self.query_ts(
            field_name='pod_with_service_relation',
            dimensions=['bcs_cluster_id', 'namespace', 'service'],
            limit=5000
        )
        
        if not result:
            return []
        
        return self._parse_series(result, ['bcs_cluster_id', 'namespace', 'service'])

    def fetch_pod_service_relations(self) -> List[Dict]:
        """Fetch pod-service relations from unify-query"""
        result = self.query_ts(
            field_name='pod_with_service_relation',
            dimensions=['bcs_cluster_id', 'namespace', 'pod', 'service'],
            limit=10000
        )
        
        if not result:
            return []
        
        return self._parse_series(result, ['bcs_cluster_id', 'namespace', 'pod', 'service'])


# ============================================================================
# Resource Cache
# ============================================================================

class ResourceCache:
    """Thread-safe cache for resources"""

    def __init__(self):
        self._lock = threading.RLock()
        self.pods: List[Dict] = []
        self.nodes: List[Dict] = []
        self.containers: List[Dict] = []
        self.services: List[Dict] = []
        self.systems: List[Dict] = []
        self.relations: Dict[str, List[Dict]] = {}
        self.last_update = 0

    def update_from_db(self, client: SurrealDBClient):
        """Update cache from database"""
        with self._lock:
            self.pods = client.get_all_pods()
            self.nodes = client.get_all_nodes()
            self.containers = client.get_all_containers()
            self.services = client.get_all_services()
            self.systems = client.get_all_systems()
            self.relations['node_with_pod'] = client.get_all_relations('node_with_pod')
            self.relations['container_with_pod'] = client.get_all_relations('container_with_pod')
            self.relations['pod_to_pod'] = client.get_all_relations('pod_to_pod')
            self.relations['pod_to_system'] = client.get_all_relations('pod_to_system')
            self.last_update = time.time()
            
            logger.info(f"Cache updated: {len(self.pods)} pods, {len(self.nodes)} nodes, "
                       f"{len(self.containers)} containers, {len(self.services)} services, "
                       f"{len(self.systems)} systems")

    def get_random_pods(self, count: int) -> List[Dict]:
        """Get random pods from cache"""
        with self._lock:
            if not self.pods:
                return []
            return random.sample(self.pods, min(count, len(self.pods)))

    def get_random_nodes(self, count: int) -> List[Dict]:
        """Get random nodes from cache"""
        with self._lock:
            if not self.nodes:
                return []
            return random.sample(self.nodes, min(count, len(self.nodes)))

    def get_random_systems(self, count: int) -> List[Dict]:
        """Get random systems from cache"""
        with self._lock:
            if not self.systems:
                return []
            return random.sample(self.systems, min(count, len(self.systems)))

    def get_sample_for_refresh(self, resource_type: str, ratio: float) -> List[Dict]:
        """Get a sample of resources for heartbeat refresh"""
        with self._lock:
            if resource_type == 'pod':
                resources = self.pods
            elif resource_type == 'node':
                resources = self.nodes
            elif resource_type == 'container':
                resources = self.containers
            elif resource_type == 'service':
                resources = self.services
            elif resource_type == 'system':
                resources = self.systems
            else:
                return []
            
            if not resources:
                return []
            
            count = max(1, int(len(resources) * ratio))
            return random.sample(resources, min(count, len(resources)))


# ============================================================================
# Realtime Simulator
# ============================================================================

class RealtimeSimulator:
    """Main simulator class that orchestrates all background tasks"""

    def __init__(
            self,
            import_interval: int = SimulatorConfig.IMPORT_INTERVAL,
            heartbeat_interval: int = SimulatorConfig.HEARTBEAT_INTERVAL,
            refresh_ratio: float = SimulatorConfig.REFRESH_RATIO,
            pod_to_pod_prob: float = SimulatorConfig.POD_TO_POD_PROBABILITY,
            pod_to_system_prob: float = SimulatorConfig.POD_TO_SYSTEM_PROBABILITY
    ):
        self.import_interval = import_interval
        self.heartbeat_interval = heartbeat_interval
        self.refresh_ratio = refresh_ratio
        self.pod_to_pod_prob = pod_to_pod_prob
        self.pod_to_system_prob = pod_to_system_prob
        
        self.db_client = SurrealDBClient()
        self.uq_client = UnifyQueryClient()
        self.cache = ResourceCache()
        
        self._running = False
        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        
        # Statistics
        self.stats = {
            'imports': 0,
            'heartbeats': 0,
            'pod_to_pod_events': 0,
            'pod_to_system_events': 0,
            'errors': 0,
            'start_time': 0
        }

    def init_schema(self, tolerance_time_ms: int = None):
        """Initialize database schema"""
        self.db_client.init_schema(tolerance_time_ms)

    def _import_data_task(self):
        """Background task: Import real data from unify-query"""
        logger.info(f"Data import task started (interval: {self.import_interval}s)")
        
        while not self._stop_event.is_set():
            try:
                self._do_import()
                self.stats['imports'] += 1
            except Exception as e:
                logger.error(f"Import task error: {e}")
                self.stats['errors'] += 1
            
            # Wait for next interval or stop signal
            self._stop_event.wait(self.import_interval)

    def _do_import(self):
        """Perform data import from unify-query"""
        logger.info("Starting data import from unify-query...")
        now = self.db_client.datetime_to_ms()
        
        # Try to fetch real data from unify-query
        pods_from_uq = self.uq_client.fetch_pods()
        nodes_from_uq = self.uq_client.fetch_nodes()
        
        if pods_from_uq:
            logger.info(f"Fetched {len(pods_from_uq)} pods from unify-query")
            for pod in pods_from_uq:
                self.db_client.upsert_pod(
                    pod['bcs_cluster_id'], pod['namespace'], pod['pod'], now
                )
        else:
            # Generate mock data if unify-query is not available
            logger.info("No data from unify-query, generating mock data...")
            self._generate_mock_resources(now)
        
        if nodes_from_uq:
            logger.info(f"Fetched {len(nodes_from_uq)} nodes from unify-query")
            for node in nodes_from_uq:
                self.db_client.upsert_node(node['bcs_cluster_id'], node['node'], now)
        
        # Update cache from database
        self.cache.update_from_db(self.db_client)
        
        # Generate relations for imported resources
        self._generate_relations(now)
        
        logger.info("Data import completed")

    def _generate_mock_resources(self, updated_at: int):
        """Generate mock resources when unify-query is not available"""
        cluster_id = SimulatorConfig.DEFAULT_CLUSTER_ID
        namespace = SimulatorConfig.DEFAULT_NAMESPACE
        cloud_id = SimulatorConfig.DEFAULT_CLOUD_ID
        
        # Generate nodes
        for i in range(3):
            node_name = f"node-{i}"
            self.db_client.upsert_node(cluster_id, node_name, updated_at)
        
        # Generate pods
        for i in range(10):
            pod_name = f"pod-{i}"
            self.db_client.upsert_pod(cluster_id, namespace, pod_name, updated_at)
            
            # Generate container for each pod
            container_name = f"container-{i}"
            self.db_client.upsert_container(
                cluster_id, namespace, pod_name, container_name, updated_at
            )
        
        # Generate services
        for svc in ["api", "web", "worker"]:
            self.db_client.upsert_service(cluster_id, namespace, svc, updated_at)
        
        # Generate systems
        for i in range(5):
            ip = f"10.0.0.{i+1}"
            self.db_client.upsert_system(ip, cloud_id, updated_at)

    def _generate_relations(self, updated_at: int):
        """Generate relations for resources"""
        pods = self.cache.pods
        nodes = self.cache.nodes
        containers = self.cache.containers
        systems = self.cache.systems
        
        if not pods:
            return
        
        # node_with_pod relations
        if nodes:
            for pod in pods:
                node = random.choice(nodes)
                if pod.get('bcs_cluster_id') == node.get('bcs_cluster_id'):
                    self.db_client.upsert_node_with_pod(
                        pod['bcs_cluster_id'], node['node'],
                        pod['namespace'], pod['pod'], updated_at
                    )
        
        # container_with_pod relations
        for container in containers:
            self.db_client.upsert_container_with_pod(
                container['bcs_cluster_id'], container['namespace'],
                container['pod'], container['container'], updated_at
            )

    def _heartbeat_task(self):
        """Background task: Heartbeat refresh for resources"""
        logger.info(f"Heartbeat task started (interval: {self.heartbeat_interval}s, ratio: {self.refresh_ratio})")
        
        while not self._stop_event.is_set():
            try:
                self._do_heartbeat()
                self.stats['heartbeats'] += 1
            except Exception as e:
                logger.error(f"Heartbeat task error: {e}")
                self.stats['errors'] += 1
            
            self._stop_event.wait(self.heartbeat_interval)

    def _do_heartbeat(self):
        """Perform heartbeat refresh"""
        now = self.db_client.datetime_to_ms()
        refreshed = {'pods': 0, 'nodes': 0, 'containers': 0, 'relations': 0}
        
        # Refresh pods
        pods_to_refresh = self.cache.get_sample_for_refresh('pod', self.refresh_ratio)
        for pod in pods_to_refresh:
            self.db_client.upsert_pod(
                pod['bcs_cluster_id'], pod['namespace'], pod['pod'], now
            )
            refreshed['pods'] += 1
        
        # Refresh nodes
        nodes_to_refresh = self.cache.get_sample_for_refresh('node', self.refresh_ratio)
        for node in nodes_to_refresh:
            self.db_client.upsert_node(node['bcs_cluster_id'], node['node'], now)
            refreshed['nodes'] += 1
        
        # Refresh containers
        containers_to_refresh = self.cache.get_sample_for_refresh('container', self.refresh_ratio)
        for container in containers_to_refresh:
            self.db_client.upsert_container(
                container['bcs_cluster_id'], container['namespace'],
                container['pod'], container['container'], now
            )
            refreshed['containers'] += 1
        
        # Refresh related relations (node_with_pod, container_with_pod)
        for pod in pods_to_refresh:
            nodes = self.cache.nodes
            if nodes:
                node = random.choice([n for n in nodes if n.get('bcs_cluster_id') == pod.get('bcs_cluster_id')] or nodes)
                self.db_client.upsert_node_with_pod(
                    pod['bcs_cluster_id'], node['node'],
                    pod['namespace'], pod['pod'], now
                )
                refreshed['relations'] += 1
        
        logger.info(f"Heartbeat refresh: {refreshed}")

    def _traffic_task(self):
        """Background task: Generate pod-to-pod and pod-to-system traffic events"""
        logger.info(f"Traffic task started (pod-to-pod: {self.pod_to_pod_prob}, pod-to-system: {self.pod_to_system_prob})")
        
        # Traffic generation runs more frequently (every 30 seconds)
        traffic_interval = 30
        
        while not self._stop_event.is_set():
            try:
                self._do_traffic_generation()
            except Exception as e:
                logger.error(f"Traffic task error: {e}")
                self.stats['errors'] += 1
            
            self._stop_event.wait(traffic_interval)

    def _do_traffic_generation(self):
        """Generate traffic events between pods and systems"""
        now = self.db_client.datetime_to_ms()
        pods = self.cache.pods
        systems = self.cache.systems
        
        if not pods:
            return
        
        pod_to_pod_count = 0
        pod_to_system_count = 0
        
        # Generate pod-to-pod traffic
        for pod in pods:
            if random.random() < self.pod_to_pod_prob:
                # Pick a random target pod (different from source)
                other_pods = [p for p in pods if p != pod]
                if other_pods:
                    target = random.choice(other_pods)
                    self.db_client.upsert_pod_to_pod(
                        pod['bcs_cluster_id'], pod['namespace'], pod['pod'],
                        target['bcs_cluster_id'], target['namespace'], target['pod'],
                        now
                    )
                    pod_to_pod_count += 1
        
        # Generate pod-to-system traffic
        if systems:
            for pod in pods:
                if random.random() < self.pod_to_system_prob:
                    target = random.choice(systems)
                    self.db_client.upsert_pod_to_system(
                        pod['bcs_cluster_id'], pod['namespace'], pod['pod'],
                        target['bk_cloud_id'], target['bk_target_ip'],
                        now
                    )
                    pod_to_system_count += 1
        
        if pod_to_pod_count > 0 or pod_to_system_count > 0:
            logger.debug(f"Traffic generated: pod-to-pod={pod_to_pod_count}, pod-to-system={pod_to_system_count}")
        
        self.stats['pod_to_pod_events'] += pod_to_pod_count
        self.stats['pod_to_system_events'] += pod_to_system_count

    def start(self):
        """Start all background tasks"""
        if self._running:
            logger.warning("Simulator is already running")
            return
        
        self._running = True
        self._stop_event.clear()
        self.stats['start_time'] = time.time()
        
        # Initial data import
        logger.info("Performing initial data import...")
        self._do_import()
        
        # Start background threads
        import_thread = threading.Thread(target=self._import_data_task, name="ImportTask", daemon=True)
        heartbeat_thread = threading.Thread(target=self._heartbeat_task, name="HeartbeatTask", daemon=True)
        traffic_thread = threading.Thread(target=self._traffic_task, name="TrafficTask", daemon=True)
        
        self._threads = [import_thread, heartbeat_thread, traffic_thread]
        
        for t in self._threads:
            t.start()
        
        logger.info("=" * 60)
        logger.info("Realtime Simulator started!")
        logger.info(f"  Import interval: {self.import_interval}s")
        logger.info(f"  Heartbeat interval: {self.heartbeat_interval}s")
        logger.info(f"  Refresh ratio: {self.refresh_ratio * 100}%")
        logger.info(f"  Pod-to-pod probability: {self.pod_to_pod_prob * 100}%")
        logger.info(f"  Pod-to-system probability: {self.pod_to_system_prob * 100}%")
        logger.info("=" * 60)

    def stop(self):
        """Stop all background tasks"""
        if not self._running:
            return
        
        logger.info("Stopping simulator...")
        self._stop_event.set()
        
        for t in self._threads:
            t.join(timeout=5)
        
        self._running = False
        self._print_stats()
        logger.info("Simulator stopped")

    def _print_stats(self):
        """Print statistics"""
        runtime = time.time() - self.stats['start_time']
        logger.info("=" * 60)
        logger.info("Simulator Statistics:")
        logger.info(f"  Runtime: {runtime:.1f}s")
        logger.info(f"  Imports: {self.stats['imports']}")
        logger.info(f"  Heartbeats: {self.stats['heartbeats']}")
        logger.info(f"  Pod-to-pod events: {self.stats['pod_to_pod_events']}")
        logger.info(f"  Pod-to-system events: {self.stats['pod_to_system_events']}")
        logger.info(f"  Errors: {self.stats['errors']}")
        logger.info("=" * 60)

    def run_forever(self):
        """Run simulator until interrupted"""
        self.start()
        
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            self.stop()


# ============================================================================
# Signal Handlers
# ============================================================================

_simulator: Optional[RealtimeSimulator] = None


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global _simulator
    if _simulator:
        _simulator.stop()
    sys.exit(0)


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    global _simulator
    
    parser = argparse.ArgumentParser(
        description='Realtime Simulator - Plan 03: Liveness Record',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with default settings
    python 006.realtime_simulator.py
    
    # Initialize schema first
    python 006.realtime_simulator.py --init-schema
    
    # Custom intervals
    python 006.realtime_simulator.py --import-interval 300 --heartbeat-interval 120
    
    # Custom probabilities
    python 006.realtime_simulator.py --pod-to-pod-prob 0.3 --pod-to-system-prob 0.2
        """
    )
    
    parser.add_argument(
        '--init-schema', action='store_true',
        help='Initialize database schema before starting'
    )
    parser.add_argument(
        '--import-interval', type=int, default=SimulatorConfig.IMPORT_INTERVAL,
        help=f'Data import interval in seconds (default: {SimulatorConfig.IMPORT_INTERVAL})'
    )
    parser.add_argument(
        '--heartbeat-interval', type=int, default=SimulatorConfig.HEARTBEAT_INTERVAL,
        help=f'Heartbeat refresh interval in seconds (default: {SimulatorConfig.HEARTBEAT_INTERVAL})'
    )
    parser.add_argument(
        '--refresh-ratio', type=float, default=SimulatorConfig.REFRESH_RATIO,
        help=f'Ratio of resources to refresh on each heartbeat (default: {SimulatorConfig.REFRESH_RATIO})'
    )
    parser.add_argument(
        '--pod-to-pod-prob', type=float, default=SimulatorConfig.POD_TO_POD_PROBABILITY,
        help=f'Probability of pod-to-pod traffic events (default: {SimulatorConfig.POD_TO_POD_PROBABILITY})'
    )
    parser.add_argument(
        '--pod-to-system-prob', type=float, default=SimulatorConfig.POD_TO_SYSTEM_PROBABILITY,
        help=f'Probability of pod-to-system traffic events (default: {SimulatorConfig.POD_TO_SYSTEM_PROBABILITY})'
    )
    parser.add_argument(
        '--tolerance-ms', type=int, default=SimulatorConfig.TOLERANCE_TIME_MS,
        help=f'Tolerance time in milliseconds (default: {SimulatorConfig.TOLERANCE_TIME_MS})'
    )
    parser.add_argument(
        '--debug', action='store_true',
        help='Enable debug logging'
    )
    parser.add_argument(
        '--foreground', action='store_true',
        help='Run in foreground (default behavior, kept for compatibility)'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create simulator
    _simulator = RealtimeSimulator(
        import_interval=args.import_interval,
        heartbeat_interval=args.heartbeat_interval,
        refresh_ratio=args.refresh_ratio,
        pod_to_pod_prob=args.pod_to_pod_prob,
        pod_to_system_prob=args.pod_to_system_prob
    )
    
    # Initialize schema if requested
    if args.init_schema:
        logger.info("Initializing schema...")
        _simulator.init_schema(args.tolerance_ms)
    
    # Run forever
    _simulator.run_forever()


if __name__ == '__main__':
    main()
