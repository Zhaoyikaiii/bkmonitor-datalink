#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mock Full Resource Graph to SurrealDB

This script generates mock resource association data for all resource types
defined in the design document (00.文档.md), including:
- All Kubernetes resources (Pod, Node, Container, Deployment, ReplicaSet, etc.)
- Network resources (System, K8s Address, Domain)
- APM resources (APM Service, APM Service Instance)
- Data source resources (DataSource, BKLogConfig)
- CMDB resources (Biz, Set, Module, Host)
- App version resources (App Version, Git Commit, Environment)
- All static relations (27 types)
- All dynamic relations (5 types)
- Metric associations

Key Features:
    - Uses SurrealDB RELATION type tables for proper graph traversal
    - Uses document-defined ID format for nodes and relations
    - Implements ALL resource types from design document section 2
    - Implements ALL relation types from design document section 3
    - Idempotent: can be run multiple times without data conflicts

ID Format (per documentation section 4):
    - Node ID: {resource_type}:{key1}={value1},{key2}={value2},...
    - Static Relation ID: {res1}_with_{res2}:{res1_kv}|{res2_kv} (res1 < res2 alphabetically)
    - Dynamic Relation ID: {src}_to_{dst}:{src_kv}|{dst_kv}

Usage:
    # Initialize schema (first time or to reset)
    python 002.mock_full_resource_graph.py --backend native --init-schema
    
    # Use native SurrealDB (default)
    python 002.mock_full_resource_graph.py --backend native
    
    # Enable debug logging
    python 002.mock_full_resource_graph.py --backend=native --debug

Configuration:
    Connection settings are managed in config.yaml
"""

import argparse
import logging
import os
import random
import sys
from datetime import datetime, timedelta
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
    
    # Time
    DEFAULT_TIME_BACK_HOURS = 1
    START_TIME = datetime.now().replace(tzinfo=None) - timedelta(hours=DEFAULT_TIME_BACK_HOURS)
    END_TIME = datetime.now().replace(tzinfo=None)


# ============================================================================
# Enums - All Resource Types from Design Document Section 2
# ============================================================================

class ResourceType(Enum):
    # Kubernetes Resources (Section 2.1)
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
    
    # Network Resources (Section 2.2)
    SYSTEM = "system"
    K8S_ADDRESS = "k8s_address"
    DOMAIN = "domain"
    
    # APM Resources (Section 2.3)
    APM_SERVICE = "apm_service"
    APM_SERVICE_INSTANCE = "apm_service_instance"
    
    # Data Source Resources (Section 2.4)
    DATASOURCE = "datasource"
    BKLOGCONFIG = "bklogconfig"
    
    # CMDB Resources (Section 2.5)
    BIZ = "biz"
    SET = "set"
    MODULE = "module"
    HOST = "host"
    
    # App Version Resources (Section 2.6)
    APP_VERSION = "app_version"
    GIT_COMMIT = "git_commit"
    ENVIRONMENT = "environment"
    
    # Metric (Section 7)
    METRIC = "metric"


# ============================================================================
# Enums - All Relation Types from Design Document Section 3
# ============================================================================

class RelationType(Enum):
    # Kubernetes Static Relations (Section 3.2)
    NODE_WITH_SYSTEM = "node_with_system"
    NODE_WITH_POD = "node_with_pod"
    JOB_WITH_POD = "job_with_pod"
    POD_WITH_REPLICASET = "pod_with_replicaset"
    POD_WITH_STATEFULSET = "pod_with_statefulset"
    DAEMONSET_WITH_POD = "daemonset_with_pod"
    DEPLOYMENT_WITH_REPLICASET = "deployment_with_replicaset"
    POD_WITH_SERVICE = "pod_with_service"
    INGRESS_WITH_SERVICE = "ingress_with_service"
    
    # Network Static Relations (Section 3.3)
    K8S_ADDRESS_WITH_SERVICE = "k8s_address_with_service"
    DOMAIN_WITH_SERVICE = "domain_with_service"
    
    # APM Static Relations (Section 3.4)
    APM_SERVICE_INSTANCE_WITH_POD = "apm_service_instance_with_pod"
    APM_SERVICE_INSTANCE_WITH_SYSTEM = "apm_service_instance_with_system"
    APM_SERVICE_WITH_APM_SERVICE_INSTANCE = "apm_service_with_apm_service_instance"
    
    # Container Static Relations (Section 3.5)
    CONTAINER_WITH_POD = "container_with_pod"
    
    # Data Source Static Relations (Section 3.6)
    DATASOURCE_WITH_POD = "datasource_with_pod"
    DATASOURCE_WITH_NODE = "datasource_with_node"
    BKLOGCONFIG_WITH_DATASOURCE = "bklogconfig_with_datasource"
    
    # CMDB Static Relations (Section 3.7)
    BIZ_WITH_SET = "biz_with_set"
    MODULE_WITH_SET = "module_with_set"
    HOST_WITH_MODULE = "host_with_module"
    HOST_WITH_SYSTEM = "host_with_system"
    
    # App Version Static Relations (Section 3.8)
    APP_VERSION_WITH_CONTAINER = "app_version_with_container"
    APP_VERSION_WITH_SYSTEM = "app_version_with_system"
    CONTAINER_WITH_ENVIRONMENT = "container_with_environment"
    ENVIRONMENT_WITH_SYSTEM = "environment_with_system"
    APP_VERSION_WITH_GIT_COMMIT = "app_version_with_git_commit"
    
    # Dynamic Relations (Section 3.9)
    POD_TO_POD = "pod_to_pod"
    POD_TO_SYSTEM = "pod_to_system"
    SYSTEM_TO_POD = "system_to_pod"
    SYSTEM_TO_SYSTEM = "system_to_system"
    SERVICE_TO_SERVICE = "service_to_service"
    
    # Metric Relations (Section 7)
    NODE_HAS_METRIC = "node_has_metric"
    RELATION_HAS_METRIC = "relation_has_metric"


# ============================================================================
# Resource Index Fields Definition (per documentation section 2)
# ============================================================================

RESOURCE_INDEX_FIELDS = {
    # Kubernetes Resources
    ResourceType.POD: ["bcs_cluster_id", "namespace", "pod"],
    ResourceType.NODE: ["bcs_cluster_id", "node"],
    ResourceType.CONTAINER: ["bcs_cluster_id", "namespace", "pod", "container"],
    ResourceType.DEPLOYMENT: ["bcs_cluster_id", "namespace", "deployment"],
    ResourceType.REPLICASET: ["bcs_cluster_id", "namespace", "replicaset"],
    ResourceType.STATEFULSET: ["bcs_cluster_id", "namespace", "statefulset"],
    ResourceType.DAEMONSET: ["bcs_cluster_id", "namespace", "daemonset"],
    ResourceType.JOB: ["bcs_cluster_id", "namespace", "job"],
    ResourceType.SERVICE: ["bcs_cluster_id", "namespace", "service"],
    ResourceType.INGRESS: ["bcs_cluster_id", "namespace", "ingress"],
    ResourceType.CLUSTER: ["bcs_cluster_id"],
    ResourceType.NAMESPACE: ["bcs_cluster_id", "namespace"],
    
    # Network Resources
    ResourceType.SYSTEM: ["bk_cloud_id", "bk_target_ip"],
    ResourceType.K8S_ADDRESS: ["bcs_cluster_id", "address"],
    ResourceType.DOMAIN: ["bcs_cluster_id", "domain"],
    
    # APM Resources
    ResourceType.APM_SERVICE: ["apm_application_name", "apm_service_name"],
    ResourceType.APM_SERVICE_INSTANCE: ["apm_application_name", "apm_service_name", "apm_service_instance_name"],
    
    # Data Source Resources
    ResourceType.DATASOURCE: ["bk_data_id"],
    ResourceType.BKLOGCONFIG: ["bklogconfig_namespace", "bklogconfig_name"],
    
    # CMDB Resources
    ResourceType.BIZ: ["bk_biz_id"],
    ResourceType.SET: ["bk_set_id"],
    ResourceType.MODULE: ["bk_module_id"],
    ResourceType.HOST: ["bk_host_id"],
    
    # App Version Resources
    ResourceType.APP_VERSION: ["app_name", "version"],
    ResourceType.GIT_COMMIT: ["git_repo", "commit_id"],
    ResourceType.ENVIRONMENT: ["environment"],
    
    # Metric
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

def generate_schema_sql() -> str:
    """Generate complete schema SQL for all resource and relation types"""
    
    # Node table definitions
    node_tables = """
-- ============================================
-- Drop existing tables (for clean reset)
-- ============================================

-- Drop all node tables
REMOVE TABLE IF EXISTS pod;
REMOVE TABLE IF EXISTS node;
REMOVE TABLE IF EXISTS container;
REMOVE TABLE IF EXISTS deployment;
REMOVE TABLE IF EXISTS replicaset;
REMOVE TABLE IF EXISTS statefulset;
REMOVE TABLE IF EXISTS daemonset;
REMOVE TABLE IF EXISTS job;
REMOVE TABLE IF EXISTS service;
REMOVE TABLE IF EXISTS ingress;
REMOVE TABLE IF EXISTS cluster;
REMOVE TABLE IF EXISTS namespace;
REMOVE TABLE IF EXISTS system;
REMOVE TABLE IF EXISTS k8s_address;
REMOVE TABLE IF EXISTS domain;
REMOVE TABLE IF EXISTS apm_service;
REMOVE TABLE IF EXISTS apm_service_instance;
REMOVE TABLE IF EXISTS datasource;
REMOVE TABLE IF EXISTS bklogconfig;
REMOVE TABLE IF EXISTS biz;
REMOVE TABLE IF EXISTS set;
REMOVE TABLE IF EXISTS module;
REMOVE TABLE IF EXISTS host;
REMOVE TABLE IF EXISTS app_version;
REMOVE TABLE IF EXISTS git_commit;
REMOVE TABLE IF EXISTS environment;
REMOVE TABLE IF EXISTS metric;

-- Drop all relation tables
REMOVE TABLE IF EXISTS node_with_system;
REMOVE TABLE IF EXISTS node_with_pod;
REMOVE TABLE IF EXISTS job_with_pod;
REMOVE TABLE IF EXISTS pod_with_replicaset;
REMOVE TABLE IF EXISTS pod_with_statefulset;
REMOVE TABLE IF EXISTS daemonset_with_pod;
REMOVE TABLE IF EXISTS deployment_with_replicaset;
REMOVE TABLE IF EXISTS pod_with_service;
REMOVE TABLE IF EXISTS ingress_with_service;
REMOVE TABLE IF EXISTS k8s_address_with_service;
REMOVE TABLE IF EXISTS domain_with_service;
REMOVE TABLE IF EXISTS apm_service_instance_with_pod;
REMOVE TABLE IF EXISTS apm_service_instance_with_system;
REMOVE TABLE IF EXISTS apm_service_with_apm_service_instance;
REMOVE TABLE IF EXISTS container_with_pod;
REMOVE TABLE IF EXISTS datasource_with_pod;
REMOVE TABLE IF EXISTS datasource_with_node;
REMOVE TABLE IF EXISTS bklogconfig_with_datasource;
REMOVE TABLE IF EXISTS biz_with_set;
REMOVE TABLE IF EXISTS module_with_set;
REMOVE TABLE IF EXISTS host_with_module;
REMOVE TABLE IF EXISTS host_with_system;
REMOVE TABLE IF EXISTS app_version_with_container;
REMOVE TABLE IF EXISTS app_version_with_system;
REMOVE TABLE IF EXISTS container_with_environment;
REMOVE TABLE IF EXISTS environment_with_system;
REMOVE TABLE IF EXISTS app_version_with_git_commit;
REMOVE TABLE IF EXISTS pod_to_pod;
REMOVE TABLE IF EXISTS pod_to_system;
REMOVE TABLE IF EXISTS system_to_pod;
REMOVE TABLE IF EXISTS system_to_system;
REMOVE TABLE IF EXISTS service_to_service;
REMOVE TABLE IF EXISTS node_has_metric;
REMOVE TABLE IF EXISTS relation_has_metric;

-- ============================================
-- Node Tables (per documentation section 2)
-- ============================================

-- Kubernetes Resources
DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD created_at ON pod TYPE datetime;
DEFINE FIELD updated_at ON pod TYPE datetime;
DEFINE INDEX idx_pod_key ON pod FIELDS bcs_cluster_id, namespace, pod UNIQUE;

DEFINE TABLE node SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON node TYPE string;
DEFINE FIELD node ON node TYPE string;
DEFINE FIELD created_at ON node TYPE datetime;
DEFINE FIELD updated_at ON node TYPE datetime;
DEFINE INDEX idx_node_key ON node FIELDS bcs_cluster_id, node UNIQUE;

DEFINE TABLE container SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON container TYPE string;
DEFINE FIELD namespace ON container TYPE string;
DEFINE FIELD pod ON container TYPE string;
DEFINE FIELD container ON container TYPE string;
DEFINE FIELD created_at ON container TYPE datetime;
DEFINE FIELD updated_at ON container TYPE datetime;
DEFINE INDEX idx_container_key ON container FIELDS bcs_cluster_id, namespace, pod, container UNIQUE;

DEFINE TABLE deployment SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON deployment TYPE string;
DEFINE FIELD namespace ON deployment TYPE string;
DEFINE FIELD deployment ON deployment TYPE string;
DEFINE FIELD created_at ON deployment TYPE datetime;
DEFINE FIELD updated_at ON deployment TYPE datetime;
DEFINE INDEX idx_deployment_key ON deployment FIELDS bcs_cluster_id, namespace, deployment UNIQUE;

DEFINE TABLE replicaset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON replicaset TYPE string;
DEFINE FIELD namespace ON replicaset TYPE string;
DEFINE FIELD replicaset ON replicaset TYPE string;
DEFINE FIELD created_at ON replicaset TYPE datetime;
DEFINE FIELD updated_at ON replicaset TYPE datetime;
DEFINE INDEX idx_replicaset_key ON replicaset FIELDS bcs_cluster_id, namespace, replicaset UNIQUE;

DEFINE TABLE statefulset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON statefulset TYPE string;
DEFINE FIELD namespace ON statefulset TYPE string;
DEFINE FIELD statefulset ON statefulset TYPE string;
DEFINE FIELD created_at ON statefulset TYPE datetime;
DEFINE FIELD updated_at ON statefulset TYPE datetime;
DEFINE INDEX idx_statefulset_key ON statefulset FIELDS bcs_cluster_id, namespace, statefulset UNIQUE;

DEFINE TABLE daemonset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON daemonset TYPE string;
DEFINE FIELD namespace ON daemonset TYPE string;
DEFINE FIELD daemonset ON daemonset TYPE string;
DEFINE FIELD created_at ON daemonset TYPE datetime;
DEFINE FIELD updated_at ON daemonset TYPE datetime;
DEFINE INDEX idx_daemonset_key ON daemonset FIELDS bcs_cluster_id, namespace, daemonset UNIQUE;

DEFINE TABLE job SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON job TYPE string;
DEFINE FIELD namespace ON job TYPE string;
DEFINE FIELD job ON job TYPE string;
DEFINE FIELD created_at ON job TYPE datetime;
DEFINE FIELD updated_at ON job TYPE datetime;
DEFINE INDEX idx_job_key ON job FIELDS bcs_cluster_id, namespace, job UNIQUE;

DEFINE TABLE service SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON service TYPE string;
DEFINE FIELD namespace ON service TYPE string;
DEFINE FIELD service ON service TYPE string;
DEFINE FIELD created_at ON service TYPE datetime;
DEFINE FIELD updated_at ON service TYPE datetime;
DEFINE INDEX idx_service_key ON service FIELDS bcs_cluster_id, namespace, service UNIQUE;

DEFINE TABLE ingress SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON ingress TYPE string;
DEFINE FIELD namespace ON ingress TYPE string;
DEFINE FIELD ingress ON ingress TYPE string;
DEFINE FIELD created_at ON ingress TYPE datetime;
DEFINE FIELD updated_at ON ingress TYPE datetime;
DEFINE INDEX idx_ingress_key ON ingress FIELDS bcs_cluster_id, namespace, ingress UNIQUE;

DEFINE TABLE cluster SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON cluster TYPE string;
DEFINE FIELD created_at ON cluster TYPE datetime;
DEFINE FIELD updated_at ON cluster TYPE datetime;
DEFINE INDEX idx_cluster_key ON cluster FIELDS bcs_cluster_id UNIQUE;

DEFINE TABLE namespace SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON namespace TYPE string;
DEFINE FIELD namespace ON namespace TYPE string;
DEFINE FIELD created_at ON namespace TYPE datetime;
DEFINE FIELD updated_at ON namespace TYPE datetime;
DEFINE INDEX idx_namespace_key ON namespace FIELDS bcs_cluster_id, namespace UNIQUE;

-- Network Resources
DEFINE TABLE system SCHEMAFULL;
DEFINE FIELD bk_cloud_id ON system TYPE string;
DEFINE FIELD bk_target_ip ON system TYPE string;
DEFINE FIELD created_at ON system TYPE datetime;
DEFINE FIELD updated_at ON system TYPE datetime;
DEFINE INDEX idx_system_key ON system FIELDS bk_cloud_id, bk_target_ip UNIQUE;

DEFINE TABLE k8s_address SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON k8s_address TYPE string;
DEFINE FIELD address ON k8s_address TYPE string;
DEFINE FIELD created_at ON k8s_address TYPE datetime;
DEFINE FIELD updated_at ON k8s_address TYPE datetime;
DEFINE INDEX idx_k8s_address_key ON k8s_address FIELDS bcs_cluster_id, address UNIQUE;

DEFINE TABLE domain SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON domain TYPE string;
DEFINE FIELD domain ON domain TYPE string;
DEFINE FIELD created_at ON domain TYPE datetime;
DEFINE FIELD updated_at ON domain TYPE datetime;
DEFINE INDEX idx_domain_key ON domain FIELDS bcs_cluster_id, domain UNIQUE;

-- APM Resources
DEFINE TABLE apm_service SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service TYPE string;
DEFINE FIELD apm_service_name ON apm_service TYPE string;
DEFINE FIELD created_at ON apm_service TYPE datetime;
DEFINE FIELD updated_at ON apm_service TYPE datetime;
DEFINE INDEX idx_apm_service_key ON apm_service FIELDS apm_application_name, apm_service_name UNIQUE;

DEFINE TABLE apm_service_instance SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_instance_name ON apm_service_instance TYPE string;
DEFINE FIELD created_at ON apm_service_instance TYPE datetime;
DEFINE FIELD updated_at ON apm_service_instance TYPE datetime;
DEFINE INDEX idx_apm_service_instance_key ON apm_service_instance FIELDS apm_application_name, apm_service_name, apm_service_instance_name UNIQUE;

-- Data Source Resources
DEFINE TABLE datasource SCHEMAFULL;
DEFINE FIELD bk_data_id ON datasource TYPE string;
DEFINE FIELD created_at ON datasource TYPE datetime;
DEFINE FIELD updated_at ON datasource TYPE datetime;
DEFINE INDEX idx_datasource_key ON datasource FIELDS bk_data_id UNIQUE;

DEFINE TABLE bklogconfig SCHEMAFULL;
DEFINE FIELD bklogconfig_namespace ON bklogconfig TYPE string;
DEFINE FIELD bklogconfig_name ON bklogconfig TYPE string;
DEFINE FIELD created_at ON bklogconfig TYPE datetime;
DEFINE FIELD updated_at ON bklogconfig TYPE datetime;
DEFINE INDEX idx_bklogconfig_key ON bklogconfig FIELDS bklogconfig_namespace, bklogconfig_name UNIQUE;

-- CMDB Resources
DEFINE TABLE biz SCHEMAFULL;
DEFINE FIELD bk_biz_id ON biz TYPE string;
DEFINE FIELD created_at ON biz TYPE datetime;
DEFINE FIELD updated_at ON biz TYPE datetime;
DEFINE INDEX idx_biz_key ON biz FIELDS bk_biz_id UNIQUE;

DEFINE TABLE set SCHEMAFULL;
DEFINE FIELD bk_set_id ON set TYPE string;
DEFINE FIELD created_at ON set TYPE datetime;
DEFINE FIELD updated_at ON set TYPE datetime;
DEFINE INDEX idx_set_key ON set FIELDS bk_set_id UNIQUE;

DEFINE TABLE module SCHEMAFULL;
DEFINE FIELD bk_module_id ON module TYPE string;
DEFINE FIELD created_at ON module TYPE datetime;
DEFINE FIELD updated_at ON module TYPE datetime;
DEFINE INDEX idx_module_key ON module FIELDS bk_module_id UNIQUE;

DEFINE TABLE host SCHEMAFULL;
DEFINE FIELD bk_host_id ON host TYPE string;
DEFINE FIELD created_at ON host TYPE datetime;
DEFINE FIELD updated_at ON host TYPE datetime;
DEFINE INDEX idx_host_key ON host FIELDS bk_host_id UNIQUE;

-- App Version Resources
DEFINE TABLE app_version SCHEMAFULL;
DEFINE FIELD app_name ON app_version TYPE string;
DEFINE FIELD version ON app_version TYPE string;
DEFINE FIELD created_at ON app_version TYPE datetime;
DEFINE FIELD updated_at ON app_version TYPE datetime;
DEFINE INDEX idx_app_version_key ON app_version FIELDS app_name, version UNIQUE;

DEFINE TABLE git_commit SCHEMAFULL;
DEFINE FIELD git_repo ON git_commit TYPE string;
DEFINE FIELD commit_id ON git_commit TYPE string;
DEFINE FIELD created_at ON git_commit TYPE datetime;
DEFINE FIELD updated_at ON git_commit TYPE datetime;
DEFINE INDEX idx_git_commit_key ON git_commit FIELDS git_repo, commit_id UNIQUE;

DEFINE TABLE environment SCHEMAFULL;
DEFINE FIELD environment ON environment TYPE string;
DEFINE FIELD created_at ON environment TYPE datetime;
DEFINE FIELD updated_at ON environment TYPE datetime;
DEFINE INDEX idx_environment_key ON environment FIELDS environment UNIQUE;

-- Metric
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
-- Per documentation section 3
-- ============================================

-- Kubernetes Static Relations (Section 3.2)
DEFINE TABLE node_with_system SCHEMAFULL TYPE RELATION IN node OUT system;
DEFINE FIELD created_at ON node_with_system TYPE datetime;
DEFINE FIELD updated_at ON node_with_system TYPE datetime;

DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION IN node OUT pod;
DEFINE FIELD created_at ON node_with_pod TYPE datetime;
DEFINE FIELD updated_at ON node_with_pod TYPE datetime;

DEFINE TABLE job_with_pod SCHEMAFULL TYPE RELATION IN job OUT pod;
DEFINE FIELD created_at ON job_with_pod TYPE datetime;
DEFINE FIELD updated_at ON job_with_pod TYPE datetime;

DEFINE TABLE pod_with_replicaset SCHEMAFULL TYPE RELATION IN pod OUT replicaset;
DEFINE FIELD created_at ON pod_with_replicaset TYPE datetime;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE datetime;

DEFINE TABLE pod_with_statefulset SCHEMAFULL TYPE RELATION IN pod OUT statefulset;
DEFINE FIELD created_at ON pod_with_statefulset TYPE datetime;
DEFINE FIELD updated_at ON pod_with_statefulset TYPE datetime;

DEFINE TABLE daemonset_with_pod SCHEMAFULL TYPE RELATION IN daemonset OUT pod;
DEFINE FIELD created_at ON daemonset_with_pod TYPE datetime;
DEFINE FIELD updated_at ON daemonset_with_pod TYPE datetime;

DEFINE TABLE deployment_with_replicaset SCHEMAFULL TYPE RELATION IN deployment OUT replicaset;
DEFINE FIELD created_at ON deployment_with_replicaset TYPE datetime;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE datetime;

DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION IN pod OUT service;
DEFINE FIELD created_at ON pod_with_service TYPE datetime;
DEFINE FIELD updated_at ON pod_with_service TYPE datetime;

DEFINE TABLE ingress_with_service SCHEMAFULL TYPE RELATION IN ingress OUT service;
DEFINE FIELD created_at ON ingress_with_service TYPE datetime;
DEFINE FIELD updated_at ON ingress_with_service TYPE datetime;

-- Network Static Relations (Section 3.3)
DEFINE TABLE k8s_address_with_service SCHEMAFULL TYPE RELATION IN k8s_address OUT service;
DEFINE FIELD created_at ON k8s_address_with_service TYPE datetime;
DEFINE FIELD updated_at ON k8s_address_with_service TYPE datetime;

DEFINE TABLE domain_with_service SCHEMAFULL TYPE RELATION IN domain OUT service;
DEFINE FIELD created_at ON domain_with_service TYPE datetime;
DEFINE FIELD updated_at ON domain_with_service TYPE datetime;

-- APM Static Relations (Section 3.4)
DEFINE TABLE apm_service_instance_with_pod SCHEMAFULL TYPE RELATION IN apm_service_instance OUT pod;
DEFINE FIELD created_at ON apm_service_instance_with_pod TYPE datetime;
DEFINE FIELD updated_at ON apm_service_instance_with_pod TYPE datetime;

DEFINE TABLE apm_service_instance_with_system SCHEMAFULL TYPE RELATION IN apm_service_instance OUT system;
DEFINE FIELD created_at ON apm_service_instance_with_system TYPE datetime;
DEFINE FIELD updated_at ON apm_service_instance_with_system TYPE datetime;

DEFINE TABLE apm_service_with_apm_service_instance SCHEMAFULL TYPE RELATION IN apm_service OUT apm_service_instance;
DEFINE FIELD created_at ON apm_service_with_apm_service_instance TYPE datetime;
DEFINE FIELD updated_at ON apm_service_with_apm_service_instance TYPE datetime;

-- Container Static Relations (Section 3.5)
DEFINE TABLE container_with_pod SCHEMAFULL TYPE RELATION IN container OUT pod;
DEFINE FIELD created_at ON container_with_pod TYPE datetime;
DEFINE FIELD updated_at ON container_with_pod TYPE datetime;

-- Data Source Static Relations (Section 3.6)
DEFINE TABLE datasource_with_pod SCHEMAFULL TYPE RELATION IN datasource OUT pod;
DEFINE FIELD created_at ON datasource_with_pod TYPE datetime;
DEFINE FIELD updated_at ON datasource_with_pod TYPE datetime;

DEFINE TABLE datasource_with_node SCHEMAFULL TYPE RELATION IN datasource OUT node;
DEFINE FIELD created_at ON datasource_with_node TYPE datetime;
DEFINE FIELD updated_at ON datasource_with_node TYPE datetime;

DEFINE TABLE bklogconfig_with_datasource SCHEMAFULL TYPE RELATION IN bklogconfig OUT datasource;
DEFINE FIELD created_at ON bklogconfig_with_datasource TYPE datetime;
DEFINE FIELD updated_at ON bklogconfig_with_datasource TYPE datetime;

-- CMDB Static Relations (Section 3.7)
DEFINE TABLE biz_with_set SCHEMAFULL TYPE RELATION IN biz OUT set;
DEFINE FIELD created_at ON biz_with_set TYPE datetime;
DEFINE FIELD updated_at ON biz_with_set TYPE datetime;

DEFINE TABLE module_with_set SCHEMAFULL TYPE RELATION IN module OUT set;
DEFINE FIELD created_at ON module_with_set TYPE datetime;
DEFINE FIELD updated_at ON module_with_set TYPE datetime;

DEFINE TABLE host_with_module SCHEMAFULL TYPE RELATION IN host OUT module;
DEFINE FIELD created_at ON host_with_module TYPE datetime;
DEFINE FIELD updated_at ON host_with_module TYPE datetime;

DEFINE TABLE host_with_system SCHEMAFULL TYPE RELATION IN host OUT system;
DEFINE FIELD created_at ON host_with_system TYPE datetime;
DEFINE FIELD updated_at ON host_with_system TYPE datetime;

-- App Version Static Relations (Section 3.8)
DEFINE TABLE app_version_with_container SCHEMAFULL TYPE RELATION IN app_version OUT container;
DEFINE FIELD created_at ON app_version_with_container TYPE datetime;
DEFINE FIELD updated_at ON app_version_with_container TYPE datetime;

DEFINE TABLE app_version_with_system SCHEMAFULL TYPE RELATION IN app_version OUT system;
DEFINE FIELD created_at ON app_version_with_system TYPE datetime;
DEFINE FIELD updated_at ON app_version_with_system TYPE datetime;

DEFINE TABLE container_with_environment SCHEMAFULL TYPE RELATION IN container OUT environment;
DEFINE FIELD created_at ON container_with_environment TYPE datetime;
DEFINE FIELD updated_at ON container_with_environment TYPE datetime;

DEFINE TABLE environment_with_system SCHEMAFULL TYPE RELATION IN environment OUT system;
DEFINE FIELD created_at ON environment_with_system TYPE datetime;
DEFINE FIELD updated_at ON environment_with_system TYPE datetime;

DEFINE TABLE app_version_with_git_commit SCHEMAFULL TYPE RELATION IN app_version OUT git_commit;
DEFINE FIELD created_at ON app_version_with_git_commit TYPE datetime;
DEFINE FIELD updated_at ON app_version_with_git_commit TYPE datetime;

-- Dynamic Relations (Section 3.9)
DEFINE TABLE pod_to_pod SCHEMAFULL TYPE RELATION IN pod OUT pod;
DEFINE FIELD created_at ON pod_to_pod TYPE datetime;
DEFINE FIELD updated_at ON pod_to_pod TYPE datetime;

DEFINE TABLE pod_to_system SCHEMAFULL TYPE RELATION IN pod OUT system;
DEFINE FIELD created_at ON pod_to_system TYPE datetime;
DEFINE FIELD updated_at ON pod_to_system TYPE datetime;

DEFINE TABLE system_to_pod SCHEMAFULL TYPE RELATION IN system OUT pod;
DEFINE FIELD created_at ON system_to_pod TYPE datetime;
DEFINE FIELD updated_at ON system_to_pod TYPE datetime;

DEFINE TABLE system_to_system SCHEMAFULL TYPE RELATION IN system OUT system;
DEFINE FIELD created_at ON system_to_system TYPE datetime;
DEFINE FIELD updated_at ON system_to_system TYPE datetime;

DEFINE TABLE service_to_service SCHEMAFULL TYPE RELATION IN service OUT service;
DEFINE FIELD created_at ON service_to_service TYPE datetime;
DEFINE FIELD updated_at ON service_to_service TYPE datetime;

-- Metric Relations (Section 7)
DEFINE TABLE node_has_metric SCHEMAFULL TYPE RELATION IN pod OUT metric;
DEFINE FIELD result_table_id ON node_has_metric TYPE string;
DEFINE FIELD created_at ON node_has_metric TYPE datetime;
DEFINE FIELD updated_at ON node_has_metric TYPE datetime;

DEFINE TABLE relation_has_metric SCHEMAFULL TYPE RELATION IN pod_to_pod OUT metric;
DEFINE FIELD result_table_id ON relation_has_metric TYPE string;
DEFINE FIELD created_at ON relation_has_metric TYPE datetime;
DEFINE FIELD updated_at ON relation_has_metric TYPE datetime;
"""
    return node_tables


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
        
        schema_sql = generate_schema_sql()
        statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
        
        for stmt in statements:
            if not stmt or stmt.startswith('--'):
                continue
            try:
                self.execute_sql(stmt + ';')
            except Exception as e:
                logger.warning(f"Schema statement warning: {e}")
        
        logger.info("Database schema initialized")

    def upsert_node(
            self,
            resource_type: ResourceType,
            data: Dict[str, Any],
            created_at: datetime,
            updated_at: datetime
    ) -> Dict[str, Any]:
        """Upsert a node using document-defined ID format"""
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
        logger.info(f"  Batch upserted {len(nodes)} {resource_type.value} nodes")
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
        """Upsert a static (bidirectional) relation"""
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
        """Upsert a dynamic (directional) relation"""
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

class FullMockGenerator:
    """Generate complete mock data for all resource types"""

    def __init__(self, client: SurrealDBClient):
        self.client = client
        self.resources: Dict[ResourceType, List[Dict[str, Any]]] = {}
        self.current_time = MockConfig.END_TIME
        self.traffic_relations: List[Tuple[Dict, Dict, str, RelationType]] = []

    def random_time_in_range(self) -> datetime:
        """Generate random time within configured range"""
        delta = MockConfig.END_TIME - MockConfig.START_TIME
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return MockConfig.START_TIME + timedelta(seconds=random_seconds)

    # =========================================================================
    # CMDB Resources
    # =========================================================================
    
    def create_cmdb_resources(self):
        """Create CMDB resources: Biz, Set, Module, Host"""
        logger.info("Creating CMDB resources...")
        
        # Biz
        biz_data = {"bk_biz_id": MockConfig.BIZ_ID}
        self.client.upsert_node(ResourceType.BIZ, biz_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.BIZ] = [biz_data]
        
        # Set
        set_data = {"bk_set_id": MockConfig.SET_ID}
        self.client.upsert_node(ResourceType.SET, set_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.SET] = [set_data]
        
        # Module
        module_data = {"bk_module_id": MockConfig.MODULE_ID}
        self.client.upsert_node(ResourceType.MODULE, module_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.MODULE] = [module_data]
        
        # Host
        host_data = {"bk_host_id": MockConfig.HOST_ID}
        self.client.upsert_node(ResourceType.HOST, host_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.HOST] = [host_data]
        
        logger.info("  Created Biz, Set, Module, Host")

    def create_cmdb_relations(self):
        """Create CMDB relations"""
        logger.info("Creating CMDB relations...")
        
        biz = self.resources[ResourceType.BIZ][0]
        set_data = self.resources[ResourceType.SET][0]
        module = self.resources[ResourceType.MODULE][0]
        host = self.resources[ResourceType.HOST][0]
        system = self.resources.get(ResourceType.SYSTEM, [{}])[0]
        
        # biz_with_set
        self.client.upsert_static_relation(
            RelationType.BIZ_WITH_SET,
            ResourceType.BIZ, biz,
            ResourceType.SET, set_data,
            self.random_time_in_range(), self.current_time
        )
        
        # module_with_set
        self.client.upsert_static_relation(
            RelationType.MODULE_WITH_SET,
            ResourceType.MODULE, module,
            ResourceType.SET, set_data,
            self.random_time_in_range(), self.current_time
        )
        
        # host_with_module
        self.client.upsert_static_relation(
            RelationType.HOST_WITH_MODULE,
            ResourceType.HOST, host,
            ResourceType.MODULE, module,
            self.random_time_in_range(), self.current_time
        )
        
        # host_with_system
        if system:
            self.client.upsert_static_relation(
                RelationType.HOST_WITH_SYSTEM,
                ResourceType.HOST, host,
                ResourceType.SYSTEM, system,
                self.random_time_in_range(), self.current_time
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
        self.client.upsert_node(ResourceType.CLUSTER, cluster_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.CLUSTER] = [cluster_data]
        
        # Namespace
        ns_data = {"bcs_cluster_id": MockConfig.CLUSTER_ID, "namespace": MockConfig.NAMESPACE}
        self.client.upsert_node(ResourceType.NAMESPACE, ns_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.NAMESPACE] = [ns_data]
        
        # Nodes
        nodes = []
        for i in range(MockConfig.NUM_NODES):
            nodes.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "node": f"{MockConfig.BIZ_NAME}-node-{i}"
            })
        self.client.batch_upsert_nodes(ResourceType.NODE, nodes, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.NODE] = nodes
        
        # Pods
        pods = []
        for i in range(MockConfig.NUM_PODS):
            pods.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "pod": f"{MockConfig.BIZ_NAME}-pod-{i:03d}"
            })
        self.client.batch_upsert_nodes(ResourceType.POD, pods, self.random_time_in_range(), self.current_time)
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
        self.client.batch_upsert_nodes(ResourceType.CONTAINER, containers, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.CONTAINER] = containers
        
        # Services
        services = []
        for svc_name in MockConfig.SERVICE_LIST:
            services.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "service": f"{MockConfig.BIZ_NAME}-{svc_name}"
            })
        self.client.batch_upsert_nodes(ResourceType.SERVICE, services, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.SERVICE] = services
        
        # Deployments
        deployments = []
        for i, svc_name in enumerate(MockConfig.SERVICE_LIST):
            deployments.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "deployment": f"{MockConfig.BIZ_NAME}-{svc_name}-deploy"
            })
        self.client.batch_upsert_nodes(ResourceType.DEPLOYMENT, deployments, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.DEPLOYMENT] = deployments
        
        # ReplicaSets
        replicasets = []
        for deploy in deployments:
            replicasets.append({
                "bcs_cluster_id": MockConfig.CLUSTER_ID,
                "namespace": MockConfig.NAMESPACE,
                "replicaset": f"{deploy['deployment']}-rs-001"
            })
        self.client.batch_upsert_nodes(ResourceType.REPLICASET, replicasets, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.REPLICASET] = replicasets
        
        # StatefulSet
        statefulset_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "statefulset": f"{MockConfig.BIZ_NAME}-statefulset"
        }
        self.client.upsert_node(ResourceType.STATEFULSET, statefulset_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.STATEFULSET] = [statefulset_data]
        
        # DaemonSet
        daemonset_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "daemonset": f"{MockConfig.BIZ_NAME}-daemonset"
        }
        self.client.upsert_node(ResourceType.DAEMONSET, daemonset_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.DAEMONSET] = [daemonset_data]
        
        # Job
        job_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "job": f"{MockConfig.BIZ_NAME}-job"
        }
        self.client.upsert_node(ResourceType.JOB, job_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.JOB] = [job_data]
        
        # Ingress
        ingress_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "namespace": MockConfig.NAMESPACE,
            "ingress": f"{MockConfig.BIZ_NAME}-ingress"
        }
        self.client.upsert_node(ResourceType.INGRESS, ingress_data, self.random_time_in_range(), self.current_time)
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
        for i, pod in enumerate(pods):
            node = nodes[i % len(nodes)]
            self.client.upsert_static_relation(
                RelationType.NODE_WITH_POD,
                ResourceType.NODE, node,
                ResourceType.POD, pod,
                self.random_time_in_range(), self.current_time
            )
        
        # node_with_system
        for node in nodes:
            if systems:
                system = systems[0]
                self.client.upsert_static_relation(
                    RelationType.NODE_WITH_SYSTEM,
                    ResourceType.NODE, node,
                    ResourceType.SYSTEM, system,
                    self.random_time_in_range(), self.current_time
                )
        
        # pod_with_service
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
        
        # deployment_with_replicaset and pod_with_replicaset
        pods_per_rs = len(pods) // len(replicasets)
        for i, (deploy, rs) in enumerate(zip(deployments, replicasets)):
            self.client.upsert_static_relation(
                RelationType.DEPLOYMENT_WITH_REPLICASET,
                ResourceType.DEPLOYMENT, deploy,
                ResourceType.REPLICASET, rs,
                self.random_time_in_range(), self.current_time
            )
            
            start_idx = i * pods_per_rs
            end_idx = start_idx + pods_per_rs if i < len(replicasets) - 1 else len(pods)
            for pod in pods[start_idx:end_idx]:
                self.client.upsert_static_relation(
                    RelationType.POD_WITH_REPLICASET,
                    ResourceType.POD, pod,
                    ResourceType.REPLICASET, rs,
                    self.random_time_in_range(), self.current_time
                )
        
        # container_with_pod
        for container in containers:
            pod_data = {
                "bcs_cluster_id": container["bcs_cluster_id"],
                "namespace": container["namespace"],
                "pod": container["pod"]
            }
            self.client.upsert_static_relation(
                RelationType.CONTAINER_WITH_POD,
                ResourceType.CONTAINER, container,
                ResourceType.POD, pod_data,
                self.random_time_in_range(), self.current_time
            )
        
        # ingress_with_service
        ingress = self.resources.get(ResourceType.INGRESS, [{}])[0]
        if ingress and services:
            self.client.upsert_static_relation(
                RelationType.INGRESS_WITH_SERVICE,
                ResourceType.INGRESS, ingress,
                ResourceType.SERVICE, services[0],
                self.random_time_in_range(), self.current_time
            )
        
        # job_with_pod
        job = self.resources.get(ResourceType.JOB, [{}])[0]
        if job and pods:
            self.client.upsert_static_relation(
                RelationType.JOB_WITH_POD,
                ResourceType.JOB, job,
                ResourceType.POD, pods[0],
                self.random_time_in_range(), self.current_time
            )
        
        # pod_with_statefulset
        statefulset = self.resources.get(ResourceType.STATEFULSET, [{}])[0]
        if statefulset and pods:
            self.client.upsert_static_relation(
                RelationType.POD_WITH_STATEFULSET,
                ResourceType.POD, pods[0],
                ResourceType.STATEFULSET, statefulset,
                self.random_time_in_range(), self.current_time
            )
        
        # daemonset_with_pod
        daemonset = self.resources.get(ResourceType.DAEMONSET, [{}])[0]
        if daemonset and pods:
            self.client.upsert_static_relation(
                RelationType.DAEMONSET_WITH_POD,
                ResourceType.DAEMONSET, daemonset,
                ResourceType.POD, pods[1] if len(pods) > 1 else pods[0],
                self.random_time_in_range(), self.current_time
            )
        
        logger.info("  Created all Kubernetes relations")

    # =========================================================================
    # Network Resources
    # =========================================================================
    
    def create_network_resources(self):
        """Create Network resources: System, K8s Address, Domain"""
        logger.info("Creating Network resources...")
        
        # Systems
        systems = []
        for i in range(MockConfig.NUM_SYSTEMS):
            systems.append({
                "bk_cloud_id": MockConfig.CLOUD_ID,
                "bk_target_ip": f"10.0.0.{i+1}"
            })
        self.client.batch_upsert_nodes(ResourceType.SYSTEM, systems, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.SYSTEM] = systems
        
        # K8s Address
        k8s_address_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "address": "10.0.0.100"
        }
        self.client.upsert_node(ResourceType.K8S_ADDRESS, k8s_address_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.K8S_ADDRESS] = [k8s_address_data]
        
        # Domain
        domain_data = {
            "bcs_cluster_id": MockConfig.CLUSTER_ID,
            "domain": f"{MockConfig.BIZ_NAME}.example.com"
        }
        self.client.upsert_node(ResourceType.DOMAIN, domain_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.DOMAIN] = [domain_data]
        
        logger.info("  Created Network resources")

    def create_network_relations(self):
        """Create Network relations"""
        logger.info("Creating Network relations...")
        
        services = self.resources.get(ResourceType.SERVICE, [])
        k8s_address = self.resources.get(ResourceType.K8S_ADDRESS, [{}])[0]
        domain = self.resources.get(ResourceType.DOMAIN, [{}])[0]
        
        # k8s_address_with_service
        if k8s_address and services:
            self.client.upsert_static_relation(
                RelationType.K8S_ADDRESS_WITH_SERVICE,
                ResourceType.K8S_ADDRESS, k8s_address,
                ResourceType.SERVICE, services[0],
                self.random_time_in_range(), self.current_time
            )
        
        # domain_with_service
        if domain and services:
            self.client.upsert_static_relation(
                RelationType.DOMAIN_WITH_SERVICE,
                ResourceType.DOMAIN, domain,
                ResourceType.SERVICE, services[0],
                self.random_time_in_range(), self.current_time
            )
        
        logger.info("  Created Network relations")

    # =========================================================================
    # APM Resources
    # =========================================================================
    
    def create_apm_resources(self):
        """Create APM resources"""
        logger.info("Creating APM resources...")
        
        # APM Service
        apm_service_data = {
            "apm_application_name": MockConfig.APM_APP_NAME,
            "apm_service_name": MockConfig.APM_SERVICE_NAME
        }
        self.client.upsert_node(ResourceType.APM_SERVICE, apm_service_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.APM_SERVICE] = [apm_service_data]
        
        # APM Service Instances
        apm_instances = []
        for i in range(MockConfig.NUM_APM_INSTANCES):
            apm_instances.append({
                "apm_application_name": MockConfig.APM_APP_NAME,
                "apm_service_name": MockConfig.APM_SERVICE_NAME,
                "apm_service_instance_name": f"{MockConfig.APM_SERVICE_NAME}-instance-{i}"
            })
        self.client.batch_upsert_nodes(ResourceType.APM_SERVICE_INSTANCE, apm_instances, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.APM_SERVICE_INSTANCE] = apm_instances
        
        logger.info("  Created APM resources")

    def create_apm_relations(self):
        """Create APM relations"""
        logger.info("Creating APM relations...")
        
        apm_service = self.resources.get(ResourceType.APM_SERVICE, [{}])[0]
        apm_instances = self.resources.get(ResourceType.APM_SERVICE_INSTANCE, [])
        pods = self.resources.get(ResourceType.POD, [])
        systems = self.resources.get(ResourceType.SYSTEM, [])
        
        for i, instance in enumerate(apm_instances):
            # apm_service_with_apm_service_instance
            self.client.upsert_static_relation(
                RelationType.APM_SERVICE_WITH_APM_SERVICE_INSTANCE,
                ResourceType.APM_SERVICE, apm_service,
                ResourceType.APM_SERVICE_INSTANCE, instance,
                self.random_time_in_range(), self.current_time
            )
            
            # apm_service_instance_with_pod
            if pods:
                self.client.upsert_static_relation(
                    RelationType.APM_SERVICE_INSTANCE_WITH_POD,
                    ResourceType.APM_SERVICE_INSTANCE, instance,
                    ResourceType.POD, pods[i % len(pods)],
                    self.random_time_in_range(), self.current_time
                )
            
            # apm_service_instance_with_system
            if systems:
                self.client.upsert_static_relation(
                    RelationType.APM_SERVICE_INSTANCE_WITH_SYSTEM,
                    ResourceType.APM_SERVICE_INSTANCE, instance,
                    ResourceType.SYSTEM, systems[i % len(systems)],
                    self.random_time_in_range(), self.current_time
                )
        
        logger.info("  Created APM relations")

    # =========================================================================
    # Data Source Resources
    # =========================================================================
    
    def create_datasource_resources(self):
        """Create Data Source resources"""
        logger.info("Creating Data Source resources...")
        
        # DataSource
        datasource_data = {"bk_data_id": MockConfig.DATA_ID}
        self.client.upsert_node(ResourceType.DATASOURCE, datasource_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.DATASOURCE] = [datasource_data]
        
        # BKLogConfig
        bklogconfig_data = {
            "bklogconfig_namespace": MockConfig.NAMESPACE,
            "bklogconfig_name": f"{MockConfig.BIZ_NAME}-logconfig"
        }
        self.client.upsert_node(ResourceType.BKLOGCONFIG, bklogconfig_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.BKLOGCONFIG] = [bklogconfig_data]
        
        logger.info("  Created Data Source resources")

    def create_datasource_relations(self):
        """Create Data Source relations"""
        logger.info("Creating Data Source relations...")
        
        datasource = self.resources.get(ResourceType.DATASOURCE, [{}])[0]
        bklogconfig = self.resources.get(ResourceType.BKLOGCONFIG, [{}])[0]
        pods = self.resources.get(ResourceType.POD, [])
        nodes = self.resources.get(ResourceType.NODE, [])
        
        # datasource_with_pod
        if datasource and pods:
            self.client.upsert_static_relation(
                RelationType.DATASOURCE_WITH_POD,
                ResourceType.DATASOURCE, datasource,
                ResourceType.POD, pods[0],
                self.random_time_in_range(), self.current_time
            )
        
        # datasource_with_node
        if datasource and nodes:
            self.client.upsert_static_relation(
                RelationType.DATASOURCE_WITH_NODE,
                ResourceType.DATASOURCE, datasource,
                ResourceType.NODE, nodes[0],
                self.random_time_in_range(), self.current_time
            )
        
        # bklogconfig_with_datasource
        if bklogconfig and datasource:
            self.client.upsert_static_relation(
                RelationType.BKLOGCONFIG_WITH_DATASOURCE,
                ResourceType.BKLOGCONFIG, bklogconfig,
                ResourceType.DATASOURCE, datasource,
                self.random_time_in_range(), self.current_time
            )
        
        logger.info("  Created Data Source relations")

    # =========================================================================
    # App Version Resources
    # =========================================================================
    
    def create_app_version_resources(self):
        """Create App Version resources"""
        logger.info("Creating App Version resources...")
        
        # App Version
        app_version_data = {
            "app_name": MockConfig.APP_NAME,
            "version": MockConfig.VERSION
        }
        self.client.upsert_node(ResourceType.APP_VERSION, app_version_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.APP_VERSION] = [app_version_data]
        
        # Git Commit
        git_commit_data = {
            "git_repo": MockConfig.GIT_REPO,
            "commit_id": MockConfig.COMMIT_ID
        }
        self.client.upsert_node(ResourceType.GIT_COMMIT, git_commit_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.GIT_COMMIT] = [git_commit_data]
        
        # Environment
        env_data = {"environment": MockConfig.ENVIRONMENT}
        self.client.upsert_node(ResourceType.ENVIRONMENT, env_data, self.random_time_in_range(), self.current_time)
        self.resources[ResourceType.ENVIRONMENT] = [env_data]
        
        logger.info("  Created App Version resources")

    def create_app_version_relations(self):
        """Create App Version relations"""
        logger.info("Creating App Version relations...")
        
        app_version = self.resources.get(ResourceType.APP_VERSION, [{}])[0]
        git_commit = self.resources.get(ResourceType.GIT_COMMIT, [{}])[0]
        env = self.resources.get(ResourceType.ENVIRONMENT, [{}])[0]
        containers = self.resources.get(ResourceType.CONTAINER, [])
        systems = self.resources.get(ResourceType.SYSTEM, [])
        
        # app_version_with_git_commit
        if app_version and git_commit:
            self.client.upsert_static_relation(
                RelationType.APP_VERSION_WITH_GIT_COMMIT,
                ResourceType.APP_VERSION, app_version,
                ResourceType.GIT_COMMIT, git_commit,
                self.random_time_in_range(), self.current_time
            )
        
        # app_version_with_container
        if app_version and containers:
            self.client.upsert_static_relation(
                RelationType.APP_VERSION_WITH_CONTAINER,
                ResourceType.APP_VERSION, app_version,
                ResourceType.CONTAINER, containers[0],
                self.random_time_in_range(), self.current_time
            )
        
        # app_version_with_system
        if app_version and systems:
            self.client.upsert_static_relation(
                RelationType.APP_VERSION_WITH_SYSTEM,
                ResourceType.APP_VERSION, app_version,
                ResourceType.SYSTEM, systems[0],
                self.random_time_in_range(), self.current_time
            )
        
        # container_with_environment
        if containers and env:
            self.client.upsert_static_relation(
                RelationType.CONTAINER_WITH_ENVIRONMENT,
                ResourceType.CONTAINER, containers[0],
                ResourceType.ENVIRONMENT, env,
                self.random_time_in_range(), self.current_time
            )
        
        # environment_with_system
        if env and systems:
            self.client.upsert_static_relation(
                RelationType.ENVIRONMENT_WITH_SYSTEM,
                ResourceType.ENVIRONMENT, env,
                ResourceType.SYSTEM, systems[0],
                self.random_time_in_range(), self.current_time
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
                        RelationType.POD_TO_POD, ResourceType.POD, source_pod, ResourceType.POD, target_pod
                    )
                    self.traffic_relations.append((source_pod, target_pod, relation_id, RelationType.POD_TO_POD))
                    count += 1
        logger.info(f"    Created {count} pod_to_pod relations")
        
        # pod_to_system
        count = 0
        for pod in pods:
            if random.random() < MockConfig.POD_TO_SYSTEM_TRAFFIC_PROBABILITY:
                if systems:
                    target_system = random.choice(systems)
                    self.client.upsert_dynamic_relation(
                        RelationType.POD_TO_SYSTEM,
                        ResourceType.POD, pod,
                        ResourceType.SYSTEM, target_system,
                        self.random_time_in_range(), self.current_time
                    )
                    count += 1
        logger.info(f"    Created {count} pod_to_system relations")
        
        # system_to_pod
        count = 0
        for system in systems:
            if random.random() < MockConfig.POD_TO_SYSTEM_TRAFFIC_PROBABILITY:
                if pods:
                    target_pod = random.choice(pods)
                    self.client.upsert_dynamic_relation(
                        RelationType.SYSTEM_TO_POD,
                        ResourceType.SYSTEM, system,
                        ResourceType.POD, target_pod,
                        self.random_time_in_range(), self.current_time
                    )
                    count += 1
        logger.info(f"    Created {count} system_to_pod relations")
        
        # system_to_system
        count = 0
        for source_system in systems:
            if random.random() < MockConfig.POD_TO_SYSTEM_TRAFFIC_PROBABILITY:
                target_candidates = [s for s in systems if s != source_system]
                if target_candidates:
                    target_system = random.choice(target_candidates)
                    self.client.upsert_dynamic_relation(
                        RelationType.SYSTEM_TO_SYSTEM,
                        ResourceType.SYSTEM, source_system,
                        ResourceType.SYSTEM, target_system,
                        self.random_time_in_range(), self.current_time
                    )
                    count += 1
        logger.info(f"    Created {count} system_to_system relations")
        
        # service_to_service
        count = 0
        for source_service in services:
            if random.random() < MockConfig.SERVICE_TO_SERVICE_TRAFFIC_PROBABILITY:
                target_candidates = [s for s in services if s != source_service]
                if target_candidates:
                    target_service = random.choice(target_candidates)
                    self.client.upsert_dynamic_relation(
                        RelationType.SERVICE_TO_SERVICE,
                        ResourceType.SERVICE, source_service,
                        ResourceType.SERVICE, target_service,
                        self.random_time_in_range(), self.current_time
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
            {"metric_name": "pod_to_pod_flow_total", "metric_type": "counter", "unit": "count", "description": "Pod到Pod的流量访问量"},
            {"metric_name": "pod_to_pod_flow_seconds", "metric_type": "gauge", "unit": "seconds", "description": "Pod到Pod的流量访问耗时"},
            {"metric_name": "pod_to_pod_flow_error", "metric_type": "counter", "unit": "count", "description": "Pod到Pod的流量错误数"},
            {"metric_name": "cpu_usage", "metric_type": "gauge", "unit": "percent", "description": "CPU使用率"},
            {"metric_name": "memory_usage", "metric_type": "gauge", "unit": "bytes", "description": "内存使用量"},
        ]
        
        for metric_data in metrics:
            self.client.upsert_node(ResourceType.METRIC, metric_data, self.random_time_in_range(), self.current_time)
        
        self.resources[ResourceType.METRIC] = metrics
        logger.info(f"  Created {len(metrics)} metric definitions")
        
        # Create relation_has_metric associations
        count = 0
        for source, target, relation_id, rel_type in self.traffic_relations:
            if rel_type == RelationType.POD_TO_POD:
                for metric_data in metrics[:3]:  # Only pod_to_pod metrics
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
                        logger.warning(f"  Failed to create relation_has_metric: {e}")
        
        logger.info(f"  Created {count} relation_has_metric associations")

    # =========================================================================
    # Main Generation
    # =========================================================================
    
    def generate_all(self):
        """Generate all mock data"""
        logger.info("\n" + "=" * 70)
        logger.info("Starting Full Resource Graph Mock Data Generation")
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
        
        # Count by category
        k8s_count = sum(len(self.resources.get(rt, [])) for rt in [
            ResourceType.POD, ResourceType.NODE, ResourceType.CONTAINER,
            ResourceType.DEPLOYMENT, ResourceType.REPLICASET, ResourceType.STATEFULSET,
            ResourceType.DAEMONSET, ResourceType.JOB, ResourceType.SERVICE,
            ResourceType.INGRESS, ResourceType.CLUSTER, ResourceType.NAMESPACE
        ])
        network_count = sum(len(self.resources.get(rt, [])) for rt in [
            ResourceType.SYSTEM, ResourceType.K8S_ADDRESS, ResourceType.DOMAIN
        ])
        apm_count = sum(len(self.resources.get(rt, [])) for rt in [
            ResourceType.APM_SERVICE, ResourceType.APM_SERVICE_INSTANCE
        ])
        cmdb_count = sum(len(self.resources.get(rt, [])) for rt in [
            ResourceType.BIZ, ResourceType.SET, ResourceType.MODULE, ResourceType.HOST
        ])
        
        logger.info("  Category Totals:")
        logger.info(f"    Kubernetes resources: {k8s_count}")
        logger.info(f"    Network resources: {network_count}")
        logger.info(f"    APM resources: {apm_count}")
        logger.info(f"    CMDB resources: {cmdb_count}")
        logger.info("-" * 70)


# ============================================================================
# Main Function
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Mock Full Resource Graph to SurrealDB')
    parser.add_argument('--backend', type=str, default='native', choices=['native', 'bkbase'])
    parser.add_argument('--init-schema', action='store_true', help='Initialize database schema (drops existing tables)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=" * 70)
    logger.info(" Mock Full Resource Graph to SurrealDB")
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
