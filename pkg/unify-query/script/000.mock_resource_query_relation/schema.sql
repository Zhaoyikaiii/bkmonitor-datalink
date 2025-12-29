-- ============================================================================
-- SurrealDB Schema for Resource Graph
-- 
-- This schema defines all resource types and relations for the BK Monitor
-- resource topology graph. It follows the design document specifications.
--
-- Time Fields (per document section 2.0):
--   - created_at: Unix timestamp in milliseconds, part of node ID
--   - updated_at: Unix timestamp in milliseconds, updated on each report
--
-- Note: Liveness detection is handled by application logic using:
--   updated_at >= query_time - tolerance_time
--
-- Author: Auto-generated for BK Monitor
-- ============================================================================

-- ============================================================================
-- SECTION 1: Drop Existing Tables (for clean reset)
-- ============================================================================

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

-- ============================================================================
-- SECTION 2: Node Tables (Resource Types)
-- 
-- Each resource has:
--   - Index fields for unique identification (per design doc section 2)
--   - created_at: Unix timestamp in milliseconds (part of ID per section 5.1)
--   - updated_at: Unix timestamp in milliseconds (updated on each report)
--
-- Note: SurrealDB schema does NOT enforce UNIQUE on index fields because
--       the same resource can have multiple records with different created_at
--       (representing different lifecycle stages per section 2.0.2)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 2.1 Kubernetes Resources
-- ----------------------------------------------------------------------------

-- Pod: Smallest deployable unit in Kubernetes
-- Index: bcs_cluster_id, namespace, pod
DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD created_at ON pod TYPE int;
DEFINE FIELD updated_at ON pod TYPE int;

-- Node: Worker machine in Kubernetes cluster
-- Index: bcs_cluster_id, node
DEFINE TABLE node SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON node TYPE string;
DEFINE FIELD node ON node TYPE string;
DEFINE FIELD created_at ON node TYPE int;
DEFINE FIELD updated_at ON node TYPE int;

-- Container: Running instance within a Pod
-- Index: bcs_cluster_id, namespace, pod, container
DEFINE TABLE container SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON container TYPE string;
DEFINE FIELD namespace ON container TYPE string;
DEFINE FIELD pod ON container TYPE string;
DEFINE FIELD container ON container TYPE string;
DEFINE FIELD created_at ON container TYPE int;
DEFINE FIELD updated_at ON container TYPE int;

-- Deployment: Declarative updates for Pods and ReplicaSets
-- Index: bcs_cluster_id, namespace, deployment
DEFINE TABLE deployment SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON deployment TYPE string;
DEFINE FIELD namespace ON deployment TYPE string;
DEFINE FIELD deployment ON deployment TYPE string;
DEFINE FIELD created_at ON deployment TYPE int;
DEFINE FIELD updated_at ON deployment TYPE int;

-- ReplicaSet: Maintains a stable set of replica Pods
-- Index: bcs_cluster_id, namespace, replicaset
DEFINE TABLE replicaset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON replicaset TYPE string;
DEFINE FIELD namespace ON replicaset TYPE string;
DEFINE FIELD replicaset ON replicaset TYPE string;
DEFINE FIELD created_at ON replicaset TYPE int;
DEFINE FIELD updated_at ON replicaset TYPE int;

-- StatefulSet: Manages stateful applications
-- Index: bcs_cluster_id, namespace, statefulset
DEFINE TABLE statefulset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON statefulset TYPE string;
DEFINE FIELD namespace ON statefulset TYPE string;
DEFINE FIELD statefulset ON statefulset TYPE string;
DEFINE FIELD created_at ON statefulset TYPE int;
DEFINE FIELD updated_at ON statefulset TYPE int;

-- DaemonSet: Ensures all nodes run a copy of a Pod
-- Index: bcs_cluster_id, namespace, daemonset
DEFINE TABLE daemonset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON daemonset TYPE string;
DEFINE FIELD namespace ON daemonset TYPE string;
DEFINE FIELD daemonset ON daemonset TYPE string;
DEFINE FIELD created_at ON daemonset TYPE int;
DEFINE FIELD updated_at ON daemonset TYPE int;

-- Job: Creates Pods that run to completion
-- Index: bcs_cluster_id, namespace, job
DEFINE TABLE job SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON job TYPE string;
DEFINE FIELD namespace ON job TYPE string;
DEFINE FIELD job ON job TYPE string;
DEFINE FIELD created_at ON job TYPE int;
DEFINE FIELD updated_at ON job TYPE int;

-- Service: Abstract way to expose an application running on Pods
-- Index: bcs_cluster_id, namespace, service
DEFINE TABLE service SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON service TYPE string;
DEFINE FIELD namespace ON service TYPE string;
DEFINE FIELD service ON service TYPE string;
DEFINE FIELD created_at ON service TYPE int;
DEFINE FIELD updated_at ON service TYPE int;

-- Ingress: Manages external access to services
-- Index: bcs_cluster_id, namespace, ingress
DEFINE TABLE ingress SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON ingress TYPE string;
DEFINE FIELD namespace ON ingress TYPE string;
DEFINE FIELD ingress ON ingress TYPE string;
DEFINE FIELD created_at ON ingress TYPE int;
DEFINE FIELD updated_at ON ingress TYPE int;

-- Cluster: Kubernetes cluster
-- Index: bcs_cluster_id
DEFINE TABLE cluster SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON cluster TYPE string;
DEFINE FIELD created_at ON cluster TYPE int;
DEFINE FIELD updated_at ON cluster TYPE int;

-- Namespace: Virtual cluster within a physical cluster
-- Index: bcs_cluster_id, namespace
DEFINE TABLE namespace SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON namespace TYPE string;
DEFINE FIELD namespace ON namespace TYPE string;
DEFINE FIELD created_at ON namespace TYPE int;
DEFINE FIELD updated_at ON namespace TYPE int;

-- ----------------------------------------------------------------------------
-- 2.2 Network Resources
-- ----------------------------------------------------------------------------

-- System: Physical or virtual machine identified by IP
-- Index: bk_cloud_id, bk_target_ip
DEFINE TABLE system SCHEMAFULL;
DEFINE FIELD bk_cloud_id ON system TYPE string;
DEFINE FIELD bk_target_ip ON system TYPE string;
DEFINE FIELD created_at ON system TYPE int;
DEFINE FIELD updated_at ON system TYPE int;

-- K8s Address: ClusterIP or endpoint address
-- Index: bcs_cluster_id, address
DEFINE TABLE k8s_address SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON k8s_address TYPE string;
DEFINE FIELD address ON k8s_address TYPE string;
DEFINE FIELD created_at ON k8s_address TYPE int;
DEFINE FIELD updated_at ON k8s_address TYPE int;

-- Domain: DNS domain name
-- Index: bcs_cluster_id, domain
DEFINE TABLE domain SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON domain TYPE string;
DEFINE FIELD domain ON domain TYPE string;
DEFINE FIELD created_at ON domain TYPE int;
DEFINE FIELD updated_at ON domain TYPE int;

-- ----------------------------------------------------------------------------
-- 2.3 APM Resources
-- ----------------------------------------------------------------------------

-- APM Service: Application performance monitoring service
-- Index: apm_application_name, apm_service_name
DEFINE TABLE apm_service SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service TYPE string;
DEFINE FIELD apm_service_name ON apm_service TYPE string;
DEFINE FIELD created_at ON apm_service TYPE int;
DEFINE FIELD updated_at ON apm_service TYPE int;

-- APM Service Instance: Instance of an APM service
-- Index: apm_application_name, apm_service_name, apm_service_instance_name
DEFINE TABLE apm_service_instance SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_instance_name ON apm_service_instance TYPE string;
DEFINE FIELD created_at ON apm_service_instance TYPE int;
DEFINE FIELD updated_at ON apm_service_instance TYPE int;

-- ----------------------------------------------------------------------------
-- 2.4 Data Source Resources
-- ----------------------------------------------------------------------------

-- DataSource: BK Monitor data source
-- Index: bk_data_id
DEFINE TABLE datasource SCHEMAFULL;
DEFINE FIELD bk_data_id ON datasource TYPE string;
DEFINE FIELD created_at ON datasource TYPE int;
DEFINE FIELD updated_at ON datasource TYPE int;

-- BKLogConfig: Log collection configuration
-- Index: bklogconfig_namespace, bklogconfig_name
DEFINE TABLE bklogconfig SCHEMAFULL;
DEFINE FIELD bklogconfig_namespace ON bklogconfig TYPE string;
DEFINE FIELD bklogconfig_name ON bklogconfig TYPE string;
DEFINE FIELD created_at ON bklogconfig TYPE int;
DEFINE FIELD updated_at ON bklogconfig TYPE int;

-- ----------------------------------------------------------------------------
-- 2.5 CMDB Resources
-- ----------------------------------------------------------------------------

-- Biz: Business unit in CMDB
-- Index: bk_biz_id
DEFINE TABLE biz SCHEMAFULL;
DEFINE FIELD bk_biz_id ON biz TYPE string;
DEFINE FIELD created_at ON biz TYPE int;
DEFINE FIELD updated_at ON biz TYPE int;

-- Set: Set within a business
-- Index: bk_set_id
DEFINE TABLE set SCHEMAFULL;
DEFINE FIELD bk_set_id ON set TYPE string;
DEFINE FIELD created_at ON set TYPE int;
DEFINE FIELD updated_at ON set TYPE int;

-- Module: Module within a set
-- Index: bk_module_id
DEFINE TABLE module SCHEMAFULL;
DEFINE FIELD bk_module_id ON module TYPE string;
DEFINE FIELD created_at ON module TYPE int;
DEFINE FIELD updated_at ON module TYPE int;

-- Host: Physical or virtual host in CMDB
-- Index: bk_host_id
DEFINE TABLE host SCHEMAFULL;
DEFINE FIELD bk_host_id ON host TYPE string;
DEFINE FIELD created_at ON host TYPE int;
DEFINE FIELD updated_at ON host TYPE int;

-- ----------------------------------------------------------------------------
-- 2.6 App Version Resources
-- ----------------------------------------------------------------------------

-- App Version: Application version tracking
-- Index: app_name, version
DEFINE TABLE app_version SCHEMAFULL;
DEFINE FIELD app_name ON app_version TYPE string;
DEFINE FIELD version ON app_version TYPE string;
DEFINE FIELD created_at ON app_version TYPE int;
DEFINE FIELD updated_at ON app_version TYPE int;

-- Git Commit: Source code commit information
-- Index: git_repo, commit_id
DEFINE TABLE git_commit SCHEMAFULL;
DEFINE FIELD git_repo ON git_commit TYPE string;
DEFINE FIELD commit_id ON git_commit TYPE string;
DEFINE FIELD created_at ON git_commit TYPE int;
DEFINE FIELD updated_at ON git_commit TYPE int;

-- Environment: Deployment environment (production, staging, etc.)
-- Index: environment
DEFINE TABLE environment SCHEMAFULL;
DEFINE FIELD environment ON environment TYPE string;
DEFINE FIELD created_at ON environment TYPE int;
DEFINE FIELD updated_at ON environment TYPE int;

-- ----------------------------------------------------------------------------
-- 2.7 Metric Resource
-- ----------------------------------------------------------------------------

-- Metric: Metric definition
-- Index: metric_name
DEFINE TABLE metric SCHEMAFULL;
DEFINE FIELD metric_name ON metric TYPE string;
DEFINE FIELD metric_type ON metric TYPE string;
DEFINE FIELD unit ON metric TYPE string;
DEFINE FIELD description ON metric TYPE string;
DEFINE FIELD created_at ON metric TYPE int;
DEFINE FIELD updated_at ON metric TYPE int;

-- ============================================================================
-- SECTION 3: Relation Tables (TYPE RELATION for graph traversal)
--
-- Naming convention:
--   - Static relations: {res1}_with_{res2} (bidirectional, res1 < res2 alphabetically)
--   - Dynamic relations: {src}_to_{dst} (directional, for traffic flow)
--
-- All relations have:
--   - created_at: Unix timestamp in milliseconds
--   - updated_at: Unix timestamp in milliseconds
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 3.1 Kubernetes Static Relations
-- ----------------------------------------------------------------------------

-- Node contains System (physical machine)
DEFINE TABLE node_with_system SCHEMAFULL TYPE RELATION IN node OUT system;
DEFINE FIELD created_at ON node_with_system TYPE int;
DEFINE FIELD updated_at ON node_with_system TYPE int;

-- Node runs Pods
DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION IN node OUT pod;
DEFINE FIELD created_at ON node_with_pod TYPE int;
DEFINE FIELD updated_at ON node_with_pod TYPE int;

-- Job creates Pods
DEFINE TABLE job_with_pod SCHEMAFULL TYPE RELATION IN job OUT pod;
DEFINE FIELD created_at ON job_with_pod TYPE int;
DEFINE FIELD updated_at ON job_with_pod TYPE int;

-- Pod belongs to ReplicaSet
DEFINE TABLE pod_with_replicaset SCHEMAFULL TYPE RELATION IN pod OUT replicaset;
DEFINE FIELD created_at ON pod_with_replicaset TYPE int;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE int;

-- Pod belongs to StatefulSet
DEFINE TABLE pod_with_statefulset SCHEMAFULL TYPE RELATION IN pod OUT statefulset;
DEFINE FIELD created_at ON pod_with_statefulset TYPE int;
DEFINE FIELD updated_at ON pod_with_statefulset TYPE int;

-- DaemonSet manages Pods
DEFINE TABLE daemonset_with_pod SCHEMAFULL TYPE RELATION IN daemonset OUT pod;
DEFINE FIELD created_at ON daemonset_with_pod TYPE int;
DEFINE FIELD updated_at ON daemonset_with_pod TYPE int;

-- Deployment manages ReplicaSets
DEFINE TABLE deployment_with_replicaset SCHEMAFULL TYPE RELATION IN deployment OUT replicaset;
DEFINE FIELD created_at ON deployment_with_replicaset TYPE int;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE int;

-- Pod exposes through Service
DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION IN pod OUT service;
DEFINE FIELD created_at ON pod_with_service TYPE int;
DEFINE FIELD updated_at ON pod_with_service TYPE int;

-- Ingress routes to Service
DEFINE TABLE ingress_with_service SCHEMAFULL TYPE RELATION IN ingress OUT service;
DEFINE FIELD created_at ON ingress_with_service TYPE int;
DEFINE FIELD updated_at ON ingress_with_service TYPE int;

-- ----------------------------------------------------------------------------
-- 3.2 Network Static Relations
-- ----------------------------------------------------------------------------

-- K8s Address points to Service
DEFINE TABLE k8s_address_with_service SCHEMAFULL TYPE RELATION IN k8s_address OUT service;
DEFINE FIELD created_at ON k8s_address_with_service TYPE int;
DEFINE FIELD updated_at ON k8s_address_with_service TYPE int;

-- Domain resolves to Service
DEFINE TABLE domain_with_service SCHEMAFULL TYPE RELATION IN domain OUT service;
DEFINE FIELD created_at ON domain_with_service TYPE int;
DEFINE FIELD updated_at ON domain_with_service TYPE int;

-- ----------------------------------------------------------------------------
-- 3.3 APM Static Relations
-- ----------------------------------------------------------------------------

-- APM Service Instance runs on Pod
DEFINE TABLE apm_service_instance_with_pod SCHEMAFULL TYPE RELATION IN apm_service_instance OUT pod;
DEFINE FIELD created_at ON apm_service_instance_with_pod TYPE int;
DEFINE FIELD updated_at ON apm_service_instance_with_pod TYPE int;

-- APM Service Instance runs on System
DEFINE TABLE apm_service_instance_with_system SCHEMAFULL TYPE RELATION IN apm_service_instance OUT system;
DEFINE FIELD created_at ON apm_service_instance_with_system TYPE int;
DEFINE FIELD updated_at ON apm_service_instance_with_system TYPE int;

-- APM Service has Instances
DEFINE TABLE apm_service_with_apm_service_instance SCHEMAFULL TYPE RELATION IN apm_service OUT apm_service_instance;
DEFINE FIELD created_at ON apm_service_with_apm_service_instance TYPE int;
DEFINE FIELD updated_at ON apm_service_with_apm_service_instance TYPE int;

-- ----------------------------------------------------------------------------
-- 3.4 Container Static Relations
-- ----------------------------------------------------------------------------

-- Container runs in Pod
DEFINE TABLE container_with_pod SCHEMAFULL TYPE RELATION IN container OUT pod;
DEFINE FIELD created_at ON container_with_pod TYPE int;
DEFINE FIELD updated_at ON container_with_pod TYPE int;

-- ----------------------------------------------------------------------------
-- 3.5 Data Source Static Relations
-- ----------------------------------------------------------------------------

-- DataSource collects from Pod
DEFINE TABLE datasource_with_pod SCHEMAFULL TYPE RELATION IN datasource OUT pod;
DEFINE FIELD created_at ON datasource_with_pod TYPE int;
DEFINE FIELD updated_at ON datasource_with_pod TYPE int;

-- DataSource collects from Node
DEFINE TABLE datasource_with_node SCHEMAFULL TYPE RELATION IN datasource OUT node;
DEFINE FIELD created_at ON datasource_with_node TYPE int;
DEFINE FIELD updated_at ON datasource_with_node TYPE int;

-- BKLogConfig uses DataSource
DEFINE TABLE bklogconfig_with_datasource SCHEMAFULL TYPE RELATION IN bklogconfig OUT datasource;
DEFINE FIELD created_at ON bklogconfig_with_datasource TYPE int;
DEFINE FIELD updated_at ON bklogconfig_with_datasource TYPE int;

-- ----------------------------------------------------------------------------
-- 3.6 CMDB Static Relations
-- ----------------------------------------------------------------------------

-- Biz contains Sets
DEFINE TABLE biz_with_set SCHEMAFULL TYPE RELATION IN biz OUT set;
DEFINE FIELD created_at ON biz_with_set TYPE int;
DEFINE FIELD updated_at ON biz_with_set TYPE int;

-- Module belongs to Set
DEFINE TABLE module_with_set SCHEMAFULL TYPE RELATION IN module OUT set;
DEFINE FIELD created_at ON module_with_set TYPE int;
DEFINE FIELD updated_at ON module_with_set TYPE int;

-- Host belongs to Module
DEFINE TABLE host_with_module SCHEMAFULL TYPE RELATION IN host OUT module;
DEFINE FIELD created_at ON host_with_module TYPE int;
DEFINE FIELD updated_at ON host_with_module TYPE int;

-- Host has System (IP)
DEFINE TABLE host_with_system SCHEMAFULL TYPE RELATION IN host OUT system;
DEFINE FIELD created_at ON host_with_system TYPE int;
DEFINE FIELD updated_at ON host_with_system TYPE int;

-- ----------------------------------------------------------------------------
-- 3.7 App Version Static Relations
-- ----------------------------------------------------------------------------

-- App Version deployed to Container
DEFINE TABLE app_version_with_container SCHEMAFULL TYPE RELATION IN app_version OUT container;
DEFINE FIELD created_at ON app_version_with_container TYPE int;
DEFINE FIELD updated_at ON app_version_with_container TYPE int;

-- App Version deployed to System
DEFINE TABLE app_version_with_system SCHEMAFULL TYPE RELATION IN app_version OUT system;
DEFINE FIELD created_at ON app_version_with_system TYPE int;
DEFINE FIELD updated_at ON app_version_with_system TYPE int;

-- Container runs in Environment
DEFINE TABLE container_with_environment SCHEMAFULL TYPE RELATION IN container OUT environment;
DEFINE FIELD created_at ON container_with_environment TYPE int;
DEFINE FIELD updated_at ON container_with_environment TYPE int;

-- Environment has System
DEFINE TABLE environment_with_system SCHEMAFULL TYPE RELATION IN environment OUT system;
DEFINE FIELD created_at ON environment_with_system TYPE int;
DEFINE FIELD updated_at ON environment_with_system TYPE int;

-- App Version comes from Git Commit
DEFINE TABLE app_version_with_git_commit SCHEMAFULL TYPE RELATION IN app_version OUT git_commit;
DEFINE FIELD created_at ON app_version_with_git_commit TYPE int;
DEFINE FIELD updated_at ON app_version_with_git_commit TYPE int;

-- ----------------------------------------------------------------------------
-- 3.8 Dynamic Traffic Relations (Directional)
--
-- These represent actual network traffic flow between resources.
-- Direction matters: source -> target indicates traffic direction.
-- ----------------------------------------------------------------------------

-- Pod to Pod traffic
DEFINE TABLE pod_to_pod SCHEMAFULL TYPE RELATION IN pod OUT pod;
DEFINE FIELD created_at ON pod_to_pod TYPE int;
DEFINE FIELD updated_at ON pod_to_pod TYPE int;

-- Pod to external System traffic
DEFINE TABLE pod_to_system SCHEMAFULL TYPE RELATION IN pod OUT system;
DEFINE FIELD created_at ON pod_to_system TYPE int;
DEFINE FIELD updated_at ON pod_to_system TYPE int;

-- External System to Pod traffic
DEFINE TABLE system_to_pod SCHEMAFULL TYPE RELATION IN system OUT pod;
DEFINE FIELD created_at ON system_to_pod TYPE int;
DEFINE FIELD updated_at ON system_to_pod TYPE int;

-- System to System traffic
DEFINE TABLE system_to_system SCHEMAFULL TYPE RELATION IN system OUT system;
DEFINE FIELD created_at ON system_to_system TYPE int;
DEFINE FIELD updated_at ON system_to_system TYPE int;

-- Service to Service traffic (aggregated from Pod traffic)
DEFINE TABLE service_to_service SCHEMAFULL TYPE RELATION IN service OUT service;
DEFINE FIELD created_at ON service_to_service TYPE int;
DEFINE FIELD updated_at ON service_to_service TYPE int;

-- ----------------------------------------------------------------------------
-- 3.9 Metric Relations
--
-- These connect resources/relations to their associated metrics.
-- ----------------------------------------------------------------------------

-- Node has Metric (for node-level metrics)
DEFINE TABLE node_has_metric SCHEMAFULL TYPE RELATION IN pod OUT metric;
DEFINE FIELD result_table_id ON node_has_metric TYPE string;
DEFINE FIELD created_at ON node_has_metric TYPE int;
DEFINE FIELD updated_at ON node_has_metric TYPE int;

-- Traffic Relation has Metric (for traffic metrics)
DEFINE TABLE relation_has_metric SCHEMAFULL TYPE RELATION IN pod_to_pod OUT metric;
DEFINE FIELD result_table_id ON relation_has_metric TYPE string;
DEFINE FIELD created_at ON relation_has_metric TYPE int;
DEFINE FIELD updated_at ON relation_has_metric TYPE int;

-- ============================================================================
-- SECTION 3.10: Lifecycle Management Functions
--
-- These functions provide generalized lifecycle management capabilities for
-- resources and relations. They handle upsert logic with tolerance-based
-- lifecycle detection.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 3.10.1 Remove Existing Functions (for clean reset)
-- ----------------------------------------------------------------------------

REMOVE FUNCTION IF EXISTS fn::kv_block;
REMOVE FUNCTION IF EXISTS fn::upsert_resource_lifecycle;
REMOVE FUNCTION IF EXISTS fn::relation_id;
REMOVE FUNCTION IF EXISTS fn::upsert_relation_lifecycle;

-- ----------------------------------------------------------------------------
-- 3.10.2 Helper Functions
-- ----------------------------------------------------------------------------

-- fn::kv_block: Convert dimensions object to sorted key=value string with created_at
-- This function creates a deterministic string representation of resource dimensions
-- for use in ID generation.
DEFINE FUNCTION fn::kv_block($dimensions: object, $created_at: int) {
    LET $entries = object::entries($dimensions);
    LET $sorted = array::sort($entries);
    LET $pairs = array::map($sorted, |$e| string::concat($e[0], "=", <string>$e[1]));
    LET $base = array::join($pairs, ",");
    RETURN string::concat($base, ",created_at=", <string>$created_at);
};

-- fn::relation_id: Generate a deterministic relation ID from two resource record IDs
-- Format: {from_kv}|{to_kv} where kv is the key=value string extracted from resource ID
-- Example: node_with_pod:⟨bcs_cluster_id=X,node=Y,created_at=T1|bcs_cluster_id=X,namespace=N,pod=P,created_at=T2⟩
DEFINE FUNCTION fn::relation_id($from_id: record, $to_id: record) {
    LET $from_str = <string>$from_id;
    LET $to_str = <string>$to_id;
    LET $from_kv = string::split($from_str, ":")[1];
    LET $to_kv = string::split($to_str, ":")[1];
    -- Remove the angle brackets ⟨ and ⟩ from the kv strings
    LET $from_kv_clean = string::replace(string::replace($from_kv, "⟨", ""), "⟩", "");
    LET $to_kv_clean = string::replace(string::replace($to_kv, "⟨", ""), "⟩", "");
    RETURN string::concat($from_kv_clean, "|", $to_kv_clean);
};


-- ----------------------------------------------------------------------------
-- 3.10.3 Resource Lifecycle Management
-- ----------------------------------------------------------------------------

-- fn::upsert_resource_lifecycle: Generalized resource lifecycle management function
-- 
-- This function implements the upsert logic for resources with tolerance-based
-- lifecycle detection. If the last update was within tolerance, it updates the
-- existing record. Otherwise, it creates a new lifecycle record.
--
-- Note: SurrealDB closures cannot access external variables, so we use explicit
-- field matching for all possible dimension fields.
--
-- Parameters:
--   $table: The resource table name
--   $dimensions: Object containing the resource's dimension fields
--   $now: Current timestamp in milliseconds
--   $tolerance: Tolerance time in milliseconds for lifecycle detection
DEFINE FUNCTION fn::upsert_resource_lifecycle(
    $table: string,
    $dimensions: object,
    $now: int,
    $tolerance: int
) {
    LET $last_record = (
        SELECT * FROM type::table($table)
        WHERE ($dimensions.bcs_cluster_id IS NONE OR bcs_cluster_id = $dimensions.bcs_cluster_id)
          AND ($dimensions.namespace IS NONE OR namespace = $dimensions.namespace)
          AND ($dimensions.pod IS NONE OR pod = $dimensions.pod)
          AND ($dimensions.node IS NONE OR node = $dimensions.node)
          AND ($dimensions.service IS NONE OR service = $dimensions.service)
          AND ($dimensions.container IS NONE OR container = $dimensions.container)
          AND ($dimensions.deployment IS NONE OR deployment = $dimensions.deployment)
          AND ($dimensions.statefulset IS NONE OR statefulset = $dimensions.statefulset)
          AND ($dimensions.daemonset IS NONE OR daemonset = $dimensions.daemonset)
          AND ($dimensions.replicaset IS NONE OR replicaset = $dimensions.replicaset)
          AND ($dimensions.job IS NONE OR job = $dimensions.job)
          AND ($dimensions.ingress IS NONE OR ingress = $dimensions.ingress)
          AND ($dimensions.bk_cloud_id IS NONE OR bk_cloud_id = $dimensions.bk_cloud_id)
          AND ($dimensions.bk_target_ip IS NONE OR bk_target_ip = $dimensions.bk_target_ip)
          AND ($dimensions.address IS NONE OR address = $dimensions.address)
          AND ($dimensions.domain IS NONE OR domain = $dimensions.domain)
          AND ($dimensions.apm_application_name IS NONE OR apm_application_name = $dimensions.apm_application_name)
          AND ($dimensions.apm_service_name IS NONE OR apm_service_name = $dimensions.apm_service_name)
          AND ($dimensions.apm_service_instance_name IS NONE OR apm_service_instance_name = $dimensions.apm_service_instance_name)
          AND ($dimensions.bk_data_id IS NONE OR bk_data_id = $dimensions.bk_data_id)
          AND ($dimensions.bklogconfig_namespace IS NONE OR bklogconfig_namespace = $dimensions.bklogconfig_namespace)
          AND ($dimensions.bklogconfig_name IS NONE OR bklogconfig_name = $dimensions.bklogconfig_name)
          AND ($dimensions.bk_biz_id IS NONE OR bk_biz_id = $dimensions.bk_biz_id)
          AND ($dimensions.bk_set_id IS NONE OR bk_set_id = $dimensions.bk_set_id)
          AND ($dimensions.bk_module_id IS NONE OR bk_module_id = $dimensions.bk_module_id)
          AND ($dimensions.bk_host_id IS NONE OR bk_host_id = $dimensions.bk_host_id)
          AND ($dimensions.app_name IS NONE OR app_name = $dimensions.app_name)
          AND ($dimensions.version IS NONE OR version = $dimensions.version)
          AND ($dimensions.git_repo IS NONE OR git_repo = $dimensions.git_repo)
          AND ($dimensions.commit_id IS NONE OR commit_id = $dimensions.commit_id)
          AND ($dimensions.environment IS NONE OR environment = $dimensions.environment)
          AND ($dimensions.metric_name IS NONE OR metric_name = $dimensions.metric_name)
        ORDER BY created_at DESC 
        LIMIT 1
    )[0];

    RETURN IF $last_record != NONE AND ($now - $last_record.updated_at <= $tolerance) THEN
        -- UPDATE returns an array, extract first element
        (UPDATE $last_record.id SET updated_at = $now)[0]
    ELSE {
        -- Generate string-based ID using fn::kv_block
        LET $id_str = fn::kv_block($dimensions, $now);
        LET $content = object::from_entries(array::concat(object::entries($dimensions), [["created_at", $now], ["updated_at", $now]]));
        -- CREATE returns an array, extract first element
        (CREATE type::thing($table, $id_str) CONTENT $content)[0]
    }
    END;
};

-- ----------------------------------------------------------------------------
-- 3.10.4 Relation Lifecycle Management
-- ----------------------------------------------------------------------------

-- fn::upsert_relation_lifecycle: Relation lifecycle management function
--
-- This function manages the lifecycle of relations between two resources.
-- It upserts both endpoint resources and returns their IDs for relation creation.
--
-- NOTE: SurrealDB does not support dynamic table names in RELATE statements within functions.
-- The actual RELATE statement must be executed from the application layer.
-- This function prepares the resources and returns the necessary information.
--
-- Parameters:
--   $from_table: Source resource table name
--   $from_dimensions: Source resource dimensions
--   $to_table: Target resource table name
--   $to_dimensions: Target resource dimensions
--   $now: Current timestamp in milliseconds
--   $tolerance: Tolerance time in milliseconds
--   $relation_type: "static" or "dynamic" - indicates the type of relation
--   $bidirectional: bool - true for bidirectional relations, false for directional
--
-- Returns: Object with from_id, to_id, from_created_at, to_created_at for use in RELATE
DEFINE FUNCTION fn::upsert_relation_lifecycle(
    $from_table: string,
    $from_dimensions: object,
    $to_table: string,
    $to_dimensions: object,
    $now: int,
    $tolerance: int,
    $relation_type: string,
    $bidirectional: bool
) {
    -- 1) Upsert both endpoint resources
    LET $from_rec = fn::upsert_resource_lifecycle($from_table, $from_dimensions, $now, $tolerance);
    LET $to_rec = fn::upsert_resource_lifecycle($to_table, $to_dimensions, $now, $tolerance);

    -- 2) Return resource info for caller to create the relation
    RETURN {
        from_id: $from_rec.id,
        to_id: $to_rec.id,
        from_created_at: $from_rec.created_at,
        to_created_at: $to_rec.created_at,
        from_table: $from_table,
        to_table: $to_table,
        relation_type: $relation_type,
        bidirectional: $bidirectional,
        now: $now
    };
};

-- ============================================================================
-- SECTION 4: Unified Relation Upsert Function
--
-- This single function provides upsert capability for ALL relation tables.
-- It uses dynamic table names with the RELATE statement.
--
-- The function:
--   1. Generates a deterministic relation ID from the two endpoint IDs
--   2. Checks if relation already exists using the generated ID
--   3. If exists: updates updated_at timestamp
--   4. If not exists: creates new relation with RELATE statement
--
-- ID Format: {from_kv}|{to_kv}
-- Example: node_with_pod:⟨bcs_cluster_id=X,node=Y,created_at=T1|bcs_cluster_id=X,namespace=N,pod=P,created_at=T2⟩
-- ============================================================================

-- Remove all legacy per-table upsert functions (keep for backward compatibility cleanup)
REMOVE FUNCTION IF EXISTS fn::upsert_node_with_system;
REMOVE FUNCTION IF EXISTS fn::upsert_node_with_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_job_with_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_pod_with_replicaset;
REMOVE FUNCTION IF EXISTS fn::upsert_pod_with_statefulset;
REMOVE FUNCTION IF EXISTS fn::upsert_daemonset_with_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_deployment_with_replicaset;
REMOVE FUNCTION IF EXISTS fn::upsert_pod_with_service;
REMOVE FUNCTION IF EXISTS fn::upsert_ingress_with_service;
REMOVE FUNCTION IF EXISTS fn::upsert_k8s_address_with_service;
REMOVE FUNCTION IF EXISTS fn::upsert_domain_with_service;
REMOVE FUNCTION IF EXISTS fn::upsert_apm_service_instance_with_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_apm_service_instance_with_system;
REMOVE FUNCTION IF EXISTS fn::upsert_apm_service_with_apm_service_instance;
REMOVE FUNCTION IF EXISTS fn::upsert_container_with_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_datasource_with_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_datasource_with_node;
REMOVE FUNCTION IF EXISTS fn::upsert_bklogconfig_with_datasource;
REMOVE FUNCTION IF EXISTS fn::upsert_biz_with_set;
REMOVE FUNCTION IF EXISTS fn::upsert_module_with_set;
REMOVE FUNCTION IF EXISTS fn::upsert_host_with_module;
REMOVE FUNCTION IF EXISTS fn::upsert_host_with_system;
REMOVE FUNCTION IF EXISTS fn::upsert_app_version_with_container;
REMOVE FUNCTION IF EXISTS fn::upsert_app_version_with_system;
REMOVE FUNCTION IF EXISTS fn::upsert_container_with_environment;
REMOVE FUNCTION IF EXISTS fn::upsert_environment_with_system;
REMOVE FUNCTION IF EXISTS fn::upsert_app_version_with_git_commit;
REMOVE FUNCTION IF EXISTS fn::upsert_pod_to_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_pod_to_system;
REMOVE FUNCTION IF EXISTS fn::upsert_system_to_pod;
REMOVE FUNCTION IF EXISTS fn::upsert_system_to_system;
REMOVE FUNCTION IF EXISTS fn::upsert_service_to_service;
REMOVE FUNCTION IF EXISTS fn::upsert_relation;

-- ----------------------------------------------------------------------------
-- 4.1 Unified Relation Upsert Function
-- ----------------------------------------------------------------------------

-- fn::upsert_relation: Universal relation upsert function for all relation tables
--
-- This function handles upsert operations for any relation table by:
--   1. Generating a deterministic ID from the endpoint record IDs
--   2. Checking for existing relation by ID (efficient lookup)
--   3. Either updating the existing relation or creating a new one
--
-- Parameters:
--   $relation_table: The relation table name (e.g., "node_with_pod", "pod_to_pod")
--   $from_id: The source/from endpoint record ID
--   $to_id: The target/to endpoint record ID
--   $now: Current timestamp in milliseconds
--
-- Returns: The upserted relation record
--
-- Usage Examples:
--   fn::upsert_relation("node_with_pod", $node_id, $pod_id, time::millis())
--   fn::upsert_relation("pod_to_pod", $source_pod_id, $target_pod_id, time::millis())
--   fn::upsert_relation("pod_with_service", $pod_id, $service_id, time::millis())
DEFINE FUNCTION fn::upsert_relation($relation_table: string, $from_id: record, $to_id: record, $now: int) {
    -- Generate deterministic relation ID from endpoint IDs
    LET $rel_id = fn::relation_id($from_id, $to_id);
    LET $full_id = type::thing($relation_table, $rel_id);
    
    -- Use dynamic table reference for both SELECT and RELATE
    LET $rel_table = type::table($relation_table);
    
    -- Check if relation already exists by ID (efficient lookup)
    LET $existing = (SELECT * FROM type::table($relation_table) WHERE id = $full_id LIMIT 1)[0];
    
    RETURN IF $existing != NONE THEN
        -- Update existing relation's updated_at timestamp
        (UPDATE $existing.id SET updated_at = $now)[0]
    ELSE
        -- Create new relation with custom ID
        (RELATE $from_id->$rel_table->$to_id SET id = $full_id, created_at = $now, updated_at = $now)[0]
    END;
};

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
