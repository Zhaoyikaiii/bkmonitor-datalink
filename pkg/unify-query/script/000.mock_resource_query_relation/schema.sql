-- ============================================================================
-- SurrealDB Schema for Resource Graph
-- 
-- This schema defines all resource types and relations for the BK Monitor
-- resource topology graph. It follows the design document specifications.
--
-- Features:
--   - All tables have created_at and updated_at timestamp fields
--   - is_alive computed field for liveness detection based on updated_at
--   - TTL placeholder {ttl} will be replaced at runtime (default: 2m)
--   - TYPE RELATION tables for proper graph traversal
--
-- Usage:
--   The Python script will load this file and replace {ttl} with the
--   configured TTL value before executing.
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
--   - created_at: When the resource was first seen
--   - updated_at: Last time the resource was updated/seen
--   - is_alive: Computed field, true if (now - updated_at) < TTL
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
DEFINE FIELD created_at ON pod TYPE datetime;
DEFINE FIELD updated_at ON pod TYPE datetime;
DEFINE FIELD is_alive ON pod VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_pod_key ON pod FIELDS bcs_cluster_id, namespace, pod UNIQUE;

-- Node: Worker machine in Kubernetes cluster
-- Index: bcs_cluster_id, node
DEFINE TABLE node SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON node TYPE string;
DEFINE FIELD node ON node TYPE string;
DEFINE FIELD created_at ON node TYPE datetime;
DEFINE FIELD updated_at ON node TYPE datetime;
DEFINE FIELD is_alive ON node VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_node_key ON node FIELDS bcs_cluster_id, node UNIQUE;

-- Container: Running instance within a Pod
-- Index: bcs_cluster_id, namespace, pod, container
DEFINE TABLE container SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON container TYPE string;
DEFINE FIELD namespace ON container TYPE string;
DEFINE FIELD pod ON container TYPE string;
DEFINE FIELD container ON container TYPE string;
DEFINE FIELD created_at ON container TYPE datetime;
DEFINE FIELD updated_at ON container TYPE datetime;
DEFINE FIELD is_alive ON container VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_container_key ON container FIELDS bcs_cluster_id, namespace, pod, container UNIQUE;

-- Deployment: Declarative updates for Pods and ReplicaSets
-- Index: bcs_cluster_id, namespace, deployment
DEFINE TABLE deployment SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON deployment TYPE string;
DEFINE FIELD namespace ON deployment TYPE string;
DEFINE FIELD deployment ON deployment TYPE string;
DEFINE FIELD created_at ON deployment TYPE datetime;
DEFINE FIELD updated_at ON deployment TYPE datetime;
DEFINE FIELD is_alive ON deployment VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_deployment_key ON deployment FIELDS bcs_cluster_id, namespace, deployment UNIQUE;

-- ReplicaSet: Maintains a stable set of replica Pods
-- Index: bcs_cluster_id, namespace, replicaset
DEFINE TABLE replicaset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON replicaset TYPE string;
DEFINE FIELD namespace ON replicaset TYPE string;
DEFINE FIELD replicaset ON replicaset TYPE string;
DEFINE FIELD created_at ON replicaset TYPE datetime;
DEFINE FIELD updated_at ON replicaset TYPE datetime;
DEFINE FIELD is_alive ON replicaset VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_replicaset_key ON replicaset FIELDS bcs_cluster_id, namespace, replicaset UNIQUE;

-- StatefulSet: Manages stateful applications
-- Index: bcs_cluster_id, namespace, statefulset
DEFINE TABLE statefulset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON statefulset TYPE string;
DEFINE FIELD namespace ON statefulset TYPE string;
DEFINE FIELD statefulset ON statefulset TYPE string;
DEFINE FIELD created_at ON statefulset TYPE datetime;
DEFINE FIELD updated_at ON statefulset TYPE datetime;
DEFINE FIELD is_alive ON statefulset VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_statefulset_key ON statefulset FIELDS bcs_cluster_id, namespace, statefulset UNIQUE;

-- DaemonSet: Ensures all nodes run a copy of a Pod
-- Index: bcs_cluster_id, namespace, daemonset
DEFINE TABLE daemonset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON daemonset TYPE string;
DEFINE FIELD namespace ON daemonset TYPE string;
DEFINE FIELD daemonset ON daemonset TYPE string;
DEFINE FIELD created_at ON daemonset TYPE datetime;
DEFINE FIELD updated_at ON daemonset TYPE datetime;
DEFINE FIELD is_alive ON daemonset VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_daemonset_key ON daemonset FIELDS bcs_cluster_id, namespace, daemonset UNIQUE;

-- Job: Creates Pods that run to completion
-- Index: bcs_cluster_id, namespace, job
DEFINE TABLE job SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON job TYPE string;
DEFINE FIELD namespace ON job TYPE string;
DEFINE FIELD job ON job TYPE string;
DEFINE FIELD created_at ON job TYPE datetime;
DEFINE FIELD updated_at ON job TYPE datetime;
DEFINE FIELD is_alive ON job VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_job_key ON job FIELDS bcs_cluster_id, namespace, job UNIQUE;

-- Service: Abstract way to expose an application running on Pods
-- Index: bcs_cluster_id, namespace, service
DEFINE TABLE service SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON service TYPE string;
DEFINE FIELD namespace ON service TYPE string;
DEFINE FIELD service ON service TYPE string;
DEFINE FIELD created_at ON service TYPE datetime;
DEFINE FIELD updated_at ON service TYPE datetime;
DEFINE FIELD is_alive ON service VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_service_key ON service FIELDS bcs_cluster_id, namespace, service UNIQUE;

-- Ingress: Manages external access to services
-- Index: bcs_cluster_id, namespace, ingress
DEFINE TABLE ingress SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON ingress TYPE string;
DEFINE FIELD namespace ON ingress TYPE string;
DEFINE FIELD ingress ON ingress TYPE string;
DEFINE FIELD created_at ON ingress TYPE datetime;
DEFINE FIELD updated_at ON ingress TYPE datetime;
DEFINE FIELD is_alive ON ingress VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_ingress_key ON ingress FIELDS bcs_cluster_id, namespace, ingress UNIQUE;

-- Cluster: Kubernetes cluster
-- Index: bcs_cluster_id
DEFINE TABLE cluster SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON cluster TYPE string;
DEFINE FIELD created_at ON cluster TYPE datetime;
DEFINE FIELD updated_at ON cluster TYPE datetime;
DEFINE FIELD is_alive ON cluster VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_cluster_key ON cluster FIELDS bcs_cluster_id UNIQUE;

-- Namespace: Virtual cluster within a physical cluster
-- Index: bcs_cluster_id, namespace
DEFINE TABLE namespace SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON namespace TYPE string;
DEFINE FIELD namespace ON namespace TYPE string;
DEFINE FIELD created_at ON namespace TYPE datetime;
DEFINE FIELD updated_at ON namespace TYPE datetime;
DEFINE FIELD is_alive ON namespace VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_namespace_key ON namespace FIELDS bcs_cluster_id, namespace UNIQUE;

-- ----------------------------------------------------------------------------
-- 2.2 Network Resources
-- ----------------------------------------------------------------------------

-- System: Physical or virtual machine identified by IP
-- Index: bk_cloud_id, bk_target_ip
DEFINE TABLE system SCHEMAFULL;
DEFINE FIELD bk_cloud_id ON system TYPE string;
DEFINE FIELD bk_target_ip ON system TYPE string;
DEFINE FIELD created_at ON system TYPE datetime;
DEFINE FIELD updated_at ON system TYPE datetime;
DEFINE FIELD is_alive ON system VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_system_key ON system FIELDS bk_cloud_id, bk_target_ip UNIQUE;

-- K8s Address: ClusterIP or endpoint address
-- Index: bcs_cluster_id, address
DEFINE TABLE k8s_address SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON k8s_address TYPE string;
DEFINE FIELD address ON k8s_address TYPE string;
DEFINE FIELD created_at ON k8s_address TYPE datetime;
DEFINE FIELD updated_at ON k8s_address TYPE datetime;
DEFINE FIELD is_alive ON k8s_address VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_k8s_address_key ON k8s_address FIELDS bcs_cluster_id, address UNIQUE;

-- Domain: DNS domain name
-- Index: bcs_cluster_id, domain
DEFINE TABLE domain SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON domain TYPE string;
DEFINE FIELD domain ON domain TYPE string;
DEFINE FIELD created_at ON domain TYPE datetime;
DEFINE FIELD updated_at ON domain TYPE datetime;
DEFINE FIELD is_alive ON domain VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_domain_key ON domain FIELDS bcs_cluster_id, domain UNIQUE;

-- ----------------------------------------------------------------------------
-- 2.3 APM Resources
-- ----------------------------------------------------------------------------

-- APM Service: Application performance monitoring service
-- Index: apm_application_name, apm_service_name
DEFINE TABLE apm_service SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service TYPE string;
DEFINE FIELD apm_service_name ON apm_service TYPE string;
DEFINE FIELD created_at ON apm_service TYPE datetime;
DEFINE FIELD updated_at ON apm_service TYPE datetime;
DEFINE FIELD is_alive ON apm_service VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_apm_service_key ON apm_service FIELDS apm_application_name, apm_service_name UNIQUE;

-- APM Service Instance: Instance of an APM service
-- Index: apm_application_name, apm_service_name, apm_service_instance_name
DEFINE TABLE apm_service_instance SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_instance_name ON apm_service_instance TYPE string;
DEFINE FIELD created_at ON apm_service_instance TYPE datetime;
DEFINE FIELD updated_at ON apm_service_instance TYPE datetime;
DEFINE FIELD is_alive ON apm_service_instance VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_apm_service_instance_key ON apm_service_instance FIELDS apm_application_name, apm_service_name, apm_service_instance_name UNIQUE;

-- ----------------------------------------------------------------------------
-- 2.4 Data Source Resources
-- ----------------------------------------------------------------------------

-- DataSource: BK Monitor data source
-- Index: bk_data_id
DEFINE TABLE datasource SCHEMAFULL;
DEFINE FIELD bk_data_id ON datasource TYPE string;
DEFINE FIELD created_at ON datasource TYPE datetime;
DEFINE FIELD updated_at ON datasource TYPE datetime;
DEFINE FIELD is_alive ON datasource VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_datasource_key ON datasource FIELDS bk_data_id UNIQUE;

-- BKLogConfig: Log collection configuration
-- Index: bklogconfig_namespace, bklogconfig_name
DEFINE TABLE bklogconfig SCHEMAFULL;
DEFINE FIELD bklogconfig_namespace ON bklogconfig TYPE string;
DEFINE FIELD bklogconfig_name ON bklogconfig TYPE string;
DEFINE FIELD created_at ON bklogconfig TYPE datetime;
DEFINE FIELD updated_at ON bklogconfig TYPE datetime;
DEFINE FIELD is_alive ON bklogconfig VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_bklogconfig_key ON bklogconfig FIELDS bklogconfig_namespace, bklogconfig_name UNIQUE;

-- ----------------------------------------------------------------------------
-- 2.5 CMDB Resources
-- ----------------------------------------------------------------------------

-- Biz: Business unit in CMDB
-- Index: bk_biz_id
DEFINE TABLE biz SCHEMAFULL;
DEFINE FIELD bk_biz_id ON biz TYPE string;
DEFINE FIELD created_at ON biz TYPE datetime;
DEFINE FIELD updated_at ON biz TYPE datetime;
DEFINE FIELD is_alive ON biz VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_biz_key ON biz FIELDS bk_biz_id UNIQUE;

-- Set: Set within a business
-- Index: bk_set_id
DEFINE TABLE set SCHEMAFULL;
DEFINE FIELD bk_set_id ON set TYPE string;
DEFINE FIELD created_at ON set TYPE datetime;
DEFINE FIELD updated_at ON set TYPE datetime;
DEFINE FIELD is_alive ON set VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_set_key ON set FIELDS bk_set_id UNIQUE;

-- Module: Module within a set
-- Index: bk_module_id
DEFINE TABLE module SCHEMAFULL;
DEFINE FIELD bk_module_id ON module TYPE string;
DEFINE FIELD created_at ON module TYPE datetime;
DEFINE FIELD updated_at ON module TYPE datetime;
DEFINE FIELD is_alive ON module VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_module_key ON module FIELDS bk_module_id UNIQUE;

-- Host: Physical or virtual host in CMDB
-- Index: bk_host_id
DEFINE TABLE host SCHEMAFULL;
DEFINE FIELD bk_host_id ON host TYPE string;
DEFINE FIELD created_at ON host TYPE datetime;
DEFINE FIELD updated_at ON host TYPE datetime;
DEFINE FIELD is_alive ON host VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_host_key ON host FIELDS bk_host_id UNIQUE;

-- ----------------------------------------------------------------------------
-- 2.6 App Version Resources
-- ----------------------------------------------------------------------------

-- App Version: Application version tracking
-- Index: app_name, version
DEFINE TABLE app_version SCHEMAFULL;
DEFINE FIELD app_name ON app_version TYPE string;
DEFINE FIELD version ON app_version TYPE string;
DEFINE FIELD created_at ON app_version TYPE datetime;
DEFINE FIELD updated_at ON app_version TYPE datetime;
DEFINE FIELD is_alive ON app_version VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_app_version_key ON app_version FIELDS app_name, version UNIQUE;

-- Git Commit: Source code commit information
-- Index: git_repo, commit_id
DEFINE TABLE git_commit SCHEMAFULL;
DEFINE FIELD git_repo ON git_commit TYPE string;
DEFINE FIELD commit_id ON git_commit TYPE string;
DEFINE FIELD created_at ON git_commit TYPE datetime;
DEFINE FIELD updated_at ON git_commit TYPE datetime;
DEFINE FIELD is_alive ON git_commit VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_git_commit_key ON git_commit FIELDS git_repo, commit_id UNIQUE;

-- Environment: Deployment environment (production, staging, etc.)
-- Index: environment
DEFINE TABLE environment SCHEMAFULL;
DEFINE FIELD environment ON environment TYPE string;
DEFINE FIELD created_at ON environment TYPE datetime;
DEFINE FIELD updated_at ON environment TYPE datetime;
DEFINE FIELD is_alive ON environment VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_environment_key ON environment FIELDS environment UNIQUE;

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
DEFINE FIELD created_at ON metric TYPE datetime;
DEFINE FIELD updated_at ON metric TYPE datetime;
DEFINE FIELD is_alive ON metric VALUE <future> { (time::now() - updated_at) < {ttl} };
DEFINE INDEX idx_metric_key ON metric FIELDS metric_name UNIQUE;

-- ============================================================================
-- SECTION 3: Relation Tables (TYPE RELATION for graph traversal)
--
-- Naming convention:
--   - Static relations: {res1}_with_{res2} (bidirectional, res1 < res2 alphabetically)
--   - Dynamic relations: {src}_to_{dst} (directional, for traffic flow)
--
-- All relations have:
--   - created_at: When the relation was first established
--   - updated_at: Last time the relation was observed
--   - is_alive: Computed field for liveness detection
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 3.1 Kubernetes Static Relations
-- ----------------------------------------------------------------------------

-- Node contains System (physical machine)
DEFINE TABLE node_with_system SCHEMAFULL TYPE RELATION IN node OUT system;
DEFINE FIELD created_at ON node_with_system TYPE datetime;
DEFINE FIELD updated_at ON node_with_system TYPE datetime;
DEFINE FIELD is_alive ON node_with_system VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Node runs Pods
DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION IN node OUT pod;
DEFINE FIELD created_at ON node_with_pod TYPE datetime;
DEFINE FIELD updated_at ON node_with_pod TYPE datetime;
DEFINE FIELD is_alive ON node_with_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Job creates Pods
DEFINE TABLE job_with_pod SCHEMAFULL TYPE RELATION IN job OUT pod;
DEFINE FIELD created_at ON job_with_pod TYPE datetime;
DEFINE FIELD updated_at ON job_with_pod TYPE datetime;
DEFINE FIELD is_alive ON job_with_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Pod belongs to ReplicaSet
DEFINE TABLE pod_with_replicaset SCHEMAFULL TYPE RELATION IN pod OUT replicaset;
DEFINE FIELD created_at ON pod_with_replicaset TYPE datetime;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE datetime;
DEFINE FIELD is_alive ON pod_with_replicaset VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Pod belongs to StatefulSet
DEFINE TABLE pod_with_statefulset SCHEMAFULL TYPE RELATION IN pod OUT statefulset;
DEFINE FIELD created_at ON pod_with_statefulset TYPE datetime;
DEFINE FIELD updated_at ON pod_with_statefulset TYPE datetime;
DEFINE FIELD is_alive ON pod_with_statefulset VALUE <future> { (time::now() - updated_at) < {ttl} };

-- DaemonSet manages Pods
DEFINE TABLE daemonset_with_pod SCHEMAFULL TYPE RELATION IN daemonset OUT pod;
DEFINE FIELD created_at ON daemonset_with_pod TYPE datetime;
DEFINE FIELD updated_at ON daemonset_with_pod TYPE datetime;
DEFINE FIELD is_alive ON daemonset_with_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Deployment manages ReplicaSets
DEFINE TABLE deployment_with_replicaset SCHEMAFULL TYPE RELATION IN deployment OUT replicaset;
DEFINE FIELD created_at ON deployment_with_replicaset TYPE datetime;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE datetime;
DEFINE FIELD is_alive ON deployment_with_replicaset VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Pod exposes through Service
DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION IN pod OUT service;
DEFINE FIELD created_at ON pod_with_service TYPE datetime;
DEFINE FIELD updated_at ON pod_with_service TYPE datetime;
DEFINE FIELD is_alive ON pod_with_service VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Ingress routes to Service
DEFINE TABLE ingress_with_service SCHEMAFULL TYPE RELATION IN ingress OUT service;
DEFINE FIELD created_at ON ingress_with_service TYPE datetime;
DEFINE FIELD updated_at ON ingress_with_service TYPE datetime;
DEFINE FIELD is_alive ON ingress_with_service VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.2 Network Static Relations
-- ----------------------------------------------------------------------------

-- K8s Address points to Service
DEFINE TABLE k8s_address_with_service SCHEMAFULL TYPE RELATION IN k8s_address OUT service;
DEFINE FIELD created_at ON k8s_address_with_service TYPE datetime;
DEFINE FIELD updated_at ON k8s_address_with_service TYPE datetime;
DEFINE FIELD is_alive ON k8s_address_with_service VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Domain resolves to Service
DEFINE TABLE domain_with_service SCHEMAFULL TYPE RELATION IN domain OUT service;
DEFINE FIELD created_at ON domain_with_service TYPE datetime;
DEFINE FIELD updated_at ON domain_with_service TYPE datetime;
DEFINE FIELD is_alive ON domain_with_service VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.3 APM Static Relations
-- ----------------------------------------------------------------------------

-- APM Service Instance runs on Pod
DEFINE TABLE apm_service_instance_with_pod SCHEMAFULL TYPE RELATION IN apm_service_instance OUT pod;
DEFINE FIELD created_at ON apm_service_instance_with_pod TYPE datetime;
DEFINE FIELD updated_at ON apm_service_instance_with_pod TYPE datetime;
DEFINE FIELD is_alive ON apm_service_instance_with_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- APM Service Instance runs on System
DEFINE TABLE apm_service_instance_with_system SCHEMAFULL TYPE RELATION IN apm_service_instance OUT system;
DEFINE FIELD created_at ON apm_service_instance_with_system TYPE datetime;
DEFINE FIELD updated_at ON apm_service_instance_with_system TYPE datetime;
DEFINE FIELD is_alive ON apm_service_instance_with_system VALUE <future> { (time::now() - updated_at) < {ttl} };

-- APM Service has Instances
DEFINE TABLE apm_service_with_apm_service_instance SCHEMAFULL TYPE RELATION IN apm_service OUT apm_service_instance;
DEFINE FIELD created_at ON apm_service_with_apm_service_instance TYPE datetime;
DEFINE FIELD updated_at ON apm_service_with_apm_service_instance TYPE datetime;
DEFINE FIELD is_alive ON apm_service_with_apm_service_instance VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.4 Container Static Relations
-- ----------------------------------------------------------------------------

-- Container runs in Pod
DEFINE TABLE container_with_pod SCHEMAFULL TYPE RELATION IN container OUT pod;
DEFINE FIELD created_at ON container_with_pod TYPE datetime;
DEFINE FIELD updated_at ON container_with_pod TYPE datetime;
DEFINE FIELD is_alive ON container_with_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.5 Data Source Static Relations
-- ----------------------------------------------------------------------------

-- DataSource collects from Pod
DEFINE TABLE datasource_with_pod SCHEMAFULL TYPE RELATION IN datasource OUT pod;
DEFINE FIELD created_at ON datasource_with_pod TYPE datetime;
DEFINE FIELD updated_at ON datasource_with_pod TYPE datetime;
DEFINE FIELD is_alive ON datasource_with_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- DataSource collects from Node
DEFINE TABLE datasource_with_node SCHEMAFULL TYPE RELATION IN datasource OUT node;
DEFINE FIELD created_at ON datasource_with_node TYPE datetime;
DEFINE FIELD updated_at ON datasource_with_node TYPE datetime;
DEFINE FIELD is_alive ON datasource_with_node VALUE <future> { (time::now() - updated_at) < {ttl} };

-- BKLogConfig uses DataSource
DEFINE TABLE bklogconfig_with_datasource SCHEMAFULL TYPE RELATION IN bklogconfig OUT datasource;
DEFINE FIELD created_at ON bklogconfig_with_datasource TYPE datetime;
DEFINE FIELD updated_at ON bklogconfig_with_datasource TYPE datetime;
DEFINE FIELD is_alive ON bklogconfig_with_datasource VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.6 CMDB Static Relations
-- ----------------------------------------------------------------------------

-- Biz contains Sets
DEFINE TABLE biz_with_set SCHEMAFULL TYPE RELATION IN biz OUT set;
DEFINE FIELD created_at ON biz_with_set TYPE datetime;
DEFINE FIELD updated_at ON biz_with_set TYPE datetime;
DEFINE FIELD is_alive ON biz_with_set VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Module belongs to Set
DEFINE TABLE module_with_set SCHEMAFULL TYPE RELATION IN module OUT set;
DEFINE FIELD created_at ON module_with_set TYPE datetime;
DEFINE FIELD updated_at ON module_with_set TYPE datetime;
DEFINE FIELD is_alive ON module_with_set VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Host belongs to Module
DEFINE TABLE host_with_module SCHEMAFULL TYPE RELATION IN host OUT module;
DEFINE FIELD created_at ON host_with_module TYPE datetime;
DEFINE FIELD updated_at ON host_with_module TYPE datetime;
DEFINE FIELD is_alive ON host_with_module VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Host has System (IP)
DEFINE TABLE host_with_system SCHEMAFULL TYPE RELATION IN host OUT system;
DEFINE FIELD created_at ON host_with_system TYPE datetime;
DEFINE FIELD updated_at ON host_with_system TYPE datetime;
DEFINE FIELD is_alive ON host_with_system VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.7 App Version Static Relations
-- ----------------------------------------------------------------------------

-- App Version deployed to Container
DEFINE TABLE app_version_with_container SCHEMAFULL TYPE RELATION IN app_version OUT container;
DEFINE FIELD created_at ON app_version_with_container TYPE datetime;
DEFINE FIELD updated_at ON app_version_with_container TYPE datetime;
DEFINE FIELD is_alive ON app_version_with_container VALUE <future> { (time::now() - updated_at) < {ttl} };

-- App Version deployed to System
DEFINE TABLE app_version_with_system SCHEMAFULL TYPE RELATION IN app_version OUT system;
DEFINE FIELD created_at ON app_version_with_system TYPE datetime;
DEFINE FIELD updated_at ON app_version_with_system TYPE datetime;
DEFINE FIELD is_alive ON app_version_with_system VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Container runs in Environment
DEFINE TABLE container_with_environment SCHEMAFULL TYPE RELATION IN container OUT environment;
DEFINE FIELD created_at ON container_with_environment TYPE datetime;
DEFINE FIELD updated_at ON container_with_environment TYPE datetime;
DEFINE FIELD is_alive ON container_with_environment VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Environment has System
DEFINE TABLE environment_with_system SCHEMAFULL TYPE RELATION IN environment OUT system;
DEFINE FIELD created_at ON environment_with_system TYPE datetime;
DEFINE FIELD updated_at ON environment_with_system TYPE datetime;
DEFINE FIELD is_alive ON environment_with_system VALUE <future> { (time::now() - updated_at) < {ttl} };

-- App Version comes from Git Commit
DEFINE TABLE app_version_with_git_commit SCHEMAFULL TYPE RELATION IN app_version OUT git_commit;
DEFINE FIELD created_at ON app_version_with_git_commit TYPE datetime;
DEFINE FIELD updated_at ON app_version_with_git_commit TYPE datetime;
DEFINE FIELD is_alive ON app_version_with_git_commit VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.8 Dynamic Traffic Relations (Directional)
--
-- These represent actual network traffic flow between resources.
-- Direction matters: source -> target indicates traffic direction.
-- ----------------------------------------------------------------------------

-- Pod to Pod traffic
DEFINE TABLE pod_to_pod SCHEMAFULL TYPE RELATION IN pod OUT pod;
DEFINE FIELD created_at ON pod_to_pod TYPE datetime;
DEFINE FIELD updated_at ON pod_to_pod TYPE datetime;
DEFINE FIELD is_alive ON pod_to_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Pod to external System traffic
DEFINE TABLE pod_to_system SCHEMAFULL TYPE RELATION IN pod OUT system;
DEFINE FIELD created_at ON pod_to_system TYPE datetime;
DEFINE FIELD updated_at ON pod_to_system TYPE datetime;
DEFINE FIELD is_alive ON pod_to_system VALUE <future> { (time::now() - updated_at) < {ttl} };

-- External System to Pod traffic
DEFINE TABLE system_to_pod SCHEMAFULL TYPE RELATION IN system OUT pod;
DEFINE FIELD created_at ON system_to_pod TYPE datetime;
DEFINE FIELD updated_at ON system_to_pod TYPE datetime;
DEFINE FIELD is_alive ON system_to_pod VALUE <future> { (time::now() - updated_at) < {ttl} };

-- System to System traffic
DEFINE TABLE system_to_system SCHEMAFULL TYPE RELATION IN system OUT system;
DEFINE FIELD created_at ON system_to_system TYPE datetime;
DEFINE FIELD updated_at ON system_to_system TYPE datetime;
DEFINE FIELD is_alive ON system_to_system VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Service to Service traffic (aggregated from Pod traffic)
DEFINE TABLE service_to_service SCHEMAFULL TYPE RELATION IN service OUT service;
DEFINE FIELD created_at ON service_to_service TYPE datetime;
DEFINE FIELD updated_at ON service_to_service TYPE datetime;
DEFINE FIELD is_alive ON service_to_service VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ----------------------------------------------------------------------------
-- 3.9 Metric Relations
--
-- These connect resources/relations to their associated metrics.
-- ----------------------------------------------------------------------------

-- Node has Metric (for node-level metrics)
DEFINE TABLE node_has_metric SCHEMAFULL TYPE RELATION IN pod OUT metric;
DEFINE FIELD result_table_id ON node_has_metric TYPE string;
DEFINE FIELD created_at ON node_has_metric TYPE datetime;
DEFINE FIELD updated_at ON node_has_metric TYPE datetime;
DEFINE FIELD is_alive ON node_has_metric VALUE <future> { (time::now() - updated_at) < {ttl} };

-- Traffic Relation has Metric (for traffic metrics)
DEFINE TABLE relation_has_metric SCHEMAFULL TYPE RELATION IN pod_to_pod OUT metric;
DEFINE FIELD result_table_id ON relation_has_metric TYPE string;
DEFINE FIELD created_at ON relation_has_metric TYPE datetime;
DEFINE FIELD updated_at ON relation_has_metric TYPE datetime;
DEFINE FIELD is_alive ON relation_has_metric VALUE <future> { (time::now() - updated_at) < {ttl} };

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
