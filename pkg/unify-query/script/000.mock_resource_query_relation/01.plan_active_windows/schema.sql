-- ============================================================================
-- SurrealDB Schema V2 - Active Windows with Event-only Lifecycle Management
-- 
-- This schema implements the "active_windows" approach using ONLY SurrealDB Events
-- to automatically manage resource lifecycle. All lifecycle logic is embedded
-- directly in Events - no separate functions needed.
--
-- Key Features:
--   - ID is deterministic (no created_at in ID)
--   - active_windows: array of {start_time, end_time} objects
--   - Event-only lifecycle: all logic embedded in events, no functions
--   - Simple UPSERT MERGE syntax for writes
--
-- Write Pattern:
--   UPSERT pod:⟨bcs_cluster_id=X,namespace=N,pod=P⟩ MERGE {
--       bcs_cluster_id: "X",
--       namespace: "N", 
--       pod: "P",
--       updated_at: time::millis()
--   };
--
-- The Event will automatically:
--   - Initialize active_windows on first insert (Case 1: new record)
--   - Keep window open if within tolerance (Case 2: renewal)
--   - Close old window and open new one if beyond tolerance (Case 3: restart)
--
-- Author: Auto-generated for BK Monitor
-- ============================================================================

-- ============================================================================
-- SECTION 0: Configuration
-- ============================================================================

-- Tolerance time in milliseconds (default: 10 minutes = 600000ms)
-- This can be overridden by replacing {tolerance_time_ms} placeholder
LET $TOLERANCE_MS = {tolerance_time_ms};

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
-- SECTION 2: Helper Functions (ID generation only, no lifecycle function)
-- ============================================================================

-- Remove existing functions first
REMOVE FUNCTION IF EXISTS fn::kv_block;
REMOVE FUNCTION IF EXISTS fn::relation_id;
REMOVE FUNCTION IF EXISTS fn::upsert_relation;

-- fn::kv_block: Convert dimensions object to sorted key=value string
DEFINE FUNCTION fn::kv_block($dimensions: object) {
    LET $entries = object::entries($dimensions);
    LET $sorted = array::sort($entries);
    LET $pairs = array::map($sorted, |$e| string::concat($e[0], "=", <string>$e[1]));
    RETURN array::join($pairs, ",");
};

-- fn::relation_id: Generate a deterministic relation ID from two resource record IDs
DEFINE FUNCTION fn::relation_id($from_id: record, $to_id: record) {
    LET $from_str = <string>$from_id;
    LET $to_str = <string>$to_id;
    LET $from_kv = string::split($from_str, ":")[1];
    LET $to_kv = string::split($to_str, ":")[1];
    LET $from_kv_clean = string::replace(string::replace($from_kv, "⟨", ""), "⟩", "");
    LET $to_kv_clean = string::replace(string::replace($to_kv, "⟨", ""), "⟩", "");
    RETURN string::concat($from_kv_clean, "|", $to_kv_clean);
};

-- ============================================================================
-- SECTION 2.1: Relation Upsert Function
-- ============================================================================
-- fn::upsert_relation: Universal relation upsert function for all relation tables
-- Uses RELATE syntax which is required for TYPE RELATION tables
--
-- Parameters:
--   $relation_table: The relation table name (e.g., "node_with_pod", "pod_to_pod")
--   $from_id: The source/from endpoint record ID
--   $to_id: The target/to endpoint record ID
--   $now_ms: Current timestamp in milliseconds
--
-- Returns: The upserted relation record
DEFINE FUNCTION fn::upsert_relation($relation_table: string, $from_id: record, $to_id: record, $now_ms: int) {
    -- Generate deterministic relation ID from endpoint IDs
    LET $rel_id = fn::relation_id($from_id, $to_id);
    LET $full_id = type::thing($relation_table, $rel_id);
    
    -- Use dynamic table reference for RELATE
    LET $rel_table = type::table($relation_table);
    
    -- Check if relation already exists by ID (efficient lookup)
    LET $existing = (SELECT * FROM type::table($relation_table) WHERE id = $full_id LIMIT 1)[0];
    
    RETURN IF $existing != NONE THEN
        -- Update existing relation's updated_at timestamp
        (UPDATE $existing.id SET updated_at = $now_ms)[0]
    ELSE
        -- Create new relation with custom ID using RELATE syntax
        (RELATE $from_id->$rel_table->$to_id SET id = $full_id, updated_at = $now_ms)[0]
    END;
};

-- ============================================================================
-- SECTION 3: Node Tables with Event-based Lifecycle Management
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 3.1 Kubernetes Resources
-- ----------------------------------------------------------------------------

-- Pod
DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD updated_at ON pod TYPE int;
DEFINE FIELD active_windows ON pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON pod TYPE option<int>;
DEFINE INDEX idx_pod_unique ON pod FIELDS bcs_cluster_id, namespace, pod UNIQUE;

DEFINE EVENT lifecycle ON TABLE pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Node
DEFINE TABLE node SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON node TYPE string;
DEFINE FIELD node ON node TYPE string;
DEFINE FIELD updated_at ON node TYPE int;
DEFINE FIELD active_windows ON node TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON node TYPE int;
DEFINE FIELD active_windows[*].end_time ON node TYPE option<int>;
DEFINE INDEX idx_node_unique ON node FIELDS bcs_cluster_id, node UNIQUE;

DEFINE EVENT lifecycle ON TABLE node WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Container
DEFINE TABLE container SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON container TYPE string;
DEFINE FIELD namespace ON container TYPE string;
DEFINE FIELD pod ON container TYPE string;
DEFINE FIELD container ON container TYPE string;
DEFINE FIELD updated_at ON container TYPE int;
DEFINE FIELD active_windows ON container TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON container TYPE int;
DEFINE FIELD active_windows[*].end_time ON container TYPE option<int>;
DEFINE INDEX idx_container_unique ON container FIELDS bcs_cluster_id, namespace, pod, container UNIQUE;

DEFINE EVENT lifecycle ON TABLE container WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Deployment
DEFINE TABLE deployment SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON deployment TYPE string;
DEFINE FIELD namespace ON deployment TYPE string;
DEFINE FIELD deployment ON deployment TYPE string;
DEFINE FIELD updated_at ON deployment TYPE int;
DEFINE FIELD active_windows ON deployment TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON deployment TYPE int;
DEFINE FIELD active_windows[*].end_time ON deployment TYPE option<int>;
DEFINE INDEX idx_deployment_unique ON deployment FIELDS bcs_cluster_id, namespace, deployment UNIQUE;

DEFINE EVENT lifecycle ON TABLE deployment WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ReplicaSet
DEFINE TABLE replicaset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON replicaset TYPE string;
DEFINE FIELD namespace ON replicaset TYPE string;
DEFINE FIELD replicaset ON replicaset TYPE string;
DEFINE FIELD updated_at ON replicaset TYPE int;
DEFINE FIELD active_windows ON replicaset TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON replicaset TYPE int;
DEFINE FIELD active_windows[*].end_time ON replicaset TYPE option<int>;
DEFINE INDEX idx_replicaset_unique ON replicaset FIELDS bcs_cluster_id, namespace, replicaset UNIQUE;

DEFINE EVENT lifecycle ON TABLE replicaset WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- StatefulSet
DEFINE TABLE statefulset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON statefulset TYPE string;
DEFINE FIELD namespace ON statefulset TYPE string;
DEFINE FIELD statefulset ON statefulset TYPE string;
DEFINE FIELD updated_at ON statefulset TYPE int;
DEFINE FIELD active_windows ON statefulset TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON statefulset TYPE int;
DEFINE FIELD active_windows[*].end_time ON statefulset TYPE option<int>;
DEFINE INDEX idx_statefulset_unique ON statefulset FIELDS bcs_cluster_id, namespace, statefulset UNIQUE;

DEFINE EVENT lifecycle ON TABLE statefulset WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- DaemonSet
DEFINE TABLE daemonset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON daemonset TYPE string;
DEFINE FIELD namespace ON daemonset TYPE string;
DEFINE FIELD daemonset ON daemonset TYPE string;
DEFINE FIELD updated_at ON daemonset TYPE int;
DEFINE FIELD active_windows ON daemonset TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON daemonset TYPE int;
DEFINE FIELD active_windows[*].end_time ON daemonset TYPE option<int>;
DEFINE INDEX idx_daemonset_unique ON daemonset FIELDS bcs_cluster_id, namespace, daemonset UNIQUE;

DEFINE EVENT lifecycle ON TABLE daemonset WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Job
DEFINE TABLE job SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON job TYPE string;
DEFINE FIELD namespace ON job TYPE string;
DEFINE FIELD job ON job TYPE string;
DEFINE FIELD updated_at ON job TYPE int;
DEFINE FIELD active_windows ON job TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON job TYPE int;
DEFINE FIELD active_windows[*].end_time ON job TYPE option<int>;
DEFINE INDEX idx_job_unique ON job FIELDS bcs_cluster_id, namespace, job UNIQUE;

DEFINE EVENT lifecycle ON TABLE job WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Service
DEFINE TABLE service SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON service TYPE string;
DEFINE FIELD namespace ON service TYPE string;
DEFINE FIELD service ON service TYPE string;
DEFINE FIELD updated_at ON service TYPE int;
DEFINE FIELD active_windows ON service TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON service TYPE int;
DEFINE FIELD active_windows[*].end_time ON service TYPE option<int>;
DEFINE INDEX idx_service_unique ON service FIELDS bcs_cluster_id, namespace, service UNIQUE;

DEFINE EVENT lifecycle ON TABLE service WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Ingress
DEFINE TABLE ingress SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON ingress TYPE string;
DEFINE FIELD namespace ON ingress TYPE string;
DEFINE FIELD ingress ON ingress TYPE string;
DEFINE FIELD updated_at ON ingress TYPE int;
DEFINE FIELD active_windows ON ingress TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON ingress TYPE int;
DEFINE FIELD active_windows[*].end_time ON ingress TYPE option<int>;
DEFINE INDEX idx_ingress_unique ON ingress FIELDS bcs_cluster_id, namespace, ingress UNIQUE;

DEFINE EVENT lifecycle ON TABLE ingress WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Cluster
DEFINE TABLE cluster SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON cluster TYPE string;
DEFINE FIELD updated_at ON cluster TYPE int;
DEFINE FIELD active_windows ON cluster TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON cluster TYPE int;
DEFINE FIELD active_windows[*].end_time ON cluster TYPE option<int>;
DEFINE INDEX idx_cluster_unique ON cluster FIELDS bcs_cluster_id UNIQUE;

DEFINE EVENT lifecycle ON TABLE cluster WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Namespace
DEFINE TABLE namespace SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON namespace TYPE string;
DEFINE FIELD namespace ON namespace TYPE string;
DEFINE FIELD updated_at ON namespace TYPE int;
DEFINE FIELD active_windows ON namespace TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON namespace TYPE int;
DEFINE FIELD active_windows[*].end_time ON namespace TYPE option<int>;
DEFINE INDEX idx_namespace_unique ON namespace FIELDS bcs_cluster_id, namespace UNIQUE;

DEFINE EVENT lifecycle ON TABLE namespace WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 3.2 Network Resources
-- ----------------------------------------------------------------------------

-- System
DEFINE TABLE system SCHEMAFULL;
DEFINE FIELD bk_cloud_id ON system TYPE string;
DEFINE FIELD bk_target_ip ON system TYPE string;
DEFINE FIELD updated_at ON system TYPE int;
DEFINE FIELD active_windows ON system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON system TYPE int;
DEFINE FIELD active_windows[*].end_time ON system TYPE option<int>;
DEFINE INDEX idx_system_unique ON system FIELDS bk_cloud_id, bk_target_ip UNIQUE;

DEFINE EVENT lifecycle ON TABLE system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- K8s Address
DEFINE TABLE k8s_address SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON k8s_address TYPE string;
DEFINE FIELD address ON k8s_address TYPE string;
DEFINE FIELD updated_at ON k8s_address TYPE int;
DEFINE FIELD active_windows ON k8s_address TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON k8s_address TYPE int;
DEFINE FIELD active_windows[*].end_time ON k8s_address TYPE option<int>;
DEFINE INDEX idx_k8s_address_unique ON k8s_address FIELDS bcs_cluster_id, address UNIQUE;

DEFINE EVENT lifecycle ON TABLE k8s_address WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Domain
DEFINE TABLE domain SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON domain TYPE string;
DEFINE FIELD domain ON domain TYPE string;
DEFINE FIELD updated_at ON domain TYPE int;
DEFINE FIELD active_windows ON domain TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON domain TYPE int;
DEFINE FIELD active_windows[*].end_time ON domain TYPE option<int>;
DEFINE INDEX idx_domain_unique ON domain FIELDS bcs_cluster_id, domain UNIQUE;

DEFINE EVENT lifecycle ON TABLE domain WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 3.3 APM Resources
-- ----------------------------------------------------------------------------

-- APM Service
DEFINE TABLE apm_service SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service TYPE string;
DEFINE FIELD apm_service_name ON apm_service TYPE string;
DEFINE FIELD updated_at ON apm_service TYPE int;
DEFINE FIELD active_windows ON apm_service TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON apm_service TYPE int;
DEFINE FIELD active_windows[*].end_time ON apm_service TYPE option<int>;
DEFINE INDEX idx_apm_service_unique ON apm_service FIELDS apm_application_name, apm_service_name UNIQUE;

DEFINE EVENT lifecycle ON TABLE apm_service WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- APM Service Instance
DEFINE TABLE apm_service_instance SCHEMAFULL;
DEFINE FIELD apm_application_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_name ON apm_service_instance TYPE string;
DEFINE FIELD apm_service_instance_name ON apm_service_instance TYPE string;
DEFINE FIELD updated_at ON apm_service_instance TYPE int;
DEFINE FIELD active_windows ON apm_service_instance TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON apm_service_instance TYPE int;
DEFINE FIELD active_windows[*].end_time ON apm_service_instance TYPE option<int>;
DEFINE INDEX idx_apm_service_instance_unique ON apm_service_instance FIELDS apm_application_name, apm_service_name, apm_service_instance_name UNIQUE;

DEFINE EVENT lifecycle ON TABLE apm_service_instance WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 3.4 Data Source Resources
-- ----------------------------------------------------------------------------

-- DataSource
DEFINE TABLE datasource SCHEMAFULL;
DEFINE FIELD bk_data_id ON datasource TYPE string;
DEFINE FIELD updated_at ON datasource TYPE int;
DEFINE FIELD active_windows ON datasource TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON datasource TYPE int;
DEFINE FIELD active_windows[*].end_time ON datasource TYPE option<int>;
DEFINE INDEX idx_datasource_unique ON datasource FIELDS bk_data_id UNIQUE;

DEFINE EVENT lifecycle ON TABLE datasource WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- BKLogConfig
DEFINE TABLE bklogconfig SCHEMAFULL;
DEFINE FIELD bklogconfig_namespace ON bklogconfig TYPE string;
DEFINE FIELD bklogconfig_name ON bklogconfig TYPE string;
DEFINE FIELD updated_at ON bklogconfig TYPE int;
DEFINE FIELD active_windows ON bklogconfig TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON bklogconfig TYPE int;
DEFINE FIELD active_windows[*].end_time ON bklogconfig TYPE option<int>;
DEFINE INDEX idx_bklogconfig_unique ON bklogconfig FIELDS bklogconfig_namespace, bklogconfig_name UNIQUE;

DEFINE EVENT lifecycle ON TABLE bklogconfig WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 3.5 CMDB Resources
-- ----------------------------------------------------------------------------

-- Biz
DEFINE TABLE biz SCHEMAFULL;
DEFINE FIELD bk_biz_id ON biz TYPE string;
DEFINE FIELD updated_at ON biz TYPE int;
DEFINE FIELD active_windows ON biz TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON biz TYPE int;
DEFINE FIELD active_windows[*].end_time ON biz TYPE option<int>;
DEFINE INDEX idx_biz_unique ON biz FIELDS bk_biz_id UNIQUE;

DEFINE EVENT lifecycle ON TABLE biz WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Set
DEFINE TABLE set SCHEMAFULL;
DEFINE FIELD bk_set_id ON set TYPE string;
DEFINE FIELD updated_at ON set TYPE int;
DEFINE FIELD active_windows ON set TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON set TYPE int;
DEFINE FIELD active_windows[*].end_time ON set TYPE option<int>;
DEFINE INDEX idx_set_unique ON set FIELDS bk_set_id UNIQUE;

DEFINE EVENT lifecycle ON TABLE set WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Module
DEFINE TABLE module SCHEMAFULL;
DEFINE FIELD bk_module_id ON module TYPE string;
DEFINE FIELD updated_at ON module TYPE int;
DEFINE FIELD active_windows ON module TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON module TYPE int;
DEFINE FIELD active_windows[*].end_time ON module TYPE option<int>;
DEFINE INDEX idx_module_unique ON module FIELDS bk_module_id UNIQUE;

DEFINE EVENT lifecycle ON TABLE module WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Host
DEFINE TABLE host SCHEMAFULL;
DEFINE FIELD bk_host_id ON host TYPE string;
DEFINE FIELD updated_at ON host TYPE int;
DEFINE FIELD active_windows ON host TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON host TYPE int;
DEFINE FIELD active_windows[*].end_time ON host TYPE option<int>;
DEFINE INDEX idx_host_unique ON host FIELDS bk_host_id UNIQUE;

DEFINE EVENT lifecycle ON TABLE host WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 3.6 App Version Resources
-- ----------------------------------------------------------------------------

-- App Version
DEFINE TABLE app_version SCHEMAFULL;
DEFINE FIELD app_name ON app_version TYPE string;
DEFINE FIELD version ON app_version TYPE string;
DEFINE FIELD updated_at ON app_version TYPE int;
DEFINE FIELD active_windows ON app_version TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON app_version TYPE int;
DEFINE FIELD active_windows[*].end_time ON app_version TYPE option<int>;
DEFINE INDEX idx_app_version_unique ON app_version FIELDS app_name, version UNIQUE;

DEFINE EVENT lifecycle ON TABLE app_version WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Git Commit
DEFINE TABLE git_commit SCHEMAFULL;
DEFINE FIELD git_repo ON git_commit TYPE string;
DEFINE FIELD commit_id ON git_commit TYPE string;
DEFINE FIELD updated_at ON git_commit TYPE int;
DEFINE FIELD active_windows ON git_commit TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON git_commit TYPE int;
DEFINE FIELD active_windows[*].end_time ON git_commit TYPE option<int>;
DEFINE INDEX idx_git_commit_unique ON git_commit FIELDS git_repo, commit_id UNIQUE;

DEFINE EVENT lifecycle ON TABLE git_commit WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- Environment
DEFINE TABLE environment SCHEMAFULL;
DEFINE FIELD environment ON environment TYPE string;
DEFINE FIELD updated_at ON environment TYPE int;
DEFINE FIELD active_windows ON environment TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON environment TYPE int;
DEFINE FIELD active_windows[*].end_time ON environment TYPE option<int>;
DEFINE INDEX idx_environment_unique ON environment FIELDS environment UNIQUE;

DEFINE EVENT lifecycle ON TABLE environment WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 3.7 Metric Resource
-- ----------------------------------------------------------------------------

-- Metric
DEFINE TABLE metric SCHEMAFULL;
DEFINE FIELD metric_name ON metric TYPE string;
DEFINE FIELD metric_type ON metric TYPE string;
DEFINE FIELD unit ON metric TYPE option<string>;
DEFINE FIELD description ON metric TYPE option<string>;
DEFINE FIELD updated_at ON metric TYPE int;
DEFINE FIELD active_windows ON metric TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON metric TYPE int;
DEFINE FIELD active_windows[*].end_time ON metric TYPE option<int>;
DEFINE INDEX idx_metric_unique ON metric FIELDS metric_name UNIQUE;

DEFINE EVENT lifecycle ON TABLE metric WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ============================================================================
-- SECTION 4: Relation Tables with Event-based Lifecycle Management
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 4.1 Kubernetes Static Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE node_with_system SCHEMAFULL TYPE RELATION FROM node TO system;
DEFINE FIELD updated_at ON node_with_system TYPE int;
DEFINE FIELD active_windows ON node_with_system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON node_with_system TYPE int;
DEFINE FIELD active_windows[*].end_time ON node_with_system TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE node_with_system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION FROM node TO pod;
DEFINE FIELD updated_at ON node_with_pod TYPE int;
DEFINE FIELD active_windows ON node_with_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON node_with_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON node_with_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE node_with_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE job_with_pod SCHEMAFULL TYPE RELATION FROM job TO pod;
DEFINE FIELD updated_at ON job_with_pod TYPE int;
DEFINE FIELD active_windows ON job_with_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON job_with_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON job_with_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE job_with_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE pod_with_replicaset SCHEMAFULL TYPE RELATION FROM pod TO replicaset;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE int;
DEFINE FIELD active_windows ON pod_with_replicaset TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON pod_with_replicaset TYPE int;
DEFINE FIELD active_windows[*].end_time ON pod_with_replicaset TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE pod_with_replicaset WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE pod_with_statefulset SCHEMAFULL TYPE RELATION FROM pod TO statefulset;
DEFINE FIELD updated_at ON pod_with_statefulset TYPE int;
DEFINE FIELD active_windows ON pod_with_statefulset TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON pod_with_statefulset TYPE int;
DEFINE FIELD active_windows[*].end_time ON pod_with_statefulset TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE pod_with_statefulset WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE daemonset_with_pod SCHEMAFULL TYPE RELATION FROM daemonset TO pod;
DEFINE FIELD updated_at ON daemonset_with_pod TYPE int;
DEFINE FIELD active_windows ON daemonset_with_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON daemonset_with_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON daemonset_with_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE daemonset_with_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE deployment_with_replicaset SCHEMAFULL TYPE RELATION FROM deployment TO replicaset;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE int;
DEFINE FIELD active_windows ON deployment_with_replicaset TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON deployment_with_replicaset TYPE int;
DEFINE FIELD active_windows[*].end_time ON deployment_with_replicaset TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE deployment_with_replicaset WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION FROM pod TO service;
DEFINE FIELD updated_at ON pod_with_service TYPE int;
DEFINE FIELD active_windows ON pod_with_service TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON pod_with_service TYPE int;
DEFINE FIELD active_windows[*].end_time ON pod_with_service TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE pod_with_service WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE ingress_with_service SCHEMAFULL TYPE RELATION FROM ingress TO service;
DEFINE FIELD updated_at ON ingress_with_service TYPE int;
DEFINE FIELD active_windows ON ingress_with_service TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON ingress_with_service TYPE int;
DEFINE FIELD active_windows[*].end_time ON ingress_with_service TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE ingress_with_service WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.2 Network Static Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE k8s_address_with_service SCHEMAFULL TYPE RELATION FROM k8s_address TO service;
DEFINE FIELD updated_at ON k8s_address_with_service TYPE int;
DEFINE FIELD active_windows ON k8s_address_with_service TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON k8s_address_with_service TYPE int;
DEFINE FIELD active_windows[*].end_time ON k8s_address_with_service TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE k8s_address_with_service WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE domain_with_service SCHEMAFULL TYPE RELATION FROM domain TO service;
DEFINE FIELD updated_at ON domain_with_service TYPE int;
DEFINE FIELD active_windows ON domain_with_service TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON domain_with_service TYPE int;
DEFINE FIELD active_windows[*].end_time ON domain_with_service TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE domain_with_service WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.3 APM Static Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE apm_service_instance_with_pod SCHEMAFULL TYPE RELATION FROM apm_service_instance TO pod;
DEFINE FIELD updated_at ON apm_service_instance_with_pod TYPE int;
DEFINE FIELD active_windows ON apm_service_instance_with_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON apm_service_instance_with_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON apm_service_instance_with_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE apm_service_instance_with_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE apm_service_instance_with_system SCHEMAFULL TYPE RELATION FROM apm_service_instance TO system;
DEFINE FIELD updated_at ON apm_service_instance_with_system TYPE int;
DEFINE FIELD active_windows ON apm_service_instance_with_system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON apm_service_instance_with_system TYPE int;
DEFINE FIELD active_windows[*].end_time ON apm_service_instance_with_system TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE apm_service_instance_with_system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE apm_service_with_apm_service_instance SCHEMAFULL TYPE RELATION FROM apm_service TO apm_service_instance;
DEFINE FIELD updated_at ON apm_service_with_apm_service_instance TYPE int;
DEFINE FIELD active_windows ON apm_service_with_apm_service_instance TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON apm_service_with_apm_service_instance TYPE int;
DEFINE FIELD active_windows[*].end_time ON apm_service_with_apm_service_instance TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE apm_service_with_apm_service_instance WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.4 Container Static Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE container_with_pod SCHEMAFULL TYPE RELATION FROM container TO pod;
DEFINE FIELD updated_at ON container_with_pod TYPE int;
DEFINE FIELD active_windows ON container_with_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON container_with_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON container_with_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE container_with_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.5 Data Source Static Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE datasource_with_pod SCHEMAFULL TYPE RELATION FROM datasource TO pod;
DEFINE FIELD updated_at ON datasource_with_pod TYPE int;
DEFINE FIELD active_windows ON datasource_with_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON datasource_with_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON datasource_with_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE datasource_with_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE datasource_with_node SCHEMAFULL TYPE RELATION FROM datasource TO node;
DEFINE FIELD updated_at ON datasource_with_node TYPE int;
DEFINE FIELD active_windows ON datasource_with_node TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON datasource_with_node TYPE int;
DEFINE FIELD active_windows[*].end_time ON datasource_with_node TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE datasource_with_node WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE bklogconfig_with_datasource SCHEMAFULL TYPE RELATION FROM bklogconfig TO datasource;
DEFINE FIELD updated_at ON bklogconfig_with_datasource TYPE int;
DEFINE FIELD active_windows ON bklogconfig_with_datasource TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON bklogconfig_with_datasource TYPE int;
DEFINE FIELD active_windows[*].end_time ON bklogconfig_with_datasource TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE bklogconfig_with_datasource WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.6 CMDB Static Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE biz_with_set SCHEMAFULL TYPE RELATION FROM biz TO set;
DEFINE FIELD updated_at ON biz_with_set TYPE int;
DEFINE FIELD active_windows ON biz_with_set TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON biz_with_set TYPE int;
DEFINE FIELD active_windows[*].end_time ON biz_with_set TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE biz_with_set WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE module_with_set SCHEMAFULL TYPE RELATION FROM module TO set;
DEFINE FIELD updated_at ON module_with_set TYPE int;
DEFINE FIELD active_windows ON module_with_set TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON module_with_set TYPE int;
DEFINE FIELD active_windows[*].end_time ON module_with_set TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE module_with_set WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE host_with_module SCHEMAFULL TYPE RELATION FROM host TO module;
DEFINE FIELD updated_at ON host_with_module TYPE int;
DEFINE FIELD active_windows ON host_with_module TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON host_with_module TYPE int;
DEFINE FIELD active_windows[*].end_time ON host_with_module TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE host_with_module WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE host_with_system SCHEMAFULL TYPE RELATION FROM host TO system;
DEFINE FIELD updated_at ON host_with_system TYPE int;
DEFINE FIELD active_windows ON host_with_system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON host_with_system TYPE int;
DEFINE FIELD active_windows[*].end_time ON host_with_system TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE host_with_system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.7 App Version Static Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE app_version_with_container SCHEMAFULL TYPE RELATION FROM app_version TO container;
DEFINE FIELD updated_at ON app_version_with_container TYPE int;
DEFINE FIELD active_windows ON app_version_with_container TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON app_version_with_container TYPE int;
DEFINE FIELD active_windows[*].end_time ON app_version_with_container TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE app_version_with_container WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE app_version_with_system SCHEMAFULL TYPE RELATION FROM app_version TO system;
DEFINE FIELD updated_at ON app_version_with_system TYPE int;
DEFINE FIELD active_windows ON app_version_with_system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON app_version_with_system TYPE int;
DEFINE FIELD active_windows[*].end_time ON app_version_with_system TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE app_version_with_system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE container_with_environment SCHEMAFULL TYPE RELATION FROM container TO environment;
DEFINE FIELD updated_at ON container_with_environment TYPE int;
DEFINE FIELD active_windows ON container_with_environment TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON container_with_environment TYPE int;
DEFINE FIELD active_windows[*].end_time ON container_with_environment TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE container_with_environment WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE environment_with_system SCHEMAFULL TYPE RELATION FROM environment TO system;
DEFINE FIELD updated_at ON environment_with_system TYPE int;
DEFINE FIELD active_windows ON environment_with_system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON environment_with_system TYPE int;
DEFINE FIELD active_windows[*].end_time ON environment_with_system TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE environment_with_system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE app_version_with_git_commit SCHEMAFULL TYPE RELATION FROM app_version TO git_commit;
DEFINE FIELD updated_at ON app_version_with_git_commit TYPE int;
DEFINE FIELD active_windows ON app_version_with_git_commit TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON app_version_with_git_commit TYPE int;
DEFINE FIELD active_windows[*].end_time ON app_version_with_git_commit TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE app_version_with_git_commit WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.8 Dynamic Relations (Traffic Flow)
-- ----------------------------------------------------------------------------

DEFINE TABLE pod_to_pod SCHEMAFULL TYPE RELATION FROM pod TO pod;
DEFINE FIELD updated_at ON pod_to_pod TYPE int;
DEFINE FIELD active_windows ON pod_to_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON pod_to_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON pod_to_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE pod_to_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE pod_to_system SCHEMAFULL TYPE RELATION FROM pod TO system;
DEFINE FIELD updated_at ON pod_to_system TYPE int;
DEFINE FIELD active_windows ON pod_to_system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON pod_to_system TYPE int;
DEFINE FIELD active_windows[*].end_time ON pod_to_system TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE pod_to_system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE system_to_pod SCHEMAFULL TYPE RELATION FROM system TO pod;
DEFINE FIELD updated_at ON system_to_pod TYPE int;
DEFINE FIELD active_windows ON system_to_pod TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON system_to_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON system_to_pod TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE system_to_pod WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE system_to_system SCHEMAFULL TYPE RELATION FROM system TO system;
DEFINE FIELD updated_at ON system_to_system TYPE int;
DEFINE FIELD active_windows ON system_to_system TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON system_to_system TYPE int;
DEFINE FIELD active_windows[*].end_time ON system_to_system TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE system_to_system WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE service_to_service SCHEMAFULL TYPE RELATION FROM service TO service;
DEFINE FIELD updated_at ON service_to_service TYPE int;
DEFINE FIELD active_windows ON service_to_service TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON service_to_service TYPE int;
DEFINE FIELD active_windows[*].end_time ON service_to_service TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE service_to_service WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ----------------------------------------------------------------------------
-- 4.9 Metric Relations
-- ----------------------------------------------------------------------------

DEFINE TABLE node_has_metric SCHEMAFULL TYPE RELATION FROM node TO metric;
DEFINE FIELD result_table_id ON node_has_metric TYPE string;
DEFINE FIELD updated_at ON node_has_metric TYPE int;
DEFINE FIELD active_windows ON node_has_metric TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON node_has_metric TYPE int;
DEFINE FIELD active_windows[*].end_time ON node_has_metric TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE node_has_metric WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

DEFINE TABLE relation_has_metric SCHEMAFULL TYPE RELATION FROM pod_to_pod|pod_to_system|system_to_pod|system_to_system|service_to_service TO metric;
DEFINE FIELD result_table_id ON relation_has_metric TYPE string;
DEFINE FIELD updated_at ON relation_has_metric TYPE int;
DEFINE FIELD active_windows ON relation_has_metric TYPE array<object> DEFAULT [];
DEFINE FIELD active_windows[*].start_time ON relation_has_metric TYPE int;
DEFINE FIELD active_windows[*].end_time ON relation_has_metric TYPE option<int>;

DEFINE EVENT lifecycle ON TABLE relation_has_metric WHEN $event = "CREATE" OR ($event = "UPDATE" AND $before.updated_at != $after.updated_at) THEN {
    LET $tolerance = {tolerance_time_ms};
    LET $new_windows = IF $before.active_windows == NONE OR array::len($before.active_windows) == 0 THEN {
        [{ start_time: $after.updated_at, end_time: NONE }]
    } ELSE IF $before.updated_at != NONE AND ($after.updated_at - $before.updated_at) <= $tolerance THEN {
        $before.active_windows
    } ELSE {
        LET $last_idx = array::len($before.active_windows) - 1;
        LET $last_window = $before.active_windows[$last_idx];
        LET $closed_window = { start_time: $last_window.start_time, end_time: $before.updated_at };
        array::concat(
            array::slice($before.active_windows, 0, $last_idx),
            [$closed_window, { start_time: $after.updated_at, end_time: NONE }]
        )
    } END;
    UPDATE $after.id SET active_windows = $new_windows;
};

-- ============================================================================
-- End of Schema V2 - Active Windows with Event-only Lifecycle Management
-- ============================================================================
