-- ============================================================================
-- SurrealDB Schema - Plan 03: Liveness Record (驱动标记方案)
-- 
-- This schema implements the "liveness_record" approach where:
-- - Each resource table has a corresponding _liveness_record table
-- - Events automatically manage liveness records on CREATE/UPDATE
-- - Separate tables for lifecycle tracking (not embedded in resource table)
--
-- Key Features:
--   - Separate liveness_record tables for each resource type
--   - Events auto-create records on CREATE
--   - Events extend period_end if within tolerance on UPDATE
--   - Events close old record and create new if beyond tolerance
--   - is_active flag for easy cleanup and query optimization
--
-- Write Pattern:
--   UPSERT pod:⟨pod-0:default:BCS-K8S-001⟩ MERGE {
--       bcs_cluster_id: "BCS-K8S-001",
--       namespace: "default", 
--       pod: "pod-0",
--       updated_at: <timestamp_seconds>  -- Client必须传入
--   };
--
-- The Event will automatically:
--   - Create liveness_record on first insert
--   - Extend period_end if within tolerance
--   - Close old record and create new if beyond tolerance
--
-- Author: Auto-generated for BK Monitor - Plan 03
-- ============================================================================

-- ============================================================================
-- SECTION 1: Drop Existing Tables (for clean reset)
-- ============================================================================

-- Drop resource tables
REMOVE TABLE IF EXISTS pod;
REMOVE TABLE IF EXISTS node;
REMOVE TABLE IF EXISTS container;
REMOVE TABLE IF EXISTS deployment;
REMOVE TABLE IF EXISTS replicaset;
REMOVE TABLE IF EXISTS service;
REMOVE TABLE IF EXISTS system;

-- Drop liveness record tables
REMOVE TABLE IF EXISTS pod_liveness_record;
REMOVE TABLE IF EXISTS node_liveness_record;
REMOVE TABLE IF EXISTS container_liveness_record;
REMOVE TABLE IF EXISTS deployment_liveness_record;
REMOVE TABLE IF EXISTS replicaset_liveness_record;
REMOVE TABLE IF EXISTS service_liveness_record;
REMOVE TABLE IF EXISTS system_liveness_record;

-- Drop relation tables
REMOVE TABLE IF EXISTS node_with_pod;
REMOVE TABLE IF EXISTS node_with_system;
REMOVE TABLE IF EXISTS container_with_pod;
REMOVE TABLE IF EXISTS pod_with_service;
REMOVE TABLE IF EXISTS deployment_with_replicaset;
REMOVE TABLE IF EXISTS pod_with_replicaset;

-- Drop relation liveness record tables
REMOVE TABLE IF EXISTS node_with_pod_liveness_record;
REMOVE TABLE IF EXISTS node_with_system_liveness_record;
REMOVE TABLE IF EXISTS container_with_pod_liveness_record;
REMOVE TABLE IF EXISTS pod_with_service_liveness_record;
REMOVE TABLE IF EXISTS deployment_with_replicaset_liveness_record;
REMOVE TABLE IF EXISTS pod_with_replicaset_liveness_record;

-- ============================================================================
-- SECTION 2: Helper Functions
-- ============================================================================

REMOVE FUNCTION IF EXISTS fn::kv_block;
REMOVE FUNCTION IF EXISTS fn::relation_id;
REMOVE FUNCTION IF EXISTS fn::upsert_relation;
REMOVE FUNCTION IF EXISTS fn::check_liveness_range_exists;
REMOVE FUNCTION IF EXISTS fn::is_alive_at;
REMOVE FUNCTION IF EXISTS fn::get_liveness_records;

-- ============================================================================
-- SECTION 3: Pod Table and Liveness Record
-- ============================================================================

-- ------------------------------
-- TABLE: pod 信息主表
-- ------------------------------
DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD created_at ON pod TYPE option<int>;
DEFINE FIELD updated_at ON pod TYPE int;

-- ------------------------------
-- TABLE: pod 续期记录表
-- ------------------------------
DEFINE TABLE pod_liveness_record SCHEMAFULL;
DEFINE FIELD pod_id ON pod_liveness_record TYPE record<pod>;
DEFINE FIELD period_start ON pod_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_liveness_record TYPE int;

-- Index for efficient queries
DEFINE INDEX idx_pod_liveness_pod_id ON pod_liveness_record FIELDS pod_id;
DEFINE INDEX idx_pod_liveness_active ON pod_liveness_record FIELDS pod_id, is_active;
DEFINE INDEX idx_pod_liveness_period ON pod_liveness_record FIELDS pod_id, period_start, period_end;

-- ========================================
-- Pod 信息创建事件驱动
-- 功能：当 Pod 信息创建时，创建新的续期记录
-- ========================================
DEFINE EVENT OVERWRITE event_pod_created ON TABLE pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE pod_liveness_record SET 
        pod_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

-- ========================================
-- Pod 信息更新事件驱动（过期）
-- 功能：超过容忍间隔则新建续期记录，关闭旧的续期记录
-- ========================================
DEFINE EVENT OVERWRITE event_pod_updated_expired ON TABLE pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM pod_liveness_record WHERE pod_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE pod_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE pod_liveness_record SET 
        pod_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

-- ========================================
-- Pod 信息更新事件驱动（续期）
-- 功能：不超过容忍间隔，更新续期记录
-- ========================================
DEFINE EVENT OVERWRITE event_pod_updated_active ON TABLE pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM pod_liveness_record WHERE pod_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE pod_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 4: Node Table and Liveness Record
-- ============================================================================

DEFINE TABLE node SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON node TYPE string;
DEFINE FIELD node ON node TYPE string;
DEFINE FIELD created_at ON node TYPE option<int>;
DEFINE FIELD updated_at ON node TYPE int;

DEFINE TABLE node_liveness_record SCHEMAFULL;
DEFINE FIELD node_id ON node_liveness_record TYPE record<node>;
DEFINE FIELD period_start ON node_liveness_record TYPE int;
DEFINE FIELD period_end ON node_liveness_record TYPE int;
DEFINE FIELD is_active ON node_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON node_liveness_record TYPE int;
DEFINE FIELD updated_at ON node_liveness_record TYPE int;

DEFINE INDEX idx_node_liveness_node_id ON node_liveness_record FIELDS node_id;
DEFINE INDEX idx_node_liveness_active ON node_liveness_record FIELDS node_id, is_active;

DEFINE EVENT OVERWRITE event_node_created ON TABLE node 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE node_liveness_record SET 
        node_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_node_updated_expired ON TABLE node 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM node_liveness_record WHERE node_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE node_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE node_liveness_record SET 
        node_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_node_updated_active ON TABLE node 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM node_liveness_record WHERE node_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE node_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 5: Container Table and Liveness Record
-- ============================================================================

DEFINE TABLE container SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON container TYPE string;
DEFINE FIELD namespace ON container TYPE string;
DEFINE FIELD pod ON container TYPE string;
DEFINE FIELD container ON container TYPE string;
DEFINE FIELD created_at ON container TYPE option<int>;
DEFINE FIELD updated_at ON container TYPE int;

DEFINE TABLE container_liveness_record SCHEMAFULL;
DEFINE FIELD container_id ON container_liveness_record TYPE record<container>;
DEFINE FIELD period_start ON container_liveness_record TYPE int;
DEFINE FIELD period_end ON container_liveness_record TYPE int;
DEFINE FIELD is_active ON container_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON container_liveness_record TYPE int;
DEFINE FIELD updated_at ON container_liveness_record TYPE int;

DEFINE INDEX idx_container_liveness_container_id ON container_liveness_record FIELDS container_id;
DEFINE INDEX idx_container_liveness_active ON container_liveness_record FIELDS container_id, is_active;

DEFINE EVENT OVERWRITE event_container_created ON TABLE container 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE container_liveness_record SET 
        container_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_container_updated_expired ON TABLE container 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM container_liveness_record WHERE container_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE container_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE container_liveness_record SET 
        container_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_container_updated_active ON TABLE container 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM container_liveness_record WHERE container_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE container_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 6: Deployment Table and Liveness Record
-- ============================================================================

DEFINE TABLE deployment SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON deployment TYPE string;
DEFINE FIELD namespace ON deployment TYPE string;
DEFINE FIELD deployment ON deployment TYPE string;
DEFINE FIELD created_at ON deployment TYPE option<int>;
DEFINE FIELD updated_at ON deployment TYPE int;

DEFINE TABLE deployment_liveness_record SCHEMAFULL;
DEFINE FIELD deployment_id ON deployment_liveness_record TYPE record<deployment>;
DEFINE FIELD period_start ON deployment_liveness_record TYPE int;
DEFINE FIELD period_end ON deployment_liveness_record TYPE int;
DEFINE FIELD is_active ON deployment_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON deployment_liveness_record TYPE int;
DEFINE FIELD updated_at ON deployment_liveness_record TYPE int;

DEFINE INDEX idx_deployment_liveness_deployment_id ON deployment_liveness_record FIELDS deployment_id;
DEFINE INDEX idx_deployment_liveness_active ON deployment_liveness_record FIELDS deployment_id, is_active;

DEFINE EVENT OVERWRITE event_deployment_created ON TABLE deployment 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE deployment_liveness_record SET 
        deployment_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_deployment_updated_expired ON TABLE deployment 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM deployment_liveness_record WHERE deployment_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE deployment_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE deployment_liveness_record SET 
        deployment_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_deployment_updated_active ON TABLE deployment 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM deployment_liveness_record WHERE deployment_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE deployment_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 7: ReplicaSet Table and Liveness Record
-- ============================================================================

DEFINE TABLE replicaset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON replicaset TYPE string;
DEFINE FIELD namespace ON replicaset TYPE string;
DEFINE FIELD replicaset ON replicaset TYPE string;
DEFINE FIELD created_at ON replicaset TYPE option<int>;
DEFINE FIELD updated_at ON replicaset TYPE int;

DEFINE TABLE replicaset_liveness_record SCHEMAFULL;
DEFINE FIELD replicaset_id ON replicaset_liveness_record TYPE record<replicaset>;
DEFINE FIELD period_start ON replicaset_liveness_record TYPE int;
DEFINE FIELD period_end ON replicaset_liveness_record TYPE int;
DEFINE FIELD is_active ON replicaset_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON replicaset_liveness_record TYPE int;
DEFINE FIELD updated_at ON replicaset_liveness_record TYPE int;

DEFINE INDEX idx_replicaset_liveness_replicaset_id ON replicaset_liveness_record FIELDS replicaset_id;
DEFINE INDEX idx_replicaset_liveness_active ON replicaset_liveness_record FIELDS replicaset_id, is_active;

DEFINE EVENT OVERWRITE event_replicaset_created ON TABLE replicaset 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE replicaset_liveness_record SET 
        replicaset_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_replicaset_updated_expired ON TABLE replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM replicaset_liveness_record WHERE replicaset_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE replicaset_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE replicaset_liveness_record SET 
        replicaset_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_replicaset_updated_active ON TABLE replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM replicaset_liveness_record WHERE replicaset_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE replicaset_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 8: Service Table and Liveness Record
-- ============================================================================

DEFINE TABLE service SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON service TYPE string;
DEFINE FIELD namespace ON service TYPE string;
DEFINE FIELD service ON service TYPE string;
DEFINE FIELD created_at ON service TYPE option<int>;
DEFINE FIELD updated_at ON service TYPE int;

DEFINE TABLE service_liveness_record SCHEMAFULL;
DEFINE FIELD service_id ON service_liveness_record TYPE record<service>;
DEFINE FIELD period_start ON service_liveness_record TYPE int;
DEFINE FIELD period_end ON service_liveness_record TYPE int;
DEFINE FIELD is_active ON service_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON service_liveness_record TYPE int;
DEFINE FIELD updated_at ON service_liveness_record TYPE int;

DEFINE INDEX idx_service_liveness_service_id ON service_liveness_record FIELDS service_id;
DEFINE INDEX idx_service_liveness_active ON service_liveness_record FIELDS service_id, is_active;

DEFINE EVENT OVERWRITE event_service_created ON TABLE service 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE service_liveness_record SET 
        service_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_service_updated_expired ON TABLE service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM service_liveness_record WHERE service_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE service_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE service_liveness_record SET 
        service_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_service_updated_active ON TABLE service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM service_liveness_record WHERE service_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE service_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 9: System Table and Liveness Record
-- ============================================================================

DEFINE TABLE system SCHEMAFULL;
DEFINE FIELD bk_target_ip ON system TYPE string;
DEFINE FIELD bk_cloud_id ON system TYPE string;
DEFINE FIELD created_at ON system TYPE option<int>;
DEFINE FIELD updated_at ON system TYPE int;

DEFINE TABLE system_liveness_record SCHEMAFULL;
DEFINE FIELD system_id ON system_liveness_record TYPE record<system>;
DEFINE FIELD period_start ON system_liveness_record TYPE int;
DEFINE FIELD period_end ON system_liveness_record TYPE int;
DEFINE FIELD is_active ON system_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON system_liveness_record TYPE int;
DEFINE FIELD updated_at ON system_liveness_record TYPE int;

DEFINE INDEX idx_system_liveness_system_id ON system_liveness_record FIELDS system_id;
DEFINE INDEX idx_system_liveness_active ON system_liveness_record FIELDS system_id, is_active;

DEFINE EVENT OVERWRITE event_system_created ON TABLE system 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE system_liveness_record SET 
        system_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_system_updated_expired ON TABLE system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM system_liveness_record WHERE system_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE system_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE system_liveness_record SET 
        system_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_system_updated_active ON TABLE system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM system_liveness_record WHERE system_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE system_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 10: Relation Tables with Liveness Record
-- ============================================================================

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

-- fn::upsert_relation: Universal relation upsert function for all relation tables
DEFINE FUNCTION fn::upsert_relation($relation_table: string, $from_id: record, $to_id: record, $updated_at: int) {
    LET $rel_id = fn::relation_id($from_id, $to_id);
    LET $full_id = type::thing($relation_table, $rel_id);
    LET $rel_table = type::table($relation_table);
    LET $existing = (SELECT * FROM type::table($relation_table) WHERE id = $full_id LIMIT 1)[0];
    RETURN IF $existing != NONE THEN
        (UPDATE $existing.id SET updated_at = $updated_at)[0]
    ELSE
        (RELATE $from_id->$rel_table->$to_id SET id = $full_id, updated_at = $updated_at)[0]
    END;
};

-- node_with_pod relation
DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION FROM node TO pod;
DEFINE FIELD created_at ON node_with_pod TYPE option<int>;
DEFINE FIELD updated_at ON node_with_pod TYPE int;

DEFINE TABLE node_with_pod_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON node_with_pod_liveness_record TYPE record<node_with_pod>;
DEFINE FIELD period_start ON node_with_pod_liveness_record TYPE int;
DEFINE FIELD period_end ON node_with_pod_liveness_record TYPE int;
DEFINE FIELD is_active ON node_with_pod_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON node_with_pod_liveness_record TYPE int;
DEFINE FIELD updated_at ON node_with_pod_liveness_record TYPE int;

DEFINE INDEX idx_node_with_pod_liveness_relation_id ON node_with_pod_liveness_record FIELDS relation_id;
DEFINE INDEX idx_node_with_pod_liveness_active ON node_with_pod_liveness_record FIELDS relation_id, is_active;

DEFINE EVENT OVERWRITE event_node_with_pod_created ON TABLE node_with_pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE node_with_pod_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_node_with_pod_updated_expired ON TABLE node_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM node_with_pod_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE node_with_pod_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE node_with_pod_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_node_with_pod_updated_active ON TABLE node_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM node_with_pod_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE node_with_pod_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- container_with_pod relation
DEFINE TABLE container_with_pod SCHEMAFULL TYPE RELATION FROM container TO pod;
DEFINE FIELD created_at ON container_with_pod TYPE option<int>;
DEFINE FIELD updated_at ON container_with_pod TYPE int;

DEFINE TABLE container_with_pod_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON container_with_pod_liveness_record TYPE record<container_with_pod>;
DEFINE FIELD period_start ON container_with_pod_liveness_record TYPE int;
DEFINE FIELD period_end ON container_with_pod_liveness_record TYPE int;
DEFINE FIELD is_active ON container_with_pod_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON container_with_pod_liveness_record TYPE int;
DEFINE FIELD updated_at ON container_with_pod_liveness_record TYPE int;

DEFINE INDEX idx_container_with_pod_liveness_relation_id ON container_with_pod_liveness_record FIELDS relation_id;

DEFINE EVENT OVERWRITE event_container_with_pod_created ON TABLE container_with_pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE container_with_pod_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_container_with_pod_updated_expired ON TABLE container_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM container_with_pod_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE container_with_pod_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE container_with_pod_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_container_with_pod_updated_active ON TABLE container_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM container_with_pod_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE container_with_pod_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- pod_with_service relation
DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION FROM pod TO service;
DEFINE FIELD created_at ON pod_with_service TYPE option<int>;
DEFINE FIELD updated_at ON pod_with_service TYPE int;

DEFINE TABLE pod_with_service_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON pod_with_service_liveness_record TYPE record<pod_with_service>;
DEFINE FIELD period_start ON pod_with_service_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_with_service_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_with_service_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_with_service_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_with_service_liveness_record TYPE int;

DEFINE INDEX idx_pod_with_service_liveness_relation_id ON pod_with_service_liveness_record FIELDS relation_id;

DEFINE EVENT OVERWRITE event_pod_with_service_created ON TABLE pod_with_service 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE pod_with_service_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_with_service_updated_expired ON TABLE pod_with_service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM pod_with_service_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE pod_with_service_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE pod_with_service_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_with_service_updated_active ON TABLE pod_with_service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM pod_with_service_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE pod_with_service_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- deployment_with_replicaset relation
DEFINE TABLE deployment_with_replicaset SCHEMAFULL TYPE RELATION FROM deployment TO replicaset;
DEFINE FIELD created_at ON deployment_with_replicaset TYPE option<int>;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE int;

DEFINE TABLE deployment_with_replicaset_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON deployment_with_replicaset_liveness_record TYPE record<deployment_with_replicaset>;
DEFINE FIELD period_start ON deployment_with_replicaset_liveness_record TYPE int;
DEFINE FIELD period_end ON deployment_with_replicaset_liveness_record TYPE int;
DEFINE FIELD is_active ON deployment_with_replicaset_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON deployment_with_replicaset_liveness_record TYPE int;
DEFINE FIELD updated_at ON deployment_with_replicaset_liveness_record TYPE int;

DEFINE INDEX idx_deployment_with_replicaset_liveness_relation_id ON deployment_with_replicaset_liveness_record FIELDS relation_id;

DEFINE EVENT OVERWRITE event_deployment_with_replicaset_created ON TABLE deployment_with_replicaset 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE deployment_with_replicaset_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_deployment_with_replicaset_updated_expired ON TABLE deployment_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM deployment_with_replicaset_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE deployment_with_replicaset_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE deployment_with_replicaset_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_deployment_with_replicaset_updated_active ON TABLE deployment_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM deployment_with_replicaset_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE deployment_with_replicaset_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- pod_with_replicaset relation
DEFINE TABLE pod_with_replicaset SCHEMAFULL TYPE RELATION FROM pod TO replicaset;
DEFINE FIELD created_at ON pod_with_replicaset TYPE option<int>;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE int;

DEFINE TABLE pod_with_replicaset_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON pod_with_replicaset_liveness_record TYPE record<pod_with_replicaset>;
DEFINE FIELD period_start ON pod_with_replicaset_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_with_replicaset_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_with_replicaset_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_with_replicaset_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_with_replicaset_liveness_record TYPE int;

DEFINE INDEX idx_pod_with_replicaset_liveness_relation_id ON pod_with_replicaset_liveness_record FIELDS relation_id;

DEFINE EVENT OVERWRITE event_pod_with_replicaset_created ON TABLE pod_with_replicaset 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE pod_with_replicaset_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_with_replicaset_updated_expired ON TABLE pod_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM pod_with_replicaset_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE pod_with_replicaset_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE pod_with_replicaset_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_with_replicaset_updated_active ON TABLE pod_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM pod_with_replicaset_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE pod_with_replicaset_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- node_with_system relation
DEFINE TABLE node_with_system SCHEMAFULL TYPE RELATION FROM node TO system;
DEFINE FIELD created_at ON node_with_system TYPE option<int>;
DEFINE FIELD updated_at ON node_with_system TYPE int;

DEFINE TABLE node_with_system_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON node_with_system_liveness_record TYPE record<node_with_system>;
DEFINE FIELD period_start ON node_with_system_liveness_record TYPE int;
DEFINE FIELD period_end ON node_with_system_liveness_record TYPE int;
DEFINE FIELD is_active ON node_with_system_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON node_with_system_liveness_record TYPE int;
DEFINE FIELD updated_at ON node_with_system_liveness_record TYPE int;

DEFINE INDEX idx_node_with_system_liveness_relation_id ON node_with_system_liveness_record FIELDS relation_id;

DEFINE EVENT OVERWRITE event_node_with_system_created ON TABLE node_with_system 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE node_with_system_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_node_with_system_updated_expired ON TABLE node_with_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM node_with_system_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE node_with_system_liveness_record SET is_active = false WHERE id = $last_record.id AND is_active = true
    END;
    CREATE node_with_system_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_node_with_system_updated_active ON TABLE node_with_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $last_record = (SELECT * FROM node_with_system_liveness_record WHERE relation_id = $after.id AND is_active = true ORDER BY created_at DESC LIMIT 1)[0];
    IF $last_record != NONE THEN
        UPDATE node_with_system_liveness_record SET updated_at = $after.updated_at, period_end = $after.updated_at WHERE id = $last_record.id AND is_active = true
    END;
};

-- ============================================================================
-- SECTION 11: Helper Query Functions
-- ============================================================================

-- fn::check_liveness_range_exists: Check if a record has liveness in time range
DEFINE FUNCTION OVERWRITE fn::check_liveness_range_exists($table_suffix: string, $record_id: record, $start_time: number, $end_time: number) -> bool {
    LET $table_name = type::table($table_suffix + "_liveness_record");
    LET $field_name = $table_suffix + "_id";
    LET $result = SELECT count() as cnt FROM $table_name WHERE type::field($field_name) = $record_id AND $start_time <= period_end AND $end_time >= period_start GROUP ALL;
    RETURN $result[0].cnt > 0;
};

-- fn::is_alive_at: Check if a record is alive at a specific time
DEFINE FUNCTION OVERWRITE fn::is_alive_at($table_suffix: string, $record_id: record, $check_time: number) -> bool {
    RETURN fn::check_liveness_range_exists($table_suffix, $record_id, $check_time, $check_time);
};

-- fn::get_liveness_records: Get liveness records for a resource in time range
DEFINE FUNCTION OVERWRITE fn::get_liveness_records($table_suffix: string, $record_id: record, $start_time: number, $end_time: number) -> array {
    LET $table_name = type::table($table_suffix + "_liveness_record");
    LET $field_name = $table_suffix + "_id";
    RETURN SELECT * FROM $table_name WHERE type::field($field_name) = $record_id AND $start_time <= period_end AND $end_time >= period_start;
};
