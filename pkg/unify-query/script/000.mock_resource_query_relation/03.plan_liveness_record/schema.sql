-- ============================================================================
-- SurrealDB Schema - Plan 03: Liveness 边表方案
-- 
-- 设计模型:
--   实体表: pod, node, container, deployment, replicaset, service, system
--   Liveness 通用表: liveness (存储所有续期记录)
--   Liveness 边表: pod_liveness, node_liveness, ... (RELATION FROM entity TO liveness)
--
-- 数据关系:
--   pod -> pod_liveness (边) -> liveness
--   node -> node_liveness (边) -> liveness
--   ...
--
-- 查询优势:
--   可使用 ->pod_liveness[WHERE ...].* 语法直接获取 liveness
--   无需子查询，更简洁高效
--
-- Write Pattern:
--   UPSERT pod:⟨pod-0:default:BCS-K8S-001⟩ MERGE {
--       bcs_cluster_id: "BCS-K8S-001",
--       namespace: "default", 
--       pod: "pod-0",
--       updated_at: <timestamp_ms>
--   };
--
-- Author: Auto-generated for BK Monitor - Plan 03 V2
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

-- Drop liveness table and edge tables
REMOVE TABLE IF EXISTS liveness;
REMOVE TABLE IF EXISTS pod_liveness;
REMOVE TABLE IF EXISTS node_liveness;
REMOVE TABLE IF EXISTS container_liveness;
REMOVE TABLE IF EXISTS deployment_liveness;
REMOVE TABLE IF EXISTS replicaset_liveness;
REMOVE TABLE IF EXISTS service_liveness;
REMOVE TABLE IF EXISTS system_liveness;

-- Drop relation tables
REMOVE TABLE IF EXISTS node_with_pod;
REMOVE TABLE IF EXISTS node_with_system;
REMOVE TABLE IF EXISTS container_with_pod;
REMOVE TABLE IF EXISTS pod_with_service;
REMOVE TABLE IF EXISTS deployment_with_replicaset;
REMOVE TABLE IF EXISTS pod_with_replicaset;
REMOVE TABLE IF EXISTS pod_to_pod;
REMOVE TABLE IF EXISTS pod_to_system;

-- Drop relation liveness edge tables
REMOVE TABLE IF EXISTS node_with_pod_liveness;
REMOVE TABLE IF EXISTS node_with_system_liveness;
REMOVE TABLE IF EXISTS container_with_pod_liveness;
REMOVE TABLE IF EXISTS pod_with_service_liveness;
REMOVE TABLE IF EXISTS deployment_with_replicaset_liveness;
REMOVE TABLE IF EXISTS pod_with_replicaset_liveness;
REMOVE TABLE IF EXISTS pod_to_pod_liveness;
REMOVE TABLE IF EXISTS pod_to_system_liveness;

-- Drop helper functions
REMOVE FUNCTION IF EXISTS fn::kv_block;
REMOVE FUNCTION IF EXISTS fn::relation_id;
REMOVE FUNCTION IF EXISTS fn::upsert_relation;

-- ============================================================================
-- SECTION 2: Liveness 通用表
-- ============================================================================

DEFINE TABLE liveness SCHEMAFULL;
DEFINE FIELD period_start ON liveness TYPE int;
DEFINE FIELD period_end ON liveness TYPE int;
DEFINE FIELD is_active ON liveness TYPE bool DEFAULT true;
DEFINE FIELD created_at ON liveness TYPE int;
DEFINE FIELD updated_at ON liveness TYPE int;

-- Index for time range queries
DEFINE INDEX idx_liveness_period ON liveness FIELDS period_start, period_end;
DEFINE INDEX idx_liveness_active ON liveness FIELDS is_active;

-- ============================================================================
-- SECTION 3: Pod 表和 pod_liveness 边表
-- ============================================================================

DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD created_at ON pod TYPE option<int>;
DEFINE FIELD updated_at ON pod TYPE int;

-- pod_liveness 边表: pod -> liveness
DEFINE TABLE pod_liveness TYPE RELATION FROM pod TO liveness SCHEMAFULL;

-- Pod 创建事件：创建 liveness 记录和边
DEFINE EVENT OVERWRITE event_pod_created ON TABLE pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_liveness->$liv[0].id;
};

-- Pod 更新事件（过期）：关闭旧记录，创建新记录
DEFINE EVENT OVERWRITE event_pod_updated_expired ON TABLE pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    -- 关闭旧的 liveness 记录
    LET $old_edges = SELECT out FROM pod_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    -- 创建新的 liveness 记录和边
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_liveness->$liv[0].id;
};

-- Pod 更新事件（续期）：扩展 period_end
DEFINE EVENT OVERWRITE event_pod_updated_active ON TABLE pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM pod_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 4: Node 表和 node_liveness 边表
-- ============================================================================

DEFINE TABLE node SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON node TYPE string;
DEFINE FIELD node ON node TYPE string;
DEFINE FIELD created_at ON node TYPE option<int>;
DEFINE FIELD updated_at ON node TYPE int;

DEFINE TABLE node_liveness TYPE RELATION FROM node TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_node_created ON TABLE node 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->node_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_node_updated_expired ON TABLE node 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM node_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->node_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_node_updated_active ON TABLE node 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM node_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 5: Container 表和 container_liveness 边表
-- ============================================================================

DEFINE TABLE container SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON container TYPE string;
DEFINE FIELD namespace ON container TYPE string;
DEFINE FIELD pod ON container TYPE string;
DEFINE FIELD container ON container TYPE string;
DEFINE FIELD created_at ON container TYPE option<int>;
DEFINE FIELD updated_at ON container TYPE int;

DEFINE TABLE container_liveness TYPE RELATION FROM container TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_container_created ON TABLE container 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->container_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_container_updated_expired ON TABLE container 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM container_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->container_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_container_updated_active ON TABLE container 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM container_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 6: Deployment 表和 deployment_liveness 边表
-- ============================================================================

DEFINE TABLE deployment SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON deployment TYPE string;
DEFINE FIELD namespace ON deployment TYPE string;
DEFINE FIELD deployment ON deployment TYPE string;
DEFINE FIELD created_at ON deployment TYPE option<int>;
DEFINE FIELD updated_at ON deployment TYPE int;

DEFINE TABLE deployment_liveness TYPE RELATION FROM deployment TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_deployment_created ON TABLE deployment 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->deployment_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_deployment_updated_expired ON TABLE deployment 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM deployment_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->deployment_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_deployment_updated_active ON TABLE deployment 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM deployment_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 7: ReplicaSet 表和 replicaset_liveness 边表
-- ============================================================================

DEFINE TABLE replicaset SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON replicaset TYPE string;
DEFINE FIELD namespace ON replicaset TYPE string;
DEFINE FIELD replicaset ON replicaset TYPE string;
DEFINE FIELD created_at ON replicaset TYPE option<int>;
DEFINE FIELD updated_at ON replicaset TYPE int;

DEFINE TABLE replicaset_liveness TYPE RELATION FROM replicaset TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_replicaset_created ON TABLE replicaset 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->replicaset_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_replicaset_updated_expired ON TABLE replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM replicaset_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->replicaset_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_replicaset_updated_active ON TABLE replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM replicaset_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 8: Service 表和 service_liveness 边表
-- ============================================================================

DEFINE TABLE service SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON service TYPE string;
DEFINE FIELD namespace ON service TYPE string;
DEFINE FIELD service ON service TYPE string;
DEFINE FIELD created_at ON service TYPE option<int>;
DEFINE FIELD updated_at ON service TYPE int;

DEFINE TABLE service_liveness TYPE RELATION FROM service TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_service_created ON TABLE service 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->service_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_service_updated_expired ON TABLE service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM service_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->service_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_service_updated_active ON TABLE service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM service_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 9: System 表和 system_liveness 边表
-- ============================================================================

DEFINE TABLE system SCHEMAFULL;
DEFINE FIELD bk_target_ip ON system TYPE string;
DEFINE FIELD bk_cloud_id ON system TYPE string;
DEFINE FIELD created_at ON system TYPE option<int>;
DEFINE FIELD updated_at ON system TYPE int;

DEFINE TABLE system_liveness TYPE RELATION FROM system TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_system_created ON TABLE system 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->system_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_system_updated_expired ON TABLE system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM system_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->system_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_system_updated_active ON TABLE system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM system_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 10: 资源关系边表
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

-- ============================================================================
-- SECTION 11: node_with_pod 关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION FROM node TO pod;
DEFINE FIELD created_at ON node_with_pod TYPE option<int>;
DEFINE FIELD updated_at ON node_with_pod TYPE int;

-- node_with_pod_liveness: 关系的 liveness 边表
DEFINE TABLE node_with_pod_liveness TYPE RELATION FROM node_with_pod TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_node_with_pod_created ON TABLE node_with_pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->node_with_pod_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_node_with_pod_updated_expired ON TABLE node_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM node_with_pod_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->node_with_pod_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_node_with_pod_updated_active ON TABLE node_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM node_with_pod_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 12: container_with_pod 关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE container_with_pod SCHEMAFULL TYPE RELATION FROM container TO pod;
DEFINE FIELD created_at ON container_with_pod TYPE option<int>;
DEFINE FIELD updated_at ON container_with_pod TYPE int;

DEFINE TABLE container_with_pod_liveness TYPE RELATION FROM container_with_pod TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_container_with_pod_created ON TABLE container_with_pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->container_with_pod_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_container_with_pod_updated_expired ON TABLE container_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM container_with_pod_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->container_with_pod_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_container_with_pod_updated_active ON TABLE container_with_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM container_with_pod_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 13: pod_with_service 关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION FROM pod TO service;
DEFINE FIELD created_at ON pod_with_service TYPE option<int>;
DEFINE FIELD updated_at ON pod_with_service TYPE int;

DEFINE TABLE pod_with_service_liveness TYPE RELATION FROM pod_with_service TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_pod_with_service_created ON TABLE pod_with_service 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_with_service_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_with_service_updated_expired ON TABLE pod_with_service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM pod_with_service_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_with_service_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_with_service_updated_active ON TABLE pod_with_service 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM pod_with_service_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 14: deployment_with_replicaset 关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE deployment_with_replicaset SCHEMAFULL TYPE RELATION FROM deployment TO replicaset;
DEFINE FIELD created_at ON deployment_with_replicaset TYPE option<int>;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE int;

DEFINE TABLE deployment_with_replicaset_liveness TYPE RELATION FROM deployment_with_replicaset TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_deployment_with_replicaset_created ON TABLE deployment_with_replicaset 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->deployment_with_replicaset_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_deployment_with_replicaset_updated_expired ON TABLE deployment_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM deployment_with_replicaset_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->deployment_with_replicaset_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_deployment_with_replicaset_updated_active ON TABLE deployment_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM deployment_with_replicaset_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 15: pod_with_replicaset 关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE pod_with_replicaset SCHEMAFULL TYPE RELATION FROM pod TO replicaset;
DEFINE FIELD created_at ON pod_with_replicaset TYPE option<int>;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE int;

DEFINE TABLE pod_with_replicaset_liveness TYPE RELATION FROM pod_with_replicaset TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_pod_with_replicaset_created ON TABLE pod_with_replicaset 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_with_replicaset_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_with_replicaset_updated_expired ON TABLE pod_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM pod_with_replicaset_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_with_replicaset_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_with_replicaset_updated_active ON TABLE pod_with_replicaset 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM pod_with_replicaset_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 16: node_with_system 关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE node_with_system SCHEMAFULL TYPE RELATION FROM node TO system;
DEFINE FIELD created_at ON node_with_system TYPE option<int>;
DEFINE FIELD updated_at ON node_with_system TYPE int;

DEFINE TABLE node_with_system_liveness TYPE RELATION FROM node_with_system TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_node_with_system_created ON TABLE node_with_system 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->node_with_system_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_node_with_system_updated_expired ON TABLE node_with_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM node_with_system_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->node_with_system_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_node_with_system_updated_active ON TABLE node_with_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM node_with_system_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 17: pod_to_pod 动态关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE pod_to_pod SCHEMAFULL TYPE RELATION FROM pod TO pod;
DEFINE FIELD created_at ON pod_to_pod TYPE option<int>;
DEFINE FIELD updated_at ON pod_to_pod TYPE int;

DEFINE TABLE pod_to_pod_liveness TYPE RELATION FROM pod_to_pod TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_pod_to_pod_created ON TABLE pod_to_pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_to_pod_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_to_pod_updated_expired ON TABLE pod_to_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM pod_to_pod_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_to_pod_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_to_pod_updated_active ON TABLE pod_to_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM pod_to_pod_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};

-- ============================================================================
-- SECTION 18: pod_to_system 动态关系及其 liveness 边表
-- ============================================================================

DEFINE TABLE pod_to_system SCHEMAFULL TYPE RELATION FROM pod TO system;
DEFINE FIELD created_at ON pod_to_system TYPE option<int>;
DEFINE FIELD updated_at ON pod_to_system TYPE int;

DEFINE TABLE pod_to_system_liveness TYPE RELATION FROM pod_to_system TO liveness SCHEMAFULL;

DEFINE EVENT OVERWRITE event_pod_to_system_created ON TABLE pod_to_system 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_to_system_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_to_system_updated_expired ON TABLE pod_to_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $old_edges = SELECT out FROM pod_to_system_liveness WHERE in = $after.id;
    FOR $edge IN $old_edges {
        UPDATE liveness SET is_active = false WHERE id = $edge.out AND is_active = true;
    };
    LET $liv = CREATE liveness SET 
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
    RELATE $after.id->pod_to_system_liveness->$liv[0].id;
};

DEFINE EVENT OVERWRITE event_pod_to_system_updated_active ON TABLE pod_to_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    LET $active_edges = SELECT out FROM pod_to_system_liveness WHERE in = $after.id;
    FOR $edge IN $active_edges {
        UPDATE liveness SET 
            updated_at = $after.updated_at, 
            period_end = $after.updated_at 
        WHERE id = $edge.out AND is_active = true;
    };
};
