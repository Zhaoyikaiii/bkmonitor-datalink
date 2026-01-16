-- ============================================================================
-- SurrealDB Schema - Plan 03: Liveness 记录表方案（普通 SCHEMAFULL + source_id/target_id）
-- 
-- 设计模型:
--   实体表: pod, node, container, deployment, replicaset, service, system
--   Liveness 记录表: pod_liveness_record, node_liveness_record, ... (通过外键 xxx_id 关联)
--   关系表: 普通 SCHEMAFULL 表，使用 source_id/target_id 字段关联实体
--
-- 数据关系:
--   pod -> pod_liveness_record (通过 pod_id 字段关联)
--   node -> node_liveness_record (通过 node_id 字段关联)
--   node_with_pod -> node_with_pod_liveness_record (通过 relation_id 字段关联)
--   ...
--
-- 查询方式:
--   使用子查询: (SELECT * FROM relation WHERE target_id = $parent.id AND ...)
--
-- Write Pattern (实体):
--   UPSERT pod:⟨pod-0:default:BCS-K8S-001⟩ MERGE {
--       bcs_cluster_id: "BCS-K8S-001",
--       namespace: "default", 
--       pod: "pod-0",
--       updated_at: <timestamp_ms>
--   };
--
-- Write Pattern (关系):
--   UPSERT node_with_pod:⟨...⟩ MERGE {
--       source_id: node:⟨...⟩,
--       target_id: pod:⟨...⟩,
--       updated_at: <timestamp_ms>
--   };
--
-- Author: Auto-generated for BK Monitor - Plan 03 V4 (普通表 + source_id/target_id)
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
REMOVE TABLE IF EXISTS pod_to_pod;
REMOVE TABLE IF EXISTS pod_to_system;

-- Drop relation liveness record tables
REMOVE TABLE IF EXISTS node_with_pod_liveness_record;
REMOVE TABLE IF EXISTS node_with_system_liveness_record;
REMOVE TABLE IF EXISTS container_with_pod_liveness_record;
REMOVE TABLE IF EXISTS pod_with_service_liveness_record;
REMOVE TABLE IF EXISTS deployment_with_replicaset_liveness_record;
REMOVE TABLE IF EXISTS pod_with_replicaset_liveness_record;
REMOVE TABLE IF EXISTS pod_to_pod_liveness_record;
REMOVE TABLE IF EXISTS pod_to_system_liveness_record;

-- Drop helper functions
REMOVE FUNCTION IF EXISTS fn::kv_block;
REMOVE FUNCTION IF EXISTS fn::relation_id;
REMOVE FUNCTION IF EXISTS fn::upsert_relation;

-- ============================================================================
-- SECTION 2: Pod 表和 pod_liveness_record 记录表
-- ============================================================================

DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD created_at ON pod TYPE option<int>;
DEFINE FIELD updated_at ON pod TYPE int;

-- pod_liveness_record: 通过 pod_id 外键关联
DEFINE TABLE pod_liveness_record SCHEMAFULL;
DEFINE FIELD pod_id ON pod_liveness_record TYPE record<pod>;
DEFINE FIELD period_start ON pod_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_liveness_record TYPE int;

DEFINE INDEX idx_pod_liveness_pod_id ON pod_liveness_record FIELDS pod_id;
DEFINE INDEX idx_pod_liveness_period ON pod_liveness_record FIELDS period_start, period_end;
DEFINE INDEX idx_pod_liveness_active ON pod_liveness_record FIELDS is_active;

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

DEFINE EVENT OVERWRITE event_pod_updated_expired ON TABLE pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    UPDATE pod_liveness_record SET is_active = false 
    WHERE pod_id = $after.id AND is_active = true;
    CREATE pod_liveness_record SET 
        pod_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_updated_active ON TABLE pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    UPDATE pod_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE pod_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 3: Node 表和 node_liveness_record 记录表
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
DEFINE INDEX idx_node_liveness_period ON node_liveness_record FIELDS period_start, period_end;
DEFINE INDEX idx_node_liveness_active ON node_liveness_record FIELDS is_active;

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
    UPDATE node_liveness_record SET is_active = false 
    WHERE node_id = $after.id AND is_active = true;
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
    UPDATE node_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE node_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 4: Container 表和 container_liveness_record 记录表
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
DEFINE INDEX idx_container_liveness_period ON container_liveness_record FIELDS period_start, period_end;
DEFINE INDEX idx_container_liveness_active ON container_liveness_record FIELDS is_active;

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
    UPDATE container_liveness_record SET is_active = false 
    WHERE container_id = $after.id AND is_active = true;
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
    UPDATE container_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE container_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 5: Deployment 表和 deployment_liveness_record 记录表
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
DEFINE INDEX idx_deployment_liveness_period ON deployment_liveness_record FIELDS period_start, period_end;
DEFINE INDEX idx_deployment_liveness_active ON deployment_liveness_record FIELDS is_active;

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
    UPDATE deployment_liveness_record SET is_active = false 
    WHERE deployment_id = $after.id AND is_active = true;
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
    UPDATE deployment_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE deployment_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 6: ReplicaSet 表和 replicaset_liveness_record 记录表
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
DEFINE INDEX idx_replicaset_liveness_period ON replicaset_liveness_record FIELDS period_start, period_end;
DEFINE INDEX idx_replicaset_liveness_active ON replicaset_liveness_record FIELDS is_active;

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
    UPDATE replicaset_liveness_record SET is_active = false 
    WHERE replicaset_id = $after.id AND is_active = true;
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
    UPDATE replicaset_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE replicaset_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 7: Service 表和 service_liveness_record 记录表
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
DEFINE INDEX idx_service_liveness_period ON service_liveness_record FIELDS period_start, period_end;
DEFINE INDEX idx_service_liveness_active ON service_liveness_record FIELDS is_active;

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
    UPDATE service_liveness_record SET is_active = false 
    WHERE service_id = $after.id AND is_active = true;
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
    UPDATE service_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE service_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 8: System 表和 system_liveness_record 记录表
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
DEFINE INDEX idx_system_liveness_period ON system_liveness_record FIELDS period_start, period_end;
DEFINE INDEX idx_system_liveness_active ON system_liveness_record FIELDS is_active;

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
    UPDATE system_liveness_record SET is_active = false 
    WHERE system_id = $after.id AND is_active = true;
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
    UPDATE system_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE system_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 9: 辅助函数
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

-- ============================================================================
-- SECTION 10: node_with_pod 关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE node_with_pod SCHEMAFULL;
DEFINE FIELD source_id ON node_with_pod TYPE record<node>;
DEFINE FIELD target_id ON node_with_pod TYPE record<pod>;
DEFINE FIELD created_at ON node_with_pod TYPE option<int>;
DEFINE FIELD updated_at ON node_with_pod TYPE int;

DEFINE INDEX idx_node_with_pod_source ON node_with_pod FIELDS source_id;
DEFINE INDEX idx_node_with_pod_target ON node_with_pod FIELDS target_id;

DEFINE TABLE node_with_pod_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON node_with_pod_liveness_record TYPE record<node_with_pod>;
DEFINE FIELD period_start ON node_with_pod_liveness_record TYPE int;
DEFINE FIELD period_end ON node_with_pod_liveness_record TYPE int;
DEFINE FIELD is_active ON node_with_pod_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON node_with_pod_liveness_record TYPE int;
DEFINE FIELD updated_at ON node_with_pod_liveness_record TYPE int;

DEFINE INDEX idx_node_with_pod_liveness_relation_id ON node_with_pod_liveness_record FIELDS relation_id;
DEFINE INDEX idx_node_with_pod_liveness_period ON node_with_pod_liveness_record FIELDS period_start, period_end;

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
    UPDATE node_with_pod_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
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
    UPDATE node_with_pod_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 11: container_with_pod 关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE container_with_pod SCHEMAFULL;
DEFINE FIELD source_id ON container_with_pod TYPE record<container>;
DEFINE FIELD target_id ON container_with_pod TYPE record<pod>;
DEFINE FIELD created_at ON container_with_pod TYPE option<int>;
DEFINE FIELD updated_at ON container_with_pod TYPE int;

DEFINE INDEX idx_container_with_pod_source ON container_with_pod FIELDS source_id;
DEFINE INDEX idx_container_with_pod_target ON container_with_pod FIELDS target_id;

DEFINE TABLE container_with_pod_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON container_with_pod_liveness_record TYPE record<container_with_pod>;
DEFINE FIELD period_start ON container_with_pod_liveness_record TYPE int;
DEFINE FIELD period_end ON container_with_pod_liveness_record TYPE int;
DEFINE FIELD is_active ON container_with_pod_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON container_with_pod_liveness_record TYPE int;
DEFINE FIELD updated_at ON container_with_pod_liveness_record TYPE int;

DEFINE INDEX idx_container_with_pod_liveness_relation_id ON container_with_pod_liveness_record FIELDS relation_id;
DEFINE INDEX idx_container_with_pod_liveness_period ON container_with_pod_liveness_record FIELDS period_start, period_end;

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
    UPDATE container_with_pod_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
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
    UPDATE container_with_pod_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 12: pod_with_service 关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE pod_with_service SCHEMAFULL;
DEFINE FIELD source_id ON pod_with_service TYPE record<pod>;
DEFINE FIELD target_id ON pod_with_service TYPE record<service>;
DEFINE FIELD created_at ON pod_with_service TYPE option<int>;
DEFINE FIELD updated_at ON pod_with_service TYPE int;

DEFINE INDEX idx_pod_with_service_source ON pod_with_service FIELDS source_id;
DEFINE INDEX idx_pod_with_service_target ON pod_with_service FIELDS target_id;

DEFINE TABLE pod_with_service_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON pod_with_service_liveness_record TYPE record<pod_with_service>;
DEFINE FIELD period_start ON pod_with_service_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_with_service_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_with_service_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_with_service_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_with_service_liveness_record TYPE int;

DEFINE INDEX idx_pod_with_service_liveness_relation_id ON pod_with_service_liveness_record FIELDS relation_id;
DEFINE INDEX idx_pod_with_service_liveness_period ON pod_with_service_liveness_record FIELDS period_start, period_end;

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
    UPDATE pod_with_service_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
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
    UPDATE pod_with_service_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 13: deployment_with_replicaset 关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE deployment_with_replicaset SCHEMAFULL;
DEFINE FIELD source_id ON deployment_with_replicaset TYPE record<deployment>;
DEFINE FIELD target_id ON deployment_with_replicaset TYPE record<replicaset>;
DEFINE FIELD created_at ON deployment_with_replicaset TYPE option<int>;
DEFINE FIELD updated_at ON deployment_with_replicaset TYPE int;

DEFINE INDEX idx_deployment_with_replicaset_source ON deployment_with_replicaset FIELDS source_id;
DEFINE INDEX idx_deployment_with_replicaset_target ON deployment_with_replicaset FIELDS target_id;

DEFINE TABLE deployment_with_replicaset_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON deployment_with_replicaset_liveness_record TYPE record<deployment_with_replicaset>;
DEFINE FIELD period_start ON deployment_with_replicaset_liveness_record TYPE int;
DEFINE FIELD period_end ON deployment_with_replicaset_liveness_record TYPE int;
DEFINE FIELD is_active ON deployment_with_replicaset_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON deployment_with_replicaset_liveness_record TYPE int;
DEFINE FIELD updated_at ON deployment_with_replicaset_liveness_record TYPE int;

DEFINE INDEX idx_deployment_with_replicaset_liveness_relation_id ON deployment_with_replicaset_liveness_record FIELDS relation_id;
DEFINE INDEX idx_deployment_with_replicaset_liveness_period ON deployment_with_replicaset_liveness_record FIELDS period_start, period_end;

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
    UPDATE deployment_with_replicaset_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
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
    UPDATE deployment_with_replicaset_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 14: pod_with_replicaset 关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE pod_with_replicaset SCHEMAFULL;
DEFINE FIELD source_id ON pod_with_replicaset TYPE record<pod>;
DEFINE FIELD target_id ON pod_with_replicaset TYPE record<replicaset>;
DEFINE FIELD created_at ON pod_with_replicaset TYPE option<int>;
DEFINE FIELD updated_at ON pod_with_replicaset TYPE int;

DEFINE INDEX idx_pod_with_replicaset_source ON pod_with_replicaset FIELDS source_id;
DEFINE INDEX idx_pod_with_replicaset_target ON pod_with_replicaset FIELDS target_id;

DEFINE TABLE pod_with_replicaset_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON pod_with_replicaset_liveness_record TYPE record<pod_with_replicaset>;
DEFINE FIELD period_start ON pod_with_replicaset_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_with_replicaset_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_with_replicaset_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_with_replicaset_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_with_replicaset_liveness_record TYPE int;

DEFINE INDEX idx_pod_with_replicaset_liveness_relation_id ON pod_with_replicaset_liveness_record FIELDS relation_id;
DEFINE INDEX idx_pod_with_replicaset_liveness_period ON pod_with_replicaset_liveness_record FIELDS period_start, period_end;

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
    UPDATE pod_with_replicaset_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
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
    UPDATE pod_with_replicaset_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 15: node_with_system 关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE node_with_system SCHEMAFULL;
DEFINE FIELD source_id ON node_with_system TYPE record<node>;
DEFINE FIELD target_id ON node_with_system TYPE record<system>;
DEFINE FIELD created_at ON node_with_system TYPE option<int>;
DEFINE FIELD updated_at ON node_with_system TYPE int;

DEFINE INDEX idx_node_with_system_source ON node_with_system FIELDS source_id;
DEFINE INDEX idx_node_with_system_target ON node_with_system FIELDS target_id;

DEFINE TABLE node_with_system_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON node_with_system_liveness_record TYPE record<node_with_system>;
DEFINE FIELD period_start ON node_with_system_liveness_record TYPE int;
DEFINE FIELD period_end ON node_with_system_liveness_record TYPE int;
DEFINE FIELD is_active ON node_with_system_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON node_with_system_liveness_record TYPE int;
DEFINE FIELD updated_at ON node_with_system_liveness_record TYPE int;

DEFINE INDEX idx_node_with_system_liveness_relation_id ON node_with_system_liveness_record FIELDS relation_id;
DEFINE INDEX idx_node_with_system_liveness_period ON node_with_system_liveness_record FIELDS period_start, period_end;

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
    UPDATE node_with_system_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
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
    UPDATE node_with_system_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 16: pod_to_pod 动态关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE pod_to_pod SCHEMAFULL;
DEFINE FIELD source_id ON pod_to_pod TYPE record<pod>;
DEFINE FIELD target_id ON pod_to_pod TYPE record<pod>;
DEFINE FIELD created_at ON pod_to_pod TYPE option<int>;
DEFINE FIELD updated_at ON pod_to_pod TYPE int;

DEFINE INDEX idx_pod_to_pod_source ON pod_to_pod FIELDS source_id;
DEFINE INDEX idx_pod_to_pod_target ON pod_to_pod FIELDS target_id;

DEFINE TABLE pod_to_pod_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON pod_to_pod_liveness_record TYPE record<pod_to_pod>;
DEFINE FIELD period_start ON pod_to_pod_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_to_pod_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_to_pod_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_to_pod_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_to_pod_liveness_record TYPE int;

DEFINE INDEX idx_pod_to_pod_liveness_relation_id ON pod_to_pod_liveness_record FIELDS relation_id;
DEFINE INDEX idx_pod_to_pod_liveness_period ON pod_to_pod_liveness_record FIELDS period_start, period_end;

DEFINE EVENT OVERWRITE event_pod_to_pod_created ON TABLE pod_to_pod 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE pod_to_pod_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_to_pod_updated_expired ON TABLE pod_to_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    UPDATE pod_to_pod_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
    CREATE pod_to_pod_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_to_pod_updated_active ON TABLE pod_to_pod 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    UPDATE pod_to_pod_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};

-- ============================================================================
-- SECTION 17: pod_to_system 动态关系表（普通 SCHEMAFULL + source_id/target_id）
-- ============================================================================

DEFINE TABLE pod_to_system SCHEMAFULL;
DEFINE FIELD source_id ON pod_to_system TYPE record<pod>;
DEFINE FIELD target_id ON pod_to_system TYPE record<system>;
DEFINE FIELD created_at ON pod_to_system TYPE option<int>;
DEFINE FIELD updated_at ON pod_to_system TYPE int;

DEFINE INDEX idx_pod_to_system_source ON pod_to_system FIELDS source_id;
DEFINE INDEX idx_pod_to_system_target ON pod_to_system FIELDS target_id;

DEFINE TABLE pod_to_system_liveness_record SCHEMAFULL;
DEFINE FIELD relation_id ON pod_to_system_liveness_record TYPE record<pod_to_system>;
DEFINE FIELD period_start ON pod_to_system_liveness_record TYPE int;
DEFINE FIELD period_end ON pod_to_system_liveness_record TYPE int;
DEFINE FIELD is_active ON pod_to_system_liveness_record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON pod_to_system_liveness_record TYPE int;
DEFINE FIELD updated_at ON pod_to_system_liveness_record TYPE int;

DEFINE INDEX idx_pod_to_system_liveness_relation_id ON pod_to_system_liveness_record FIELDS relation_id;
DEFINE INDEX idx_pod_to_system_liveness_period ON pod_to_system_liveness_record FIELDS period_start, period_end;

DEFINE EVENT OVERWRITE event_pod_to_system_created ON TABLE pod_to_system 
WHEN $event = "CREATE" 
THEN {
    UPDATE $after.id SET created_at = $after.updated_at;
    CREATE pod_to_system_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_to_system_updated_expired ON TABLE pod_to_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at > {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    UPDATE pod_to_system_liveness_record SET is_active = false 
    WHERE relation_id = $after.id AND is_active = true;
    CREATE pod_to_system_liveness_record SET 
        relation_id = $after.id,
        period_start = $after.updated_at,
        period_end = $after.updated_at,
        is_active = true,
        created_at = $after.updated_at,
        updated_at = $after.updated_at;
};

DEFINE EVENT OVERWRITE event_pod_to_system_updated_active ON TABLE pod_to_system 
WHEN $event = "UPDATE" 
    AND $after.updated_at - $before.updated_at <= {tolerance_time_ms}
    AND $after.id = $before.id 
THEN {
    UPDATE pod_to_system_liveness_record SET 
        updated_at = $after.updated_at, 
        period_end = $after.updated_at 
    WHERE relation_id = $after.id AND is_active = true;
};
