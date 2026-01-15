-- 多 Root 查询验证
-- 场景: source_type = pod, namespace = "default" (匹配多个 pod)
-- 预期: 返回数组，每个 pod 是独立的一棵树

LET $timestamp = 600000;
LET $look_back_delta = 600000;
LET $start = $timestamp - $look_back_delta;
LET $end = $timestamp;

SELECT {
    root: {
        entity_type: meta::tb(id),
        entity_id: <string>id,
        entity_data: { bcs_cluster_id: bcs_cluster_id, namespace: namespace, pod: pod },
        created_at: created_at,
        updated_at: updated_at,
        -- 使用边表关联查询获取 liveness
        liveness: ->pod_liveness->liveness[WHERE period_end >= $start AND period_start <= $end].*
    },
    
    hop1: {
        node_with_pod: (SELECT {
            hop: 1,
            relation_type: "node_with_pod",
            relation_category: "static",
            relation_id: <string>id,
            -- 使用边表关联查询获取关系 liveness
            relation_liveness: ->node_with_pod_liveness->liveness[WHERE period_end >= $start AND period_start <= $end].*,
            target: {
                entity_type: "node",
                entity_id: <string>in,
                entity_data: { bcs_cluster_id: in.bcs_cluster_id, node: in.node },
                -- target 的 liveness
                liveness: in->node_liveness->liveness[WHERE period_end >= $start AND period_start <= $end].*
            }
        } FROM <-node_with_pod WHERE updated_at >= $start)
    }
} AS result 
FROM pod 
WHERE namespace = "default"
  AND updated_at >= $start
  AND array::len(->pod_liveness->liveness[WHERE period_end >= $start AND period_start <= $end]) > 0
LIMIT 10;
