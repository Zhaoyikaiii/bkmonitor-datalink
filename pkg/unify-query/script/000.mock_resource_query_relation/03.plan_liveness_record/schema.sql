-- 定义资源表（示例：Pod表）
DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD end_time ON pod TYPE int;                      -- Client必须传入，每次心跳更新

-- Event自动管理的字段
DEFINE FIELD start_time ON pod TYPE option<int>;            -- 首个窗口开始时间（首次插入后不变）
DEFINE FIELD windows_count ON pod TYPE option<int>;         -- 窗口数量
DEFINE FIELD active_windows ON pod TYPE option<array<object>>; -- 活跃窗口数组

DEFINE FIELD active_windows[*].start_time ON pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON pod TYPE int;

-- Event: 当end_time变化时自动触发，管理生命周期
DEFINE EVENT lifecycle ON TABLE pod WHEN $after.end_time != $before.end_time OR $before == NONE THEN {
    -- 首次插入处理
    IF $before == NONE THEN
        -- 设置首个窗口
        UPDATE $this SET 
            start_time = $after.end_time,
            windows_count = 1,
            active_windows = [{ start_time: $after.end_time, end_time: $after.end_time }];
    ELSE
        -- 更新处理：检查是否超过容忍时间
        LET $last_window = array::last($before.active_windows);
        LET $tolerance = 60000;  -- 60秒容忍时间
        
        -- 如果在容忍时间内更新
        IF $after.end_time <= $last_window.end_time + $tolerance THEN
            -- 更新最后一个窗口的end_time
            UPDATE $this SET 
                end_time = $after.end_time,
                active_windows = array::set(
                    $before.active_windows,
                    array::len($before.active_windows) - 1,
                    { start_time: $last_window.start_time, end_time: $after.end_time }
                );
        ELSE
            -- 超过容忍时间，新建窗口
            UPDATE $this SET 
                end_time = $after.end_time,
                windows_count = $before.windows_count + 1,
                active_windows = array::append(
                    $before.active_windows,
                    { start_time: $after.end_time, end_time: $after.end_time }
                );
        END;
    END;
};

-- 关系表（示例：Node与Pod的关系）
DEFINE TABLE node_with_pod SCHEMAFULL TYPE RELATION FROM node TO pod;
DEFINE FIELD end_time ON node_with_pod TYPE int;            -- Client必须传入

-- Event自动管理的字段
DEFINE FIELD start_time ON node_with_pod TYPE option<int>;
DEFINE FIELD windows_count ON node_with_pod TYPE option<int>;
DEFINE FIELD active_windows ON node_with_pod TYPE option<array<object>>;

DEFINE FIELD active_windows[*].start_time ON node_with_pod TYPE int;
DEFINE FIELD active_windows[*].end_time ON node_with_pod TYPE int;

-- Event: 当end_time变化时自动触发，管理关系生命周期
DEFINE EVENT relation_lifecycle ON TABLE node_with_pod WHEN $after.end_time != $before.end_time OR $before == NONE THEN {
    -- 首次插入处理
    IF $before == NONE THEN
        UPDATE $this SET 
            start_time = $after.end_time,
            windows_count = 1,
            active_windows = [{ start_time: $after.end_time, end_time: $after.end_time }];
    ELSE
        -- 更新处理：检查是否超过容忍时间
        LET $last_window = array::last($before.active_windows);
        LET $tolerance = 60000;  -- 60秒容忍时间
        
        -- 如果在容忍时间内更新
        IF $after.end_time <= $last_window.end_time + $tolerance THEN
            -- 更新最后一个窗口的end_time
            UPDATE $this SET 
                end_time = $after.end_time,
                active_windows = array::set(
                    $before.active_windows,
                    array::len($before.active_windows) - 1,
                    { start_time: $last_window.start_time, end_time: $after.end_time }
                );
        ELSE
            -- 超过容忍时间，新建窗口
            UPDATE $this SET 
                end_time = $after.end_time,
                windows_count = $before.windows_count + 1,
                active_windows = array::append(
                    $before.active_windows,
                    { start_time: $after.end_time, end_time: $after.end_time }
                );
        END;
    END;
};

-- 定义查询函数：获取资源在时间范围内的liveness记录
DEFINE FUNCTION OVERWRITE fn::get_liveness_records(
    $resource_type: string,      -- 资源类型，如 "pod", "node"
    $resource_id: record,        -- 资源ID
    $start_time: number,         -- 查询开始时间（毫秒时间戳）
    $end_time: number           -- 查询结束时间（毫秒时间戳）
) -> array {
    -- 构建动态查询
    LET $query = "SELECT * FROM type::table($resource_type) 
        WHERE type::thing($resource_type, $resource_id) = $this
        AND $start_time <= period_end 
        AND $end_time >= period_start";
    
    -- 执行动态查询
    RETURN EXECUTE $query;
};

-- 定义查询函数：获取关系在时间范围内的liveness记录
DEFINE FUNCTION OVERWRITE fn::get_relation_liveness_records(
    $relation_type: string,      -- 关系类型，如 "node_with_pod"
    $from_resource_id: record,    -- 源资源ID
    $to_resource_id: record,     -- 目标资源ID
    $start_time: number,         -- 查询开始时间（毫秒时间戳）
    $end_time: number           -- 查询结束时间（毫秒时间戳）
) -> array {
    -- 构建动态查询
    LET $query = "SELECT * FROM type::table($relation_type) 
        WHERE in = type::thing($relation_type, $from_resource_id)
        AND out = type::thing($relation_type, $to_resource_id)
        AND $start_time <= period_end 
        AND $end_time >= period_start";
    
    -- 执行动态查询
    RETURN EXECUTE $query;
};

-- 定义辅助函数：检查资源在指定时间是否活跃
DEFINE FUNCTION fn::is_active_at($windows: array<object>, $time: int) -> bool {
    RETURN array::any($windows, |$w| 
        $w.start_time <= $time 
        AND $w.end_time >= $time
    );
};

-- 定义辅助函数：获取资源的总活跃时间
DEFINE FUNCTION fn::get_total_active_time($windows: array<object>) -> number {
    RETURN math::sum(
        array::map($windows, |$w| $w.end_time - $w.start_time)
    );
};

-- 定义辅助函数：生成关系ID
DEFINE FUNCTION fn::relation_id($from_id: record, $to_id: record) -> string {
    RETURN $from_id.id + "|" + $to_id.id;
};

-- 创建测试数据：插入Pod资源
UPSERT pod:⟨bcs_cluster_id="BCS-K8S-00001", namespace="default", pod="nginx-1"⟩ MERGE {
    bcs_cluster_id: "BCS-K8S-00001",
    namespace: "default",
    pod: "nginx-1",
    end_time: time::millis()  -- REQUIRED: client必须传入end_time
};

UPSERT pod:⟨bcs_cluster_id="BCS-K8S-00001", namespace="default", pod="nginx-2"⟩ MERGE {
    bcs_cluster_id: "BCS-K8S-00001",
    namespace: "default",
    pod: "nginx-2",
    end_time: time::millis()
};

UPSERT pod:⟨bcs_cluster_id="BCS-K8S-00001", namespace="default", pod="nginx-3"⟩ MERGE {
    bcs_cluster_id: "BCS-K8S-00001",
    namespace: "default",
    pod: "nginx-3",
    end_time: time::millis()
};

-- 创建测试数据：插入Node资源
UPSERT node:⟨bcs_cluster_id="BCS-K8S-00001", node="node-1"⟩ MERGE {
    bcs_cluster_id: "BCS-K8S-00001",
    node: "node-1",
    end_time: time::millis()
};

UPSERT node:⟨bcs_cluster_id="BCS-K8S-00001", node="node-2"⟩ MERGE {
    bcs_cluster_id: "BCS-K8S-00001",
    node: "node-2",
    end_time: time::millis()
};

-- 创建测试数据：创建Node与Pod的关系
LET $now = time::millis();
fn::upsert_relation("node_with_pod", 
    node:⟨bcs_cluster_id="BCS-K8S-00001",node="node-1"⟩, 
    pod:⟨bcs_cluster_id="BCS-K8S-00001",namespace="default",pod="nginx-1"⟩, 
    $now
);

fn::upsert_relation("node_with_pod", 
    node:⟨bcs_cluster_id="BCS-K8S-00001",node="node-1"⟩, 
    pod:⟨bcs_cluster_id="BCS-K8S-00001",namespace="default",pod="nginx-2"⟩, 
    $now
);

fn::upsert_relation("node_with_pod", 
    node:⟨bcs_cluster_id="BCS-K8S-00001",node="node-2"⟩, 
    pod:⟨bcs_cluster_id="BCS-K8S-00001",namespace="default",pod="nginx-3"⟩, 
    $now
);

-- 创建测试数据：模拟时间间隔，创建新的活跃窗口
LET $one_hour_ago = time::millis() - 3600000;  -- 1小时前
fn::upsert_relation("node_with_pod", 
    node:⟨bcs_cluster_id="BCS-K8S-00001",node="node-1"⟩, 
    pod:⟨bcs_cluster_id="BCS-K8S-00001",namespace="default",pod="nginx-1"⟩, 
    $one_hour_ago
);

-- 查询示例：获取所有Pod资源
-- SELECT * FROM pod LIMIT 3;

-- 查询示例：获取当前活跃的Pod（最后心跳在1分钟内）
-- SELECT * FROM pod WHERE end_time >= time::millis() - 60000 LIMIT 3;

-- 查询示例：获取有多个活跃窗口的Pod（曾上报中断过）
-- SELECT * FROM pod WHERE windows_count > 1 LIMIT 3;

-- 查询示例：使用get_liveness_records函数查询Pod在时间范围内的记录
-- LET $pod_id = "bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-1";
-- LET $start = time::millis() - 3600000;  -- 1小时前
-- LET $end = time::millis();            -- 现在
-- SELECT * FROM fn::get_liveness_records("pod", $pod_id, $start, $end) LIMIT 3;