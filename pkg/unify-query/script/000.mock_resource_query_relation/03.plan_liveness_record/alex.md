# 方案一 驱动标记(节点) - 腾讯iWiki

方案一 驱动标记(节点)

## 测试目的

验证当 pod表记录更新时，如果更新时间间隔超过300秒，则创建新的续期记录，并且标记最近的续期记录为失活状态，方便后续分析或者清理。

## 数据模型设计

### 设计说明

- pod 表是业务方自定义的表

- pod_liveness_record 表是计算平台内部自动生成和维护的表，业务方不用定义和维护

### 建表语句

代码解释 代码改写
```
20 1-- ------------------------------2-- TABLE: pod 信息主表3-- ------------------------------4DEFINE TABLE pod SCHEMAFULL;5DEFINE FIELD bcs_cluster_id ON pod TYPE string; -- bcs 集群 ID6DEFINE FIELD namespace ON pod TYPE string; -- 命名空间7DEFINE FIELD name ON pod TYPE string; -- pod 名称8DEFINE FIELD created_at ON pod TYPE int DEFAULT updated_at; -- 创建时间9DEFINE FIELD updated_at ON pod TYPE int; -- 更新时间10​11-- ------------------------------12-- TABLE: pod 续期记录表13-- ------------------------------14DEFINE TABLE pod_liveness_record SCHEMAFULL;15DEFINE FIELD pod_id ON pod_liveness_record TYPE record<pod>; -- 关联 pod record id16DEFINE FIELD period_start ON pod_liveness_record TYPE int DEFAULT updated_at; -- 续期开始时间17DEFINE FIELD period_end ON pod_liveness_record TYPE int; -- 续期结束时间18DEFINE FIELD is_active ON pod_liveness_record TYPE bool DEFAULT true; -- 是否仍然活跃19DEFINE FIELD created_at ON pod_liveness_record TYPE int DEFAULT updated_at; -- 创建时间20DEFINE FIELD updated_at ON pod_liveness_record TYPE int; -- 更新时间
```

## 数据驱动设计

### 流程图

### 驱动定义语句

代码解释 代码改写
```
19 1-- ========================================2-- Pod 信息更新事件驱动3-- 功能：当 Pod 信息创建时，创建新的续期记录4-- ========================================5DEFINE EVENT OVERWRITE event_pod_created ON TABLE pod 6​7-- ========================================8-- Pod 信息更新事件驱动（过期）9-- 功能：超过容忍间隔则新建续期记录，关闭就的续期记录10-- ========================================11DEFINE EVENT OVERWRITE event_pod_updated_expired ON TABLE pod 12​13​14-- ========================================15-- Pod 信息更新事件驱动（续期）16-- 功能：不超过容忍间隔，更新续期记录17-- ========================================18DEFINE EVENT OVERWRITE event_pod_updated_active ON TABLE pod 19​
```

## 辅助函数设计

### 续期函数定义

代码解释 代码改写
```
2 1-- 检查在时间范围内是否续期记录2DEFINE FUNCTION OVERWRITE fn::check_liveness_range_exists
```

## 数据驱动场景案例

### 流程图

### T0 时刻模拟上报（初始化）

- 模拟上报请求

代码解释 代码改写
```
21 1-- T0 （2025-12-23 12:00:00） 时刻上报 UPSERT 事件2upsert pod merge {3 id:"pod-0:default:BCS-K8S-001",4 name: "pod-0",5 namespace: "default",6 bcs_cluster_id: "BCS-K8S-001",7 updated_at: 1766462400 -- 2025-12-23 12:00:008};9​10upsert pod merge {11 id: "pod-1:default:BCS-K8S-001",12 name: "pod-1",13 namespace: "default",14 bcs_cluster_id: "BCS-K8S-001",15 updated_at: 1766462400 -- 2025-12-23 12:00:0016};17​18-- 查看更新结果19select * from pod;20​21select * from pod_liveness_record;
```

- pod 返回数据

```
45 1-- pod 返回2-------- Query 1 (468us) --------3​4[5 {6 bcs_cluster_id: 'BCS-K8S-001',7 created_at: 1766462400,8 id: pod:⟨pod-0:default:BCS-K8S-001⟩,9 name: 'pod-0',10 namespace: 'default',11 updated_at: 176646240012 },13 {14 bcs_cluster_id: 'BCS-K8S-001',15 created_at: 1766462400,16 id: pod:⟨pod-1:default:BCS-K8S-001⟩,17 name: 'pod-1',18 namespace: 'default',19 updated_at: 176646240020 }21]22​23-- pod_liveness_record 返回24-------- Query 1 (328us) --------25​26[27 {28 created_at: 1766462400,29 id: pod_liveness_record:oagf14ijlyv3wo69umu4,30 is_active: true,31 period_end: 1766462400,32 period_start: 1766462400,33 pod_id: pod:⟨pod-1:default:BCS-K8S-001⟩,34 updated_at: 176646240035 },36 {37 created_at: 1766462400,38 id: pod_liveness_record:vhyufpyiksw05516bjsy,39 is_active: true,40 period_end: 1766462400,41 period_start: 1766462400,42 pod_id: pod:⟨pod-0:default:BCS-K8S-001⟩,43 updated_at: 176646240044 }45]
```

### T1 模拟时刻上报（续期）

代码解释 代码改写
```
13 1-- T1（2025-12-23 12:04:59）时刻上报 UPSERT 事件，只触发续期2upsert pod merge {3 id:"pod-0:default:BCS-K8S-001",4 name: "pod-0",5 namespace: "default",6 bcs_cluster_id: "BCS-K8S-001",7 updated_at: 1766462699 -- 2025-12-23 12:04:598};9​10-- 查看更新结果11select * from pod;12​13select * from pod_liveness_record;
```

预期结果：

- pod 表中 pod-0 的 updated_at 更新为 1766462699

- pod_liveness_record 表中 pod-0 最近的续期记录 pod_liveness_record:701ja1jf519kq31kviwu 更新 period_end 为 1766462699

```
45 1-- pod 结果2-------- Query 1 (407us) --------3​4[5 {6 bcs_cluster_id: 'BCS-K8S-001',7 created_at: 1766462400,8 id: pod:⟨pod-0:default:BCS-K8S-001⟩,9 name: 'pod-0',10 namespace: 'default',11 updated_at: 176646269912 },13 {14 bcs_cluster_id: 'BCS-K8S-001',15 created_at: 1766462400,16 id: pod:⟨pod-1:default:BCS-K8S-001⟩,17 name: 'pod-1',18 namespace: 'default',19 updated_at: 176646240020 }21]22​23-- pod_liveness_record 结果24-------- Query 2 (282us) --------25​26[27 {28 created_at: 1766462400,29 id: pod_liveness_record:701ja1jf519kq31kviwu,30 is_active: true,31 period_end: 1766462699,32 period_start: 1766462400,33 pod_id: pod:⟨pod-0:default:BCS-K8S-001⟩,34 updated_at: 176646269935 },36 {37 created_at: 1766462400,38 id: pod_liveness_record:h6ia5xyrc7d40y1cs1np,39 is_active: true,40 period_end: 1766462400,41 period_start: 1766462400,42 pod_id: pod:⟨pod-1:default:BCS-K8S-001⟩,43 updated_at: 176646240044 }45]
```

### T2 时刻模拟上报（过期）

代码解释 代码改写
```
14 1-- T2（2025-12-23 12:10:00）时刻上报 UPSERT 事件，距离上次更新 T1（2025-12-23 12:04:59）间隔为301秒，触发过期2​3upsert pod merge {4 id:"pod-0:default:BCS-K8S-001",5 name: "pod-0",6 namespace: "default",7 bcs_cluster_id: "BCS-K8S-001",8 updated_at: 1766463000 -- 2025-12-23 12:10:009};10​11-- 查看更新结果12SELECT * FROM pod;13​14select * from pod_liveness_record;
```

预期结果：

- pod 表中 pod-0 的 updated_at 更新为 1766463000

- pod_liveness_record 表中 pod-0 最近的续期记录 pod_liveness_record:701ja1jf519kq31kviwu 更新 is_active 为 false

- pod_liveness_record 新增一条 pod-0 的有效续期记录

```
54 1-- pod 表结果2-------- Query 1 (371us) --------3​4[5 {6 bcs_cluster_id: 'BCS-K8S-001',7 created_at: 1766462400,8 id: pod:⟨pod-0:default:BCS-K8S-001⟩,9 name: 'pod-0',10 namespace: 'default',11 updated_at: 176646300012 },13 {14 bcs_cluster_id: 'BCS-K8S-001',15 created_at: 1766462400,16 id: pod:⟨pod-1:default:BCS-K8S-001⟩,17 name: 'pod-1',18 namespace: 'default',19 updated_at: 176646240020 }21]22​23-- pod_liveness_record 结果24-------- Query 2 (196us) --------25​26[27 {28 created_at: 1766463000,29 id: pod_liveness_record:3tinxwrnmagn9169i6vm,30 is_active: true,31 period_end: 1766463000,32 period_start: 1766463000,33 pod_id: pod:⟨pod-0:default:BCS-K8S-001⟩,34 updated_at: 176646300035 },36 {37 created_at: 1766462400,38 id: pod_liveness_record:701ja1jf519kq31kviwu,39 is_active: false,40 period_end: 1766462699,41 period_start: 1766462400,42 pod_id: pod:⟨pod-0:default:BCS-K8S-001⟩,43 updated_at: 176646269944 },45 {46 created_at: 1766462400,47 id: pod_liveness_record:h6ia5xyrc7d40y1cs1np,48 is_active: true,49 period_end: 1766462400,50 period_start: 1766462400,51 pod_id: pod:⟨pod-1:default:BCS-K8S-001⟩,52 updated_at: 176646240053 }54]
```

### T3 时刻模拟上报（续期）

```
13 1-- T3 （2025-12-23 12:13:00） 时刻上报 UPSERT 事件，距离上次更新 T2（2025-12-23 12:10:00）间隔为 180s，触发续期2upsert pod merge {3 id:"pod-0:default:BCS-K8S-001",4 name: "pod-0",5 namespace: "default",6 bcs_cluster_id: "BCS-K8S-001",7 updated_at: 1766463180 -- 2025-12-23 12:13:008};9​10-- 查看更新结果11SELECT * FROM pod;12​13select * from pod_liveness_record;
```

预期结果

- pod 表中 pod-0 的 updated_at 更新为 1766463180

- pod_liveness_record 中 pod-0 最近的续期记录 pod_liveness_record:3tinxwrnmagn9169i6vm 更新 period_end 为 1766463180

```
52 1-------- Query 1 (369us) --------2​3[4 {5 bcs_cluster_id: 'BCS-K8S-001',6 created_at: 1766462400,7 id: pod:⟨pod-0:default:BCS-K8S-001⟩,8 name: 'pod-0',9 namespace: 'default',10 updated_at: 176646318011 },12 {13 bcs_cluster_id: 'BCS-K8S-001',14 created_at: 1766462400,15 id: pod:⟨pod-1:default:BCS-K8S-001⟩,16 name: 'pod-1',17 namespace: 'default',18 updated_at: 176646240019 }20]21​22-------- Query 2 (240us) --------23​24[25 {26 created_at: 1766463000,27 id: pod_liveness_record:3tinxwrnmagn9169i6vm,28 is_active: true,29 period_end: 1766463180,30 period_start: 1766463000,31 pod_id: pod:⟨pod-0:default:BCS-K8S-001⟩,32 updated_at: 176646318033 },34 {35 created_at: 1766462400,36 id: pod_liveness_record:701ja1jf519kq31kviwu,37 is_active: false,38 period_end: 1766462699,39 period_start: 1766462400,40 pod_id: pod:⟨pod-0:default:BCS-K8S-001⟩,41 updated_at: 176646269942 },43 {44 created_at: 1766462400,45 id: pod_liveness_record:h6ia5xyrc7d40y1cs1np,46 is_active: true,47 period_end: 1766462400,48 period_start: 1766462400,49 pod_id: pod:⟨pod-1:default:BCS-K8S-001⟩,50 updated_at: 176646240051 }52]
```

## 检索场景 （业务方使用）

### 指定时刻检索

案例 1：指定某一时刻 t-0 (2025-12-23 12:03:00) 和 pod 的 id，检查是否有相关的续期记录

语句：

代码解释 代码改写
```
16 1-- 指定某一时刻 t-0 (2025-12-23 12:03:00) 和 pod 的 id，检查是否有相关的续期记录2-- 预期返回为 pod_0，也就是 pod_0 在t-0 时刻位于（2025-12-23 12:00:00～2025-12-23 12:04:59） 有效区间3LET $start_time = 1766462580; -- 2025-12-23 12:03:004LET $end_time = 1766462580; -- 2025-12-23 12:03:005LET $pod_id = pod:⟨pod-0:default:BCS-K8S-001⟩;6-- 辅助函数7select * from only pod 8 where id=$pod_id 9 and fn::check_liveness_range_exists("pod", $pod_id, $start_time, $end_time);10 11 12-- 组合查询13SELECT * FROM pod 14where id = $pod_id15AND (SELECT count() FROM only pod_liveness_record WHERE pod_id = $pod_id and $end_time >= period_start 16AND $start_time <= period_end group all)>0;
```

检索结果：

```
10 1-------- Query 4 (1ms) --------2​3{4 bcs_cluster_id: 'BCS-K8S-001',5 created_at: 1766462400,6 id: pod:⟨pod-0:default:BCS-K8S-001⟩,7 name: 'pod-0',8 namespace: 'default',9 updated_at: 176646318010}
```

- 案例 2：指定某一时刻 t-1 (2025-12-23 12:06:00) 和 pod 的 id，检查是否有相关的续期记录

语句：

代码解释 代码改写
```
17 1-- 指定某一时刻 t-1 (2025-12-23 12:06:00) 和 pod 的 id，检查是否有相关的续期记录2-- 预期返回为空，因为既不在 （2025-12-23 12:00:00～2025-12-23 12:04:59），也不在 （2025-12-23 12:10:00～2025-12-23 12:13:00）有效区间3LET $start_time = 1766462760; -- 2025-12-23 12:06:004LET $end_time = 1766462760; -- 2025-12-23 12:06:005LET $pod_id = pod:⟨pod-0:default:BCS-K8S-001⟩;6​7-- 辅助函数8select * from only pod 9 where id=$pod_id 10 and fn::check_liveness_range_exists("pod", $pod_id, $start_time, $end_time);11 12 13-- 组合查询14SELECT * FROM pod 15where id = $pod_id16AND (SELECT count() FROM only pod_liveness_record WHERE pod_id = $pod_id and $end_time >= period_start 17AND $start_time <= period_end group all)>0;
```

检索结果：

```
3 1-------- Query 4 (1ms) --------2​3NONE
```

- 案例 3：指定某一时刻 t-2 (2025-12-23 12:12:00) 和 pod 的 id，检查是否有相关的续期记录

语句：

代码解释 代码改写
```
17 1-- 指定某一时刻 t-2 (2025-12-23 12:12:00) 和 pod 的 id，检查是否有相关的续期记录2-- 预期返回 pod_0，因为存在（2025-12-23 12:10:00～2025-12-23 12:13:00）有效续期区间3LET $start_time = 1766463120; -- 2025-12-23 12:12:004LET $end_time = 1766463120; -- 2025-12-23 12:12:005LET $pod_id = pod:⟨pod-0:default:BCS-K8S-001⟩;6-- 辅助函数7select * from only pod 8 where id=$pod_id 9 and fn::check_liveness_range_exists("pod", $pod_id, $start_time, $end_time);10 11 12-- 组合查询13SELECT * FROM pod 14where id = $pod_id15AND (SELECT count() FROM only pod_liveness_record WHERE pod_id = $pod_id and $end_time >= period_start 16AND $start_time <= period_end group all)>0;17​
```

检索结果：

```
10 1-------- Query 4 (1ms) --------2​3{4 bcs_cluster_id: 'BCS-K8S-001',5 created_at: 1766462400,6 id: pod:⟨pod-0:default:BCS-K8S-001⟩,7 name: 'pod-0',8 namespace: 'default',9 updated_at: 176646318010}
```

### 指定区间检索

- 案例：指定某一区间范围 t-0 ~ t-1 (2025-12-23 12:03:00 ~ 2025-12-23 12:06:00) pod 的 id，检查是否有相关的续期记录

语句：

代码解释 代码改写
```
17 1-- 指定某一区间范围 t-0 ~ t-1 (2025-12-23 12:03:00 ~ 2025-12-23 12:06:00) ,pod 的 id，检查是否有相关的续期记录2-- 预期返回 pod_0,因为存在 （2025-12-23 12:00:00～2025-12-23 12:04:59） 有效续期区间3LET $start_time = 1766462580; -- 2025-12-23 12:03:004LET $end_time = 1766462760; -- 2025-12-23 12:06:005LET $pod_id = pod:⟨pod-0:default:BCS-K8S-001⟩;6​7-- 辅助函数8select * from only pod 9 where id=$pod_id 10 and fn::check_liveness_range_exists("pod", $pod_id, $start_time, $end_time);11 12 13-- 组合查询14SELECT * FROM pod 15where id = $pod_id16AND (SELECT count() FROM only pod_liveness_record WHERE pod_id = $pod_id and $end_time >= period_start 17AND $start_time <= period_end group all)>0;
```

检索结果：

```
8 1{2 bcs_cluster_id: 'BCS-K8S-001',3 created_at: 1766462400,4 id: pod:⟨pod-0:default:BCS-K8S-001⟩,5 name: 'pod-0',6 namespace: 'default',7 updated_at: 17664631808}
```)**[T0 时刻模拟上报（初始化）](#T0-时刻模拟上报（初始化）)**[T1 模拟时刻上报（续期）](#T1-模拟时刻上报（续期）)**[T2 时刻模拟上报（过期）](#T2-时刻模拟上报（过期）)**[T3 时刻模拟上报（续期）](#T3-时刻模拟上报（续期）)**[检索场景 （业务方使用）](#检索场景-（业务方使用）)**[指定时刻检索](#指定时刻检索)**[指定区间检索](#指定区间检索)
