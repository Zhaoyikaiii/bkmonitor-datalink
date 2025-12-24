# 资源关联查询实现方案

## 一、整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              HTTP Handler                                    │
│                         (HandlerQueryGraph)                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           QueryRequest                                       │
│                    (统一的外部查询请求结构)                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GraphQueryService                                   │
│                        (查询服务协调层)                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. 解析请求参数                                                      │    │
│  │  2. 构建 GraphQuery（中间查询表示）                                    │    │
│  │  3. 调用 GraphStore 执行图查询                                        │    │
│  │  4. 调用 DynamicDataFetcher 获取动态数据                              │    │
│  │  5. 组装最终响应                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
┌───────────────────────────────┐       ┌───────────────────────────────┐
│         GraphStore            │       │     DynamicDataFetcher        │
│      (图存储抽象接口)          │       │      (动态数据获取器)          │
└───────────────────────────────┘       └───────────────────────────────┘
                    │                                   │
                    ▼                                   ▼
┌───────────────────────────────┐       ┌───────────────────────────────┐
│       GraphQuery              │       │   VictoriaMetrics / ES        │
│    (中间查询表示层)            │       │      (时序/日志存储)           │
└───────────────────────────────┘       └───────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────┐
│       DSLTranslator           │
│      (DSL 转换器接口)          │
│  ┌─────────────────────────┐  │
│  │  - SurrealDBTranslator  │  │
│  │  - Neo4jTranslator      │  │
│  │  - DgraphTranslator     │  │
│  │  - ...                  │  │
│  └─────────────────────────┘  │
└───────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────┐
│     Graph Database            │
│   (SurrealDB / Neo4j / ...)   │
└───────────────────────────────┘
```

## 二、核心数据结构设计

### 2.1 中间查询表示层（GraphQuery）

GraphQuery 是与具体图数据库无关的中间查询表示，包含：

```go
// GraphQuery 中间查询表示
type GraphQuery struct {
    // 查询类型
    QueryType QueryType // PathQuery | TopologyExpand
    
    // 来源节点条件
    SourceNode NodeMatcher
    
    // 目标节点条件（PathQuery 时使用）
    TargetNode *NodeMatcher
    
    // 路径约束
    PathConstraint PathConstraint
    
    // 时间有效性
    TimeValidity TimeValidity
    
    // 返回选项
    ReturnOptions ReturnOptions
}

// NodeMatcher 节点匹配条件
type NodeMatcher struct {
    // 节点类型
    NodeType ResourceType
    
    // 属性匹配条件
    Properties map[string]PropertyMatcher
}

// PropertyMatcher 属性匹配器
type PropertyMatcher struct {
    Operator  MatchOperator // EQ, NEQ, IN, LIKE, GT, LT, GTE, LTE
    Value     any
    Values    []any         // IN 操作使用
}

// PathConstraint 路径约束
type PathConstraint struct {
    // 最大跳数
    MaxHops int
    
    // 最小跳数
    MinHops int
    
    // 允许的关系类型
    AllowedRelationTypes []RelationType
    
    // 动态关系方向
    DynamicDirection DynamicRelationDirection
    
    // 路径必须经过的中间节点类型
    RequiredIntermediateTypes []ResourceType
    
    // 允许的关系名称（可选，进一步限制）
    AllowedRelationNames []string
    
    // 是否允许环路
    AllowCycles bool
}

// TimeValidity 时间有效性约束
type TimeValidity struct {
    Timestamp     int64
    LookbackDelta time.Duration
}

// ReturnOptions 返回选项
type ReturnOptions struct {
    // 是否返回路径详情
    IncludePath bool
    
    // 是否返回节点属性
    IncludeNodeProperties bool
    
    // 是否返回关系属性
    IncludeRelationProperties bool
    
    // 结果限制
    Limit int
}
```

### 2.2 查询结果（GraphResult）

```go
// GraphResult 图查询结果
type GraphResult struct {
    // 匹配的路径列表
    Paths []GraphPath
    
    // 所有涉及的节点（去重）
    Nodes map[string]*GraphNode
    
    // 所有涉及的关系（去重）
    Relations map[string]*GraphRelation
}

// GraphPath 单条路径
type GraphPath struct {
    PathID   string
    Hops     int
    Elements []PathElement // 交替出现：Node -> Relation -> Node -> ...
}

// GraphNode 图节点
type GraphNode struct {
    ID         string
    Type       ResourceType
    Properties map[string]any
    CreatedAt  int64
    UpdatedAt  int64
}

// GraphRelation 图关系
type GraphRelation struct {
    ID            string
    Type          string
    RelationClass RelationType
    SourceID      string
    TargetID      string
    Properties    map[string]any
    CreatedAt     int64
    UpdatedAt     int64
}
```

## 三、DSL 转换器设计

### 3.1 DSLTranslator 接口

```go
// DSLTranslator DSL 转换器接口
type DSLTranslator interface {
    // TranslatePathQuery 转换路径查询
    TranslatePathQuery(query *GraphQuery) (string, map[string]any, error)
    
    // TranslateTopologyExpand 转换拓扑展开查询
    TranslateTopologyExpand(query *GraphQuery) (string, map[string]any, error)
    
    // TranslateNodeQuery 转换节点查询
    TranslateNodeQuery(matcher *NodeMatcher, timeValidity *TimeValidity) (string, map[string]any, error)
    
    // ParseResult 解析查询结果
    ParseResult(raw any) (*GraphResult, error)
}
```

### 3.2 SurrealDB DSL 转换实现

#### 3.2.1 节点查询转换

**输入（GraphQuery）：**
```go
NodeMatcher{
    NodeType: "pod",
    Properties: map[string]PropertyMatcher{
        "bcs_cluster_id": {Operator: EQ, Value: "BCS-K8S-00000"},
        "namespace":      {Operator: EQ, Value: "bkmonitor-operator"},
        "pod":            {Operator: EQ, Value: "bkm-pod-1"},
    },
}
TimeValidity{
    Timestamp:     1734249600000,
    LookbackDelta: 10 * time.Minute,
}
```

**输出（SurrealQL）：**
```sql
SELECT * FROM pod 
WHERE bcs_cluster_id = $bcs_cluster_id 
  AND namespace = $namespace 
  AND pod = $pod
  AND created_at <= $timestamp
  AND updated_at >= $lookback_threshold;
```

**参数绑定：**
```go
map[string]any{
    "bcs_cluster_id":     "BCS-K8S-00000",
    "namespace":          "bkmonitor-operator",
    "pod":                "bkm-pod-1",
    "timestamp":          1734249600000,
    "lookback_threshold": 1734249000000, // timestamp - 10min
}
```

#### 3.2.2 路径查询转换（场景一：指定目标类型）

**输入（GraphQuery）：**
```go
GraphQuery{
    QueryType: PathQuery,
    SourceNode: NodeMatcher{
        NodeType: "pod",
        Properties: map[string]PropertyMatcher{
            "bcs_cluster_id": {Operator: EQ, Value: "BCS-K8S-00000"},
            "namespace":      {Operator: EQ, Value: "bkmonitor-operator"},
            "pod":            {Operator: EQ, Value: "bkm-pod-1"},
        },
    },
    TargetNode: &NodeMatcher{
        NodeType: "service",
    },
    PathConstraint: PathConstraint{
        MaxHops:              3,
        AllowedRelationTypes: []RelationType{RelationTypeStatic, RelationTypeDynamic},
        DynamicDirection:     DirectionBoth,
    },
    TimeValidity: TimeValidity{
        Timestamp:     1734249600000,
        LookbackDelta: 10 * time.Minute,
    },
}
```

**输出（SurrealQL）：**
```sql
-- 方案一：使用 RELATE 和图遍历语法
LET $source = (
    SELECT * FROM pod 
    WHERE bcs_cluster_id = $bcs_cluster_id 
      AND namespace = $namespace 
      AND pod = $pod
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
    LIMIT 1
);

-- 查找从 source 到 service 类型节点的所有路径（最多3跳）
SELECT 
    id,
    ->?..3->service AS paths
FROM $source
WHERE paths != NONE;

-- 方案二：使用递归 CTE 风格（更通用）
-- SurrealDB 支持通过 RELATE 遍历图
SELECT * FROM (
    SELECT 
        id AS source_id,
        (
            SELECT 
                out.id AS target_id,
                id AS relation_id,
                out AS target_node
            FROM ->pod_with_service, ->pod_to_pod->?->pod_with_service
            WHERE out.meta.tb = 'service'
              AND created_at <= $timestamp
              AND updated_at >= $lookback_threshold
        ) AS paths
    FROM pod
    WHERE bcs_cluster_id = $bcs_cluster_id 
      AND namespace = $namespace 
      AND pod = $pod
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
);
```

**更精确的 SurrealQL（使用图遍历函数）：**
```sql
-- 定义起点
LET $start = (
    SELECT id FROM pod 
    WHERE bcs_cluster_id = $bcs_cluster_id 
      AND namespace = $namespace 
      AND pod = $pod
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
    LIMIT 1
)[0].id;

-- BFS 查找路径
-- 使用 SurrealDB 的图遍历语法
-- ->relation_name-> 表示沿着关系遍历
-- ?..n 表示 0 到 n 跳

-- 静态关系遍历（双向）
SELECT 
    id,
    <->pod_with_service<->.id AS connected_services
FROM $start
WHERE connected_services IS NOT NONE;

-- 带路径信息的查询
SELECT 
    $start AS source,
    path,
    path[$-1] AS target
FROM (
    SELECT graph::path($start, service, 3) AS path
    FROM service
    WHERE created_at <= $timestamp
      AND updated_at >= $lookback_threshold
)
WHERE path IS NOT NONE;
```

#### 3.2.3 拓扑展开查询转换（场景二：不指定目标类型）

**输入（GraphQuery）：**
```go
GraphQuery{
    QueryType: TopologyExpand,
    SourceNode: NodeMatcher{
        NodeType: "pod",
        Properties: map[string]PropertyMatcher{
            "bcs_cluster_id": {Operator: EQ, Value: "BCS-K8S-00000"},
            "namespace":      {Operator: EQ, Value: "bkmonitor-operator"},
            "pod":            {Operator: EQ, Value: "bkm-pod-1"},
        },
    },
    PathConstraint: PathConstraint{
        MaxHops:              3,
        AllowedRelationTypes: []RelationType{RelationTypeStatic},
    },
    TimeValidity: TimeValidity{
        Timestamp:     1734249600000,
        LookbackDelta: 10 * time.Minute,
    },
}
```

**输出（SurrealQL）：**
```sql
-- 定义起点
LET $start = (
    SELECT id FROM pod 
    WHERE bcs_cluster_id = $bcs_cluster_id 
      AND namespace = $namespace 
      AND pod = $pod
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
    LIMIT 1
)[0].id;

-- 第1跳邻居
LET $hop1 = (
    SELECT 
        out AS node,
        id AS relation,
        1 AS hop
    FROM $start->*
    WHERE out.created_at <= $timestamp
      AND out.updated_at >= $lookback_threshold
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
);

-- 第2跳邻居
LET $hop2 = (
    SELECT 
        out AS node,
        id AS relation,
        2 AS hop
    FROM $hop1.node->*
    WHERE out.created_at <= $timestamp
      AND out.updated_at >= $lookback_threshold
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
      AND out NOT IN [$start, $hop1.node]
);

-- 第3跳邻居
LET $hop3 = (
    SELECT 
        out AS node,
        id AS relation,
        3 AS hop
    FROM $hop2.node->*
    WHERE out.created_at <= $timestamp
      AND out.updated_at >= $lookback_threshold
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
      AND out NOT IN [$start, $hop1.node, $hop2.node]
);

-- 返回结果
RETURN {
    source: $start,
    hop1: $hop1,
    hop2: $hop2,
    hop3: $hop3
};
```

#### 3.2.4 带关系类型过滤的查询

**静态关系查询（双向）：**
```sql
-- 只查询静态关系
SELECT 
    out AS target,
    id AS relation
FROM $start->(
    pod_with_service |
    pod_with_replicaset |
    container_with_pod |
    node_with_pod
)->*
WHERE out.created_at <= $timestamp
  AND out.updated_at >= $lookback_threshold;
```

**动态关系查询（有方向）：**
```sql
-- 出站方向（outbound）
SELECT 
    out AS target,
    id AS relation
FROM $start->(pod_to_pod | pod_to_system)->*
WHERE out.created_at <= $timestamp
  AND out.updated_at >= $lookback_threshold;

-- 入站方向（inbound）
SELECT 
    in AS source,
    id AS relation
FROM $start<-(pod_to_pod | system_to_pod)<-*
WHERE in.created_at <= $timestamp
  AND in.updated_at >= $lookback_threshold;

-- 双向
SELECT 
    out AS target,
    id AS relation
FROM $start<->(pod_to_pod | pod_to_system | system_to_pod)<->*
WHERE out.created_at <= $timestamp
  AND out.updated_at >= $lookback_threshold;
```

## 四、查询流转详细流程

### 4.1 完整查询流程

```
1. HTTP Request
   │
   ▼
2. JSON Unmarshal -> QueryRequest
   │
   ▼
3. Validate QueryRequest
   │
   ▼
4. For each SingleQuery in QueryList:
   │
   ├── 4.1 Build GraphQuery (中间表示)
   │   │
   │   ├── 构建 SourceNode NodeMatcher
   │   ├── 构建 TargetNode NodeMatcher (如果有)
   │   ├── 构建 PathConstraint
   │   └── 构建 TimeValidity
   │
   ├── 4.2 GraphStore.Execute(GraphQuery)
   │   │
   │   ├── DSLTranslator.Translate(GraphQuery)
   │   │   │
   │   │   └── 生成 SurrealQL + 参数
   │   │
   │   ├── SurrealDB.Query(sql, params)
   │   │
   │   └── DSLTranslator.ParseResult(raw)
   │       │
   │       └── 返回 GraphResult
   │
   ├── 4.3 如果需要动态数据:
   │   │
   │   ├── 从 GraphResult 提取节点和关系 ID
   │   │
   │   ├── 构建动态数据查询条件
   │   │
   │   └── DynamicDataFetcher.Fetch()
   │       │
   │       ├── Metrics -> VictoriaMetrics Query
   │       ├── Logs -> Elasticsearch Query
   │       └── Trace -> Elasticsearch Query
   │
   └── 4.4 组装 SingleQueryResponse
   │
   ▼
5. 组装 QueryResponse
   │
   ▼
6. JSON Marshal -> HTTP Response
```

### 4.2 详细示例：Pod -> Service 查询

**请求：**
```json
{
  "query_list": [{
    "timestamp": 1734249600000,
    "source_type": "pod",
    "source_info": {
      "bcs_cluster_id": "BCS-K8S-00000",
      "namespace": "bkmonitor-operator",
      "pod": "bkm-pod-1"
    },
    "target_type": "service",
    "allowed_relation_types": ["static"],
    "look_back_delta": "10m"
  }]
}
```

**Step 1: 构建 GraphQuery**
```go
graphQuery := &GraphQuery{
    QueryType: PathQuery,
    SourceNode: NodeMatcher{
        NodeType: ResourcePod,
        Properties: map[string]PropertyMatcher{
            "bcs_cluster_id": {Operator: EQ, Value: "BCS-K8S-00000"},
            "namespace":      {Operator: EQ, Value: "bkmonitor-operator"},
            "pod":            {Operator: EQ, Value: "bkm-pod-1"},
        },
    },
    TargetNode: &NodeMatcher{
        NodeType: ResourceService,
    },
    PathConstraint: PathConstraint{
        MaxHops:              10, // 默认值
        AllowedRelationTypes: []RelationType{RelationTypeStatic},
    },
    TimeValidity: TimeValidity{
        Timestamp:     1734249600000,
        LookbackDelta: 10 * time.Minute,
    },
}
```

**Step 2: DSL 转换**
```go
translator := NewSurrealDBTranslator()
sql, params, err := translator.TranslatePathQuery(graphQuery)
```

**生成的 SurrealQL：**
```sql
-- 1. 查找起始节点
LET $source = (
    SELECT * FROM pod 
    WHERE bcs_cluster_id = $p_bcs_cluster_id 
      AND namespace = $p_namespace 
      AND pod = $p_pod
      AND created_at <= $timestamp
      AND updated_at >= $lookback_threshold
    LIMIT 1
);

-- 2. 如果起始节点不存在，返回空
IF $source = [] THEN
    RETURN [];
END;

-- 3. BFS 查找到 service 的路径
-- 由于只允许静态关系，遍历双向关系
LET $paths = (
    SELECT 
        $source[0].id AS source_id,
        id AS target_id,
        meta::tb(id) AS target_type,
        -- 路径信息
        {
            elements: [
                { type: 'node', data: $source[0] },
                { type: 'relation', data: relation },
                { type: 'node', data: this }
            ]
        } AS path_info
    FROM (
        -- 1跳：直接连接的 service
        SELECT *, 
               (SELECT * FROM ->pod_with_service LIMIT 1)[0] AS relation
        FROM service
        WHERE id IN $source[0]->pod_with_service->service.id
          AND created_at <= $timestamp
          AND updated_at >= $lookback_threshold
    )
);

-- 4. 返回结果
RETURN $paths;
```

**参数：**
```go
params := map[string]any{
    "p_bcs_cluster_id":   "BCS-K8S-00000",
    "p_namespace":        "bkmonitor-operator",
    "p_pod":              "bkm-pod-1",
    "timestamp":          1734249600000,
    "lookback_threshold": 1734249000000,
}
```

**Step 3: 执行查询并解析结果**
```go
rawResult, err := surrealClient.Query(sql, params)
graphResult, err := translator.ParseResult(rawResult)
```

**Step 4: 组装响应**
```go
response := &SingleQueryResponse{
    QueryIndex: 0,
    Timestamp:  query.Timestamp,
    SourceType: query.SourceType,
    SourceInfo: query.SourceInfo,
    TargetType: query.TargetType,
    TargetList: convertToTargetList(graphResult),
    Total:      len(graphResult.Paths),
}
```

## 五、代码结构设计

```
pkg/unify-query/tsdb/graph/
├── types.go              # 基础类型定义（ResourceType, RelationType 等）
├── request.go            # 请求结构体（QueryRequest, SingleQuery）
├── response.go           # 响应结构体（QueryResponse, SingleQueryResponse）
├── graph_query.go        # 中间查询表示（GraphQuery, NodeMatcher 等）
├── graph_result.go       # 查询结果（GraphResult, GraphPath 等）
├── id_gen.go             # ID 生成（已存在）
│
├── translator/
│   ├── interface.go      # DSLTranslator 接口定义
│   ├── surrealdb.go      # SurrealDB DSL 转换实现
│   ├── surrealdb_test.go # SurrealDB 转换测试
│   ├── neo4j.go          # Neo4j Cypher 转换（可选）
│   └── builder.go        # 通用 SQL 构建器
│
├── store/
│   ├── interface.go      # GraphStore 接口定义
│   ├── surrealdb.go      # SurrealDB 存储实现
│   └── mock.go           # Mock 实现（用于测试）
│
├── fetcher/
│   ├── interface.go      # DynamicDataFetcher 接口
│   ├── metrics.go        # 指标数据获取（VictoriaMetrics）
│   ├── logs.go           # 日志数据获取（Elasticsearch）
│   └── trace.go          # Trace 数据获取（Elasticsearch）
│
├── service.go            # GraphQueryService 主服务
├── service_test.go       # 服务测试
├── instance.go           # Instance 实现（实现 tsdb.Instance 接口）
└── settings.go           # 配置
```

## 六、关键接口定义

### 6.1 GraphStore 接口

```go
// GraphStore 图存储接口
type GraphStore interface {
    // QueryPaths 执行路径查询
    QueryPaths(ctx context.Context, query *GraphQuery) (*GraphResult, error)
    
    // QueryTopology 执行拓扑展开查询
    QueryTopology(ctx context.Context, query *GraphQuery) (*GraphResult, error)
    
    // QueryNodes 查询节点
    QueryNodes(ctx context.Context, matcher *NodeMatcher, timeValidity *TimeValidity) ([]*GraphNode, error)
    
    // QueryRelations 查询关系
    QueryRelations(ctx context.Context, nodeID string, direction Direction, relationTypes []string) ([]*GraphRelation, error)
    
    // GetNode 获取单个节点
    GetNode(ctx context.Context, nodeID string) (*GraphNode, error)
    
    // GetRelation 获取单个关系
    GetRelation(ctx context.Context, relationID string) (*GraphRelation, error)
    
    // Ping 健康检查
    Ping(ctx context.Context) error
    
    // Close 关闭连接
    Close() error
}
```

### 6.2 DSLTranslator 接口

```go
// DSLTranslator DSL 转换器接口
type DSLTranslator interface {
    // Name 返回转换器名称
    Name() string
    
    // TranslatePathQuery 转换路径查询为 DSL
    // 返回: DSL 语句, 参数绑定, 错误
    TranslatePathQuery(query *GraphQuery) (string, map[string]any, error)
    
    // TranslateTopologyExpand 转换拓扑展开查询为 DSL
    TranslateTopologyExpand(query *GraphQuery) (string, map[string]any, error)
    
    // TranslateNodeQuery 转换节点查询为 DSL
    TranslateNodeQuery(matcher *NodeMatcher, tv *TimeValidity) (string, map[string]any, error)
    
    // TranslateRelationQuery 转换关系查询为 DSL
    TranslateRelationQuery(nodeID string, direction Direction, relationTypes []string, tv *TimeValidity) (string, map[string]any, error)
    
    // ParsePathResult 解析路径查询结果
    ParsePathResult(raw any) (*GraphResult, error)
    
    // ParseNodeResult 解析节点查询结果
    ParseNodeResult(raw any) ([]*GraphNode, error)
}
```

### 6.3 DynamicDataFetcher 接口

```go
// DynamicDataFetcher 动态数据获取器接口
type DynamicDataFetcher interface {
    // FetchMetrics 获取指标数据
    FetchMetrics(ctx context.Context, req *MetricsFetchRequest) (*MetricsFetchResult, error)
    
    // FetchLogs 获取日志数据
    FetchLogs(ctx context.Context, req *LogsFetchRequest) (*LogsFetchResult, error)
    
    // FetchTraces 获取 Trace 数据
    FetchTraces(ctx context.Context, req *TraceFetchRequest) (*TraceFetchResult, error)
}

// MetricsFetchRequest 指标获取请求
type MetricsFetchRequest struct {
    // 实体列表（节点 ID -> 实体类型）
    Entities map[string]ResourceType
    
    // 关系列表（关系 ID -> 关系类型）
    Relations map[string]string
    
    // 指标过滤
    EntityMetrics   map[string][]string // 实体类型 -> 指标名称列表
    RelationMetrics map[string][]string // 关系类型 -> 指标名称列表
    
    // 时间范围
    Start int64
    End   int64
}
```

## 七、SurrealDB 具体查询示例

### 7.1 表结构

```sql
-- 节点表：pod
DEFINE TABLE pod SCHEMAFULL;
DEFINE FIELD id ON pod TYPE string;
DEFINE FIELD bcs_cluster_id ON pod TYPE string;
DEFINE FIELD namespace ON pod TYPE string;
DEFINE FIELD pod ON pod TYPE string;
DEFINE FIELD created_at ON pod TYPE int;
DEFINE FIELD updated_at ON pod TYPE int;
DEFINE INDEX idx_pod_key ON pod FIELDS bcs_cluster_id, namespace, pod UNIQUE;
DEFINE INDEX idx_pod_time ON pod FIELDS created_at, updated_at;

-- 关系表：pod_with_service
DEFINE TABLE pod_with_service SCHEMAFULL TYPE RELATION IN pod OUT service;
DEFINE FIELD created_at ON pod_with_service TYPE int;
DEFINE FIELD updated_at ON pod_with_service TYPE int;

-- 动态关系表：pod_to_pod
DEFINE TABLE pod_to_pod SCHEMAFULL TYPE RELATION IN pod OUT pod;
DEFINE FIELD created_at ON pod_to_pod TYPE int;
DEFINE FIELD updated_at ON pod_to_pod TYPE int;
```

### 7.2 查询示例

#### 7.2.1 查找 Pod 直接关联的 Service

```sql
-- 输入: pod (bcs_cluster_id=BCS-K8S-00000, namespace=bkmonitor-operator, pod=bkm-pod-1)
-- 输出: 所有直接关联的 service

LET $timestamp = 1734249600000;
LET $lookback = 1734249000000;

SELECT 
    in.id AS source_id,
    out.id AS target_id,
    out.bcs_cluster_id,
    out.namespace,
    out.service,
    id AS relation_id,
    created_at AS relation_created_at,
    updated_at AS relation_updated_at
FROM pod_with_service
WHERE in.bcs_cluster_id = 'BCS-K8S-00000'
  AND in.namespace = 'bkmonitor-operator'
  AND in.pod = 'bkm-pod-1'
  AND in.created_at <= $timestamp
  AND in.updated_at >= $lookback
  AND out.created_at <= $timestamp
  AND out.updated_at >= $lookback
  AND created_at <= $timestamp
  AND updated_at >= $lookback;
```

#### 7.2.2 查找 Service -> Pod -> Pod -> Service 路径

```sql
LET $timestamp = 1734249600000;
LET $lookback = 1734249000000;

-- 1. 找到起始 service
LET $start_service = (
    SELECT * FROM service 
    WHERE bcs_cluster_id = 'BCS-K8S-00000'
      AND namespace = 'bkmonitor-operator'
      AND service = 'svc-1'
      AND created_at <= $timestamp
      AND updated_at >= $lookback
    LIMIT 1
)[0];

-- 2. 找到与起始 service 关联的 pod
LET $source_pods = (
    SELECT out AS pod, id AS relation FROM pod_with_service
    WHERE in = $start_service.id
      AND out.created_at <= $timestamp
      AND out.updated_at >= $lookback
      AND created_at <= $timestamp
      AND updated_at >= $lookback
);

-- 3. 找到这些 pod 通过 pod_to_pod 关联的其他 pod
LET $target_pods = (
    SELECT 
        out AS pod,
        id AS relation,
        in AS source_pod
    FROM pod_to_pod
    WHERE in IN $source_pods.pod.id
      AND out.created_at <= $timestamp
      AND out.updated_at >= $lookback
      AND created_at <= $timestamp
      AND updated_at >= $lookback
);

-- 4. 找到目标 pod 关联的 service
LET $target_services = (
    SELECT 
        out AS service,
        id AS relation,
        in AS source_pod
    FROM pod_with_service
    WHERE in IN $target_pods.pod.id
      AND out.id != $start_service.id  -- 排除起始 service
      AND out.created_at <= $timestamp
      AND out.updated_at >= $lookback
      AND created_at <= $timestamp
      AND updated_at >= $lookback
);

-- 5. 组装路径
RETURN {
    source: $start_service,
    paths: $target_services
};
```

#### 7.2.3 拓扑展开（3跳）

```sql
LET $timestamp = 1734249600000;
LET $lookback = 1734249000000;

-- 起始节点
LET $start = (
    SELECT * FROM pod 
    WHERE bcs_cluster_id = 'BCS-K8S-00000'
      AND namespace = 'bkmonitor-operator'
      AND pod = 'bkm-pod-1'
      AND created_at <= $timestamp
      AND updated_at >= $lookback
    LIMIT 1
)[0];

-- 使用 SurrealDB 的图遍历
-- <-> 表示双向遍历（静态关系）
-- -> 表示单向遍历（动态关系出站）
-- <- 表示反向遍历（动态关系入站）

-- 第1跳
LET $hop1 = SELECT 
    out.id AS node_id,
    meta::tb(out.id) AS node_type,
    out AS node_data,
    id AS relation_id,
    meta::tb(id) AS relation_type,
    1 AS hop
FROM (
    SELECT * FROM $start.id->(
        pod_with_service |
        pod_with_replicaset |
        container_with_pod |
        node_with_pod |
        pod_to_pod
    )
    WHERE out.created_at <= $timestamp
      AND out.updated_at >= $lookback
);

-- 第2跳（排除已访问节点）
LET $visited1 = array::concat([$start.id], $hop1.node_id);
LET $hop2 = SELECT 
    out.id AS node_id,
    meta::tb(out.id) AS node_type,
    out AS node_data,
    id AS relation_id,
    meta::tb(id) AS relation_type,
    2 AS hop
FROM (
    SELECT * FROM $hop1.node_id->*
    WHERE out.created_at <= $timestamp
      AND out.updated_at >= $lookback
      AND out.id NOT IN $visited1
);

-- 第3跳
LET $visited2 = array::concat($visited1, $hop2.node_id);
LET $hop3 = SELECT 
    out.id AS node_id,
    meta::tb(out.id) AS node_type,
    out AS node_data,
    id AS relation_id,
    meta::tb(id) AS relation_type,
    3 AS hop
FROM (
    SELECT * FROM $hop2.node_id->*
    WHERE out.created_at <= $timestamp
      AND out.updated_at >= $lookback
      AND out.id NOT IN $visited2
);

RETURN {
    source: $start,
    topology: [
        { hop: 1, entities: $hop1 },
        { hop: 2, entities: $hop2 },
        { hop: 3, entities: $hop3 }
    ]
};
```

## 八、配置设计

```yaml
graph:
  # 图存储配置
  store:
    type: surrealdb  # surrealdb | neo4j | dgraph
    address: "ws://localhost:8000/rpc"
    namespace: "bkmonitor"
    database: "graph"
    username: "root"
    password: "root"
    timeout: 30s
    max_connections: 10

  # 查询配置
  query:
    default_max_hops: 10
    default_lookback_delta: 10m
    max_results: 1000
    timeout: 60s

  # 动态数据配置
  dynamic_data:
    metrics:
      enabled: true
      source: victoria_metrics
    logs:
      enabled: true
      source: elasticsearch
    trace:
      enabled: true
      source: elasticsearch
```

## 九、错误处理

```go
// GraphError 图查询错误
type GraphError struct {
    Code    GraphErrorCode
    Message string
    Cause   error
}

type GraphErrorCode string

const (
    ErrCodeInvalidRequest    GraphErrorCode = "INVALID_REQUEST"
    ErrCodeSourceNotFound    GraphErrorCode = "SOURCE_NOT_FOUND"
    ErrCodeTargetNotFound    GraphErrorCode = "TARGET_NOT_FOUND"
    ErrCodePathNotFound      GraphErrorCode = "PATH_NOT_FOUND"
    ErrCodeTimeout           GraphErrorCode = "TIMEOUT"
    ErrCodeStoreUnavailable  GraphErrorCode = "STORE_UNAVAILABLE"
    ErrCodeDSLTranslation    GraphErrorCode = "DSL_TRANSLATION_ERROR"
    ErrCodeResultParsing     GraphErrorCode = "RESULT_PARSING_ERROR"
)
```

## 十、总结

本方案的核心设计原则：

1. **中间层抽象（GraphQuery）**：将外部请求转换为与具体图数据库无关的中间查询表示，便于支持多种图数据库后端。

2. **DSLTranslator 模式**：每种图数据库实现自己的 DSL 转换器，将 GraphQuery 转换为具体的查询语言（SurrealQL、Cypher 等）。

3. **分层架构**：
   - Handler 层：处理 HTTP 请求
   - Service 层：协调查询和数据组装
   - Store 层：图数据库访问
   - Translator 层：DSL 转换
   - Fetcher 层：动态数据获取

4. **可扩展性**：新增图数据库后端只需实现 DSLTranslator 和 GraphStore 接口。

5. **时间有效性**：所有查询都会考虑节点和关系的时间有效性，支持历史数据查询。

## 十一、真实数据示例（Mock 数据验证）

以下是通过 `001.mock_bkop_business_traffic.py` 脚本生成的真实数据示例，用于验证设计文档与实现的一致性。

### 11.1 节点数据示例

#### Pod 节点
```json
{
    "id": "pod:⟨pod:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-000⟩",
    "bcs_cluster_id": "BCS-K8S-00002",
    "namespace": "bkop",
    "pod": "bkop-pod-000",
    "created_at": "2025-12-23T15:52:34Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

**ID 格式说明**：`pod:{key=value,...}` 其中 keys 按字母序排列 (`bcs_cluster_id`, `namespace`, `pod`)

#### Node 节点
```json
{
    "id": "node:⟨node:bcs_cluster_id=BCS-K8S-00002,node=bkop-node-0⟩",
    "bcs_cluster_id": "BCS-K8S-00002",
    "node": "bkop-node-0",
    "created_at": "2025-12-23T15:25:03Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

#### Service 节点
```json
{
    "id": "service:⟨service:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,service=bkop-api⟩",
    "bcs_cluster_id": "BCS-K8S-00002",
    "namespace": "bkop",
    "service": "bkop-api",
    "created_at": "2025-12-23T16:06:14Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

#### Deployment 节点
```json
{
    "id": "deployment:⟨deployment:bcs_cluster_id=BCS-K8S-00002,deployment=bkop-api-deploy,namespace=bkop⟩",
    "bcs_cluster_id": "BCS-K8S-00002",
    "namespace": "bkop",
    "deployment": "bkop-api-deploy",
    "created_at": "2025-12-23T15:39:37Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

**注意**：Deployment ID 中的 keys 按字母序排列为 `bcs_cluster_id`, `deployment`, `namespace`

#### Metric 节点
```json
{
    "id": "metric:⟨metric:metric_name=pod_to_pod_flow_total⟩",
    "metric_name": "pod_to_pod_flow_total",
    "metric_type": "counter",
    "unit": "count",
    "description": "Pod到Pod的流量访问量",
    "created_at": "2025-12-23T15:51:44Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

### 11.2 静态关系数据示例

#### node_with_pod（Node 与 Pod 的双向关联）
```json
{
    "id": "node_with_pod:⟨node_with_pod:bcs_cluster_id=BCS-K8S-00002,node=bkop-node-0|bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-000⟩",
    "in": "node:⟨node:bcs_cluster_id=BCS-K8S-00002,node=bkop-node-0⟩",
    "out": "pod:⟨pod:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-000⟩",
    "created_at": "2025-12-23T16:13:27Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

**ID 格式说明**：
- 关系名：`node_with_pod`（node < pod 按字母序）
- ID 格式：`{relation_type}:{res1_kv}|{res2_kv}`
- 使用 `|` 分隔两个资源的维度信息

#### pod_with_service（Pod 与 Service 的双向关联）
```json
{
    "id": "pod_with_service:⟨pod_with_service:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-000|bcs_cluster_id=BCS-K8S-00002,namespace=bkop,service=bkop-api⟩",
    "in": "pod:⟨pod:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-000⟩",
    "out": "service:⟨service:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,service=bkop-api⟩",
    "created_at": "2025-12-23T15:47:55Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

#### deployment_with_replicaset（Deployment 与 ReplicaSet 的双向关联）
```json
{
    "id": "deployment_with_replicaset:⟨deployment_with_replicaset:bcs_cluster_id=BCS-K8S-00002,deployment=bkop-api-deploy,namespace=bkop|bcs_cluster_id=BCS-K8S-00002,namespace=bkop,replicaset=bkop-api-deploy-rs-001⟩",
    "in": "deployment:⟨deployment:bcs_cluster_id=BCS-K8S-00002,deployment=bkop-api-deploy,namespace=bkop⟩",
    "out": "replicaset:⟨replicaset:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,replicaset=bkop-api-deploy-rs-001⟩",
    "created_at": "2025-12-23T15:18:32Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

### 11.3 动态关系数据示例

#### pod_to_pod（Pod 到 Pod 的流量关联，有方向）
```json
{
    "id": "pod_to_pod:⟨pod_to_pod:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-002|bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-003⟩",
    "in": "pod:⟨pod:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-002⟩",
    "out": "pod:⟨pod:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-003⟩",
    "created_at": "2025-12-23T15:17:04Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

**ID 格式说明**：
- 关系名：`pod_to_pod`（使用 `_to_` 表示有方向性）
- ID 格式：`{src}_to_{dst}:{src_kv}|{dst_kv}`
- `in` 为源节点，`out` 为目标节点

### 11.4 指标关联数据示例

#### relation_has_metric（动态关系关联指标）
```json
{
    "id": "relation_has_metric:32o6xg5cvwrj9fy7bme7",
    "in": "pod_to_pod:⟨pod_to_pod:bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-002|bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=bkop-pod-003⟩",
    "out": "metric:⟨metric:metric_name=pod_to_pod_flow_total⟩",
    "result_table_id": "2_bkmonitor_bkop_2_pod_to_pod_flow_total",
    "created_at": "2025-12-23T16:15:52Z",
    "updated_at": "2025-12-23T16:15:52Z"
}
```

**说明**：
- `in`：指向 `pod_to_pod` 动态关系
- `out`：指向 `metric` 指标节点
- `result_table_id`：用于查询 VictoriaMetrics 的结果表 ID

### 11.5 数据统计摘要

| 资源类型 | 数量 | 说明 |
|---------|------|------|
| biz | 1 | 业务节点 |
| cluster | 1 | K8s 集群节点 |
| namespace | 1 | 命名空间节点 |
| node | 3 | K8s Node 节点 |
| pod | 10 | K8s Pod 节点 |
| service | 3 | K8s Service 节点 |
| deployment | 3 | K8s Deployment 节点 |
| replicaset | 3 | K8s ReplicaSet 节点 |
| metric | 3 | 指标定义节点 |
| node_with_pod | 10 | Node-Pod 静态关系 |
| pod_with_service | 10 | Pod-Service 静态关系 |
| deployment_with_replicaset | 3 | Deployment-ReplicaSet 静态关系 |
| pod_with_replicaset | 10 | Pod-ReplicaSet 静态关系 |
| pod_to_pod | 5 | Pod-Pod 动态流量关系 |
| relation_has_metric | 15 | 关系-指标关联 (5 relations × 3 metrics) |

### 11.6 ID 生成规则验证

通过真实数据验证，ID 生成规则与设计文档完全一致：

1. **节点 ID**：`{resource_type}:{key1=value1,key2=value2,...}`
   - Keys 按字母序排列
   - 使用 `=` 连接键值对，`,` 分隔多个键值对

2. **静态关系 ID**：`{res1}_with_{res2}:{res1_kv}|{res2_kv}`
   - `res1 < res2` 按字母序
   - 使用 `|` 分隔两个资源的维度信息

3. **动态关系 ID**：`{src}_to_{dst}:{src_kv}|{dst_kv}`
   - 按流量方向（源 -> 目标）排列
   - 使用 `|` 分隔源和目标的维度信息

4. **时间字段**：所有节点和关系都包含 `created_at` 和 `updated_at` 字段
