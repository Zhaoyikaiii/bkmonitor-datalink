// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package graph

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ResourceType 资源类型
type ResourceType string

const (
	ResourceTypePod         ResourceType = "pod"
	ResourceTypeNode        ResourceType = "node"
	ResourceTypeContainer   ResourceType = "container"
	ResourceTypeDeployment  ResourceType = "deployment"
	ResourceTypeReplicaSet  ResourceType = "replicaset"
	ResourceTypeStatefulSet ResourceType = "statefulset"
	ResourceTypeDaemonSet   ResourceType = "daemonset"
	ResourceTypeJob         ResourceType = "job"
	ResourceTypeService     ResourceType = "service"
	ResourceTypeIngress     ResourceType = "ingress"
	ResourceTypeCluster     ResourceType = "cluster"
	ResourceTypeNamespace   ResourceType = "namespace"

	ResourceTypeSystem     ResourceType = "system"
	ResourceTypeK8sAddress ResourceType = "k8s_address"
	ResourceTypeDomain     ResourceType = "domain"

	ResourceTypeAPMService         ResourceType = "apm_service"
	ResourceTypeAPMServiceInstance ResourceType = "apm_service_instance"

	ResourceTypeDataSource  ResourceType = "datasource"
	ResourceTypeBKLogConfig ResourceType = "bklogconfig"

	ResourceTypeBiz    ResourceType = "biz"
	ResourceTypeSet    ResourceType = "set"
	ResourceTypeModule ResourceType = "module"
	ResourceTypeHost   ResourceType = "host"

	ResourceTypeAppVersion  ResourceType = "app_version"
	ResourceTypeGitCommit   ResourceType = "git_commit"
	ResourceTypeEnvironment ResourceType = "environment"
)

// RelationType 关系类型
type RelationType string

const (
	// 静态关系（拓扑关系，双向）

	RelationNodeWithSystem           RelationType = "node_with_system"
	RelationNodeWithPod              RelationType = "node_with_pod"
	RelationJobWithPod               RelationType = "job_with_pod"
	RelationPodWithReplicaSet        RelationType = "pod_with_replicaset"
	RelationPodWithStatefulSet       RelationType = "pod_with_statefulset"
	RelationDaemonSetWithPod         RelationType = "daemonset_with_pod"
	RelationDeploymentWithReplicaSet RelationType = "deployment_with_replicaset"
	RelationPodWithService           RelationType = "pod_with_service"
	RelationIngressWithService       RelationType = "ingress_with_service"

	RelationK8sAddressWithService RelationType = "k8s_address_with_service"
	RelationDomainWithService     RelationType = "domain_with_service"

	RelationAPMServiceInstanceWithPod        RelationType = "apm_service_instance_with_pod"
	RelationAPMServiceInstanceWithSystem     RelationType = "apm_service_instance_with_system"
	RelationAPMServiceWithAPMServiceInstance RelationType = "apm_service_with_apm_service_instance"

	RelationContainerWithPod RelationType = "container_with_pod"

	RelationDataSourceWithPod         RelationType = "datasource_with_pod"
	RelationDataSourceWithNode        RelationType = "datasource_with_node"
	RelationBKLogConfigWithDataSource RelationType = "bklogconfig_with_datasource"

	RelationBizWithSet     RelationType = "biz_with_set"
	RelationModuleWithSet  RelationType = "module_with_set"
	RelationHostWithModule RelationType = "host_with_module"
	RelationHostWithSystem RelationType = "host_with_system"

	RelationAppVersionWithContainer  RelationType = "app_version_with_container"
	RelationAppVersionWithSystem     RelationType = "app_version_with_system"
	RelationContainerWithEnvironment RelationType = "container_with_environment"
	RelationEnvironmentWithSystem    RelationType = "environment_with_system"
	RelationAppVersionWithGitCommit  RelationType = "app_version_with_git_commit"

	RelationPodToPod         RelationType = "pod_to_pod"
	RelationPodToSystem      RelationType = "pod_to_system"
	RelationSystemToPod      RelationType = "system_to_pod"
	RelationSystemToSystem   RelationType = "system_to_system"
	RelationServiceToService RelationType = "service_to_service"
)

// RelationCategory 关系类别
type RelationCategory string

const (
	RelationCategoryStatic  RelationCategory = "static"  // 静态关系
	RelationCategoryDynamic RelationCategory = "dynamic" // 动态关系
)

// TraversalDirection 图遍历方向
type TraversalDirection string

const (
	DirectionOutbound TraversalDirection = "outbound" // 出向
	DirectionInbound  TraversalDirection = "inbound"  // 入向
	DirectionBoth     TraversalDirection = "both"     // 双向
)

// SurrealDB 字段名常量
const (
	FieldID           = "id"
	FieldIn           = "in"  // 关系源端
	FieldOut          = "out" // 关系目标端
	FieldCreatedAt    = "created_at"
	FieldUpdatedAt    = "updated_at"
	FieldPeriodStart  = "period_start"
	FieldPeriodEnd    = "period_end"
	FieldIsActive     = "is_active"
	FieldRelationType = "relation_type"
	FieldFromID       = "from_id"
	FieldToID         = "to_id"
)

// GetRelationCategory 获取关系类别
func GetRelationCategory(relationType RelationType) RelationCategory {
	switch relationType {
	case RelationPodToPod, RelationPodToSystem, RelationSystemToPod,
		RelationSystemToSystem, RelationServiceToService:
		return RelationCategoryDynamic
	default:
		return RelationCategoryStatic
	}
}

// Resource 资源实体
type Resource struct {
	ID        string            `json:"id"`
	Type      ResourceType      `json:"type"`
	Labels    map[string]string `json:"labels"`
	CreatedAt *time.Time        `json:"created_at,omitempty"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// LivenessRecord 资源存活记录
type LivenessRecord struct {
	ID          string `json:"id"`
	ResourceID  string `json:"resource_id"`
	PeriodStart int64  `json:"period_start"` // 毫秒时间戳
	PeriodEnd   int64  `json:"period_end"`   // 毫秒时间戳
	IsActive    bool   `json:"is_active"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

// VisiblePeriod 可见时间段（查询范围与资源存活周期的交集）
type VisiblePeriod struct {
	Start int64 `json:"start"` // 可见开始时间（毫秒时间戳）
	End   int64 `json:"end"`   // 可见结束时间（毫秒时间戳）
}

// Relation 资源关系
type Relation struct {
	ID        string       `json:"id"`
	Type      RelationType `json:"type"`
	FromID    string       `json:"from_id"`
	ToID      string       `json:"to_id"`
	CreatedAt *time.Time   `json:"created_at,omitempty"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// HopQueryRequest 单跳查询请求
type HopQueryRequest struct {
	Timestamp                int64              `json:"timestamp"`                            // 查询时间点（毫秒时间戳）
	SourceType               ResourceType       `json:"source_type"`                          // 源资源类型
	SourceInfo               map[string]string  `json:"source_info"`                          // 源资源过滤条件
	TargetType               ResourceType       `json:"target_type,omitempty"`                // 目标资源类型
	MaxHops                  int                `json:"max_hops,omitempty"`                   // 最大跳数
	AllowedRelationTypes     []RelationCategory `json:"allowed_relation_types,omitempty"`     // 允许的关系类型
	DynamicRelationDirection string             `json:"dynamic_relation_direction,omitempty"` // 动态关系方向
	LookBackDelta            string             `json:"look_back_delta,omitempty"`            // 回溯时间窗口
}

// HopQueryResponse 单跳查询响应
type HopQueryResponse struct {
	Timestamp  int64             `json:"timestamp"`
	SourceType ResourceType      `json:"source_type"`
	SourceInfo map[string]string `json:"source_info"`
	TargetType ResourceType      `json:"target_type,omitempty"`
	MaxHops    int               `json:"max_hops,omitempty"`
	TargetList []*TargetResult   `json:"target_list,omitempty"`
	Topology   []*TopologyLevel  `json:"topology,omitempty"`
	Total      int64             `json:"total"`
}

// TargetResult 目标资源结果
type TargetResult struct {
	Paths []*PathResult `json:"paths"`
}

// PathResult 路径结果
type PathResult struct {
	PathID string         `json:"path_id"`
	Hops   int            `json:"hops"`
	Path   []*PathElement `json:"path"`
}

// PathElement 路径元素（资源或关系）
type PathElement struct {
	EntityID     string            `json:"entity_id,omitempty"`
	EntityType   ResourceType      `json:"entity_type,omitempty"`
	EntityData   map[string]string `json:"entity_data,omitempty"`
	RelationType RelationType      `json:"relation_type,omitempty"`
	RelationID   string            `json:"relation_id,omitempty"`
	CreatedAt    int64             `json:"created_at,omitempty"`
	UpdatedAt    int64             `json:"updated_at,omitempty"`
}

// IsEntity 判断是否为资源元素
func (e *PathElement) IsEntity() bool {
	return e.EntityID != ""
}

// IsRelation 判断是否为关系元素
func (e *PathElement) IsRelation() bool {
	return e.RelationID != ""
}

// TopologyLevel 拓扑层级
type TopologyLevel struct {
	Hops     int               `json:"hops"`
	Entities []*TopologyEntity `json:"entities"`
}

// TopologyEntity 拓扑实体
type TopologyEntity struct {
	EntityID   string            `json:"entity_id"`
	EntityType ResourceType      `json:"entity_type"`
	EntityInfo map[string]string `json:"entity_info"`
	Paths      []*PathResult     `json:"paths"`
}

// BatchQueryRequest 批量查询请求
type BatchQueryRequest struct {
	QueryList []*HopQueryRequest `json:"query_list"`
}

// BatchQueryResponse 批量查询响应
type BatchQueryResponse struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    *BatchQueryData `json:"data"`
}

// BatchQueryData 批量查询数据
type BatchQueryData struct {
	QueryList []*SingleHopQueryResult `json:"query_list"`
}

// SingleHopQueryResult 单跳查询结果
type SingleHopQueryResult struct {
	QueryIndex int `json:"query_index"`
	*HopQueryResponse
}

// ResourceQueryRequest 资源查询请求
type ResourceQueryRequest struct {
	ResourceType ResourceType      `json:"resource_type"`
	Labels       map[string]string `json:"labels,omitempty"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	Limit        int               `json:"limit,omitempty"`
	Offset       int               `json:"offset,omitempty"`
}

// RelationQueryRequest 关系查询请求
type RelationQueryRequest struct {
	FromType     ResourceType      `json:"from_type,omitempty"`
	ToType       ResourceType      `json:"to_type,omitempty"`
	FromLabels   map[string]string `json:"from_labels,omitempty"`
	ToLabels     map[string]string `json:"to_labels,omitempty"`
	RelationType RelationType      `json:"relation_type,omitempty"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	Depth        int               `json:"depth,omitempty"`
	Limit        int               `json:"limit,omitempty"`
	Offset       int               `json:"offset,omitempty"`
}

// ResourceQueryResponse 资源查询响应
type ResourceQueryResponse struct {
	Resources []*Resource `json:"resources"`
	Total     int64       `json:"total"`
}

// RelationQueryResponse 关系查询响应
type RelationQueryResponse struct {
	Relations []*Relation `json:"relations"`
	Total     int64       `json:"total"`
}

// GraphPath 图路径
type GraphPath struct {
	Nodes     []*Resource `json:"nodes"`
	Relations []*Relation `json:"relations"`
}

// GraphQueryResponse 图查询响应
type GraphQueryResponse struct {
	Paths []*GraphPath `json:"paths"`
	Total int64        `json:"total"`
}

// GenerateResourceID 生成资源ID
// 格式: {resource_type}:⟨key1=value1,key2=value2,...⟩
func GenerateResourceID(resourceType ResourceType, labels map[string]string) string {
	if len(labels) == 0 {
		return string(resourceType) + ":⟨⟩"
	}

	// Sort keys for consistent ID generation
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(labels))
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, labels[k]))
	}

	return fmt.Sprintf("%s:⟨%s⟩", resourceType, strings.Join(pairs, ","))
}

// GenerateRelationID 生成关系ID
// 格式: {relation_table}:⟨from_kv|to_kv⟩
func GenerateRelationID(relationType RelationType, fromID, toID string) string {
	fromKV := extractKVFromResourceID(fromID)
	toKV := extractKVFromResourceID(toID)
	return fmt.Sprintf("%s:⟨%s|%s⟩", relationType, fromKV, toKV)
}

// extractKVFromResourceID 从资源ID中提取键值部分
func extractKVFromResourceID(resourceID string) string {
	parts := strings.SplitN(resourceID, ":⟨", 2)
	if len(parts) != 2 {
		return ""
	}
	return strings.TrimSuffix(parts[1], "⟩")
}

// ParseResourceID 解析资源ID，返回类型和标签
func ParseResourceID(resourceID string) (ResourceType, map[string]string, error) {
	parts := strings.SplitN(resourceID, ":⟨", 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid resource ID format: %s", resourceID)
	}

	resourceType := ResourceType(parts[0])
	kvPart := strings.TrimSuffix(parts[1], "⟩")

	labels := make(map[string]string)
	if kvPart != "" {
		pairs := strings.Split(kvPart, ",")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				labels[kv[0]] = kv[1]
			}
		}
	}

	return resourceType, labels, nil
}

// GetResourcePrimaryKeys 获取资源类型的主键字段
func GetResourcePrimaryKeys(resourceType ResourceType) []string {
	switch resourceType {
	// Kubernetes 资源
	case ResourceTypePod:
		return []string{"bcs_cluster_id", "namespace", "pod"}
	case ResourceTypeNode:
		return []string{"bcs_cluster_id", "node"}
	case ResourceTypeContainer:
		return []string{"bcs_cluster_id", "namespace", "pod", "container"}
	case ResourceTypeDeployment:
		return []string{"bcs_cluster_id", "namespace", "deployment"}
	case ResourceTypeReplicaSet:
		return []string{"bcs_cluster_id", "namespace", "replicaset"}
	case ResourceTypeStatefulSet:
		return []string{"bcs_cluster_id", "namespace", "statefulset"}
	case ResourceTypeDaemonSet:
		return []string{"bcs_cluster_id", "namespace", "daemonset"}
	case ResourceTypeJob:
		return []string{"bcs_cluster_id", "namespace", "job"}
	case ResourceTypeService:
		return []string{"bcs_cluster_id", "namespace", "service"}
	case ResourceTypeIngress:
		return []string{"bcs_cluster_id", "namespace", "ingress"}
	case ResourceTypeCluster:
		return []string{"bcs_cluster_id"}
	case ResourceTypeNamespace:
		return []string{"bcs_cluster_id", "namespace"}

	// 网络资源
	case ResourceTypeSystem:
		return []string{"bk_cloud_id", "bk_target_ip"}
	case ResourceTypeK8sAddress:
		return []string{"bcs_cluster_id", "address"}
	case ResourceTypeDomain:
		return []string{"bcs_cluster_id", "domain"}

	// APM 资源
	case ResourceTypeAPMService:
		return []string{"apm_application_name", "apm_service_name"}
	case ResourceTypeAPMServiceInstance:
		return []string{"apm_application_name", "apm_service_name", "apm_service_instance_name"}

	// 数据源资源
	case ResourceTypeDataSource:
		return []string{"bk_data_id"}
	case ResourceTypeBKLogConfig:
		return []string{"bklogconfig_namespace", "bklogconfig_name"}

	// CMDB 资源
	case ResourceTypeBiz:
		return []string{"bk_biz_id"}
	case ResourceTypeSet:
		return []string{"bk_set_id"}
	case ResourceTypeModule:
		return []string{"bk_module_id"}
	case ResourceTypeHost:
		return []string{"bk_host_id"}

	// 应用版本资源
	case ResourceTypeAppVersion:
		return []string{"app_name", "version"}
	case ResourceTypeGitCommit:
		return []string{"git_repo", "commit_id"}
	case ResourceTypeEnvironment:
		return []string{"environment"}

	default:
		return nil
	}
}

// GetLivenessTableName 获取资源类型的存活记录表名
func GetLivenessTableName(resourceType ResourceType) string {
	return string(resourceType) + "_liveness_record"
}

// GetRelationLivenessTableName 获取关系类型的存活记录表名
func GetRelationLivenessTableName(relationType RelationType) string {
	return string(relationType) + "_liveness_record"
}

// GetStaticRelationsBetween 获取两个资源类型之间的静态关系
func GetStaticRelationsBetween(fromType, toType ResourceType) []RelationType {
	var result []RelationType
	for _, schema := range schemaRegistry {
		if schema.Category != RelationCategoryStatic {
			continue
		}
		// 静态关系是双向的
		if (schema.FromType == fromType && schema.ToType == toType) ||
			(schema.FromType == toType && schema.ToType == fromType) {
			result = append(result, schema.RelationType)
		}
	}
	return result
}

// GetDynamicRelationsBetween 获取两个资源类型之间的动态关系
func GetDynamicRelationsBetween(fromType, toType ResourceType) []RelationType {
	var result []RelationType
	for _, schema := range schemaRegistry {
		if schema.Category != RelationCategoryDynamic {
			continue
		}
		// 动态关系是单向的
		if schema.FromType == fromType && schema.ToType == toType {
			result = append(result, schema.RelationType)
		}
	}
	return result
}

// RelationSchema 关系模式定义
type RelationSchema struct {
	RelationType RelationType
	Category     RelationCategory
	FromType     ResourceType
	ToType       ResourceType
	IsBelongsTo  bool // 是否为归属关系
}

// schemaRegistry 关系模式注册表
// 共 32 种关系（27 静态 + 5 动态）
var schemaRegistry = []RelationSchema{
	// 静态关系

	// Kubernetes 资源关系
	{RelationNodeWithSystem, RelationCategoryStatic, ResourceTypeNode, ResourceTypeSystem, false},
	{RelationNodeWithPod, RelationCategoryStatic, ResourceTypeNode, ResourceTypePod, false},
	{RelationJobWithPod, RelationCategoryStatic, ResourceTypeJob, ResourceTypePod, false},
	{RelationPodWithReplicaSet, RelationCategoryStatic, ResourceTypePod, ResourceTypeReplicaSet, true},
	{RelationPodWithStatefulSet, RelationCategoryStatic, ResourceTypePod, ResourceTypeStatefulSet, true},
	{RelationDaemonSetWithPod, RelationCategoryStatic, ResourceTypeDaemonSet, ResourceTypePod, true},
	{RelationDeploymentWithReplicaSet, RelationCategoryStatic, ResourceTypeDeployment, ResourceTypeReplicaSet, true},
	{RelationPodWithService, RelationCategoryStatic, ResourceTypePod, ResourceTypeService, false},
	{RelationIngressWithService, RelationCategoryStatic, ResourceTypeIngress, ResourceTypeService, false},

	// 网络资源关系
	{RelationK8sAddressWithService, RelationCategoryStatic, ResourceTypeK8sAddress, ResourceTypeService, false},
	{RelationDomainWithService, RelationCategoryStatic, ResourceTypeDomain, ResourceTypeService, false},

	// APM 资源关系
	{RelationAPMServiceInstanceWithPod, RelationCategoryStatic, ResourceTypeAPMServiceInstance, ResourceTypePod, false},
	{RelationAPMServiceInstanceWithSystem, RelationCategoryStatic, ResourceTypeAPMServiceInstance, ResourceTypeSystem, false},
	{RelationAPMServiceWithAPMServiceInstance, RelationCategoryStatic, ResourceTypeAPMService, ResourceTypeAPMServiceInstance, true},

	// 容器关系
	{RelationContainerWithPod, RelationCategoryStatic, ResourceTypeContainer, ResourceTypePod, true},

	// 数据源关系
	{RelationDataSourceWithPod, RelationCategoryStatic, ResourceTypeDataSource, ResourceTypePod, false},
	{RelationDataSourceWithNode, RelationCategoryStatic, ResourceTypeDataSource, ResourceTypeNode, false},
	{RelationBKLogConfigWithDataSource, RelationCategoryStatic, ResourceTypeBKLogConfig, ResourceTypeDataSource, false},

	// CMDB 关系
	{RelationBizWithSet, RelationCategoryStatic, ResourceTypeBiz, ResourceTypeSet, true},
	{RelationModuleWithSet, RelationCategoryStatic, ResourceTypeModule, ResourceTypeSet, true},
	{RelationHostWithModule, RelationCategoryStatic, ResourceTypeHost, ResourceTypeModule, true},
	{RelationHostWithSystem, RelationCategoryStatic, ResourceTypeHost, ResourceTypeSystem, false},

	// 应用版本关系
	{RelationAppVersionWithContainer, RelationCategoryStatic, ResourceTypeAppVersion, ResourceTypeContainer, false},
	{RelationAppVersionWithSystem, RelationCategoryStatic, ResourceTypeAppVersion, ResourceTypeSystem, false},
	{RelationContainerWithEnvironment, RelationCategoryStatic, ResourceTypeContainer, ResourceTypeEnvironment, false},
	{RelationEnvironmentWithSystem, RelationCategoryStatic, ResourceTypeEnvironment, ResourceTypeSystem, false},
	{RelationAppVersionWithGitCommit, RelationCategoryStatic, ResourceTypeAppVersion, ResourceTypeGitCommit, false},

	// 动态关系
	{RelationPodToPod, RelationCategoryDynamic, ResourceTypePod, ResourceTypePod, false},
	{RelationPodToSystem, RelationCategoryDynamic, ResourceTypePod, ResourceTypeSystem, false},
	{RelationSystemToPod, RelationCategoryDynamic, ResourceTypeSystem, ResourceTypePod, false},
	{RelationSystemToSystem, RelationCategoryDynamic, ResourceTypeSystem, ResourceTypeSystem, false},
	{RelationServiceToService, RelationCategoryDynamic, ResourceTypeService, ResourceTypeService, false},
}

// GetAllRelationTypesForResource 获取与指定资源类型相关的所有关系类型
func GetAllRelationTypesForResource(resourceType ResourceType) []RelationType {
	seen := make(map[RelationType]bool)
	var result []RelationType

	for _, schema := range schemaRegistry {
		// Check if this resource type is involved in the relation
		if schema.FromType == resourceType || schema.ToType == resourceType {
			if !seen[schema.RelationType] {
				seen[schema.RelationType] = true
				result = append(result, schema.RelationType)
			}
		}
	}

	return result
}

// GetAllRelationTypes 获取所有已注册的关系类型
func GetAllRelationTypes() []RelationType {
	result := make([]RelationType, len(schemaRegistry))
	for i, schema := range schemaRegistry {
		result[i] = schema.RelationType
	}
	return result
}

// GetRelationSchema 获取指定关系类型的模式定义
func GetRelationSchema(relationType RelationType) *RelationSchema {
	for i := range schemaRegistry {
		if schemaRegistry[i].RelationType == relationType {
			return &schemaRegistry[i]
		}
	}
	return nil
}

// GetRelationsBetweenTypes 获取连接两个资源类型的所有关系类型
func GetRelationsBetweenTypes(fromType, toType ResourceType) []RelationType {
	var result []RelationType

	for _, schema := range schemaRegistry {
		if schema.Category == RelationCategoryStatic {
			// 静态关系检查双向
			if (schema.FromType == fromType && schema.ToType == toType) ||
				(schema.FromType == toType && schema.ToType == fromType) {
				result = append(result, schema.RelationType)
			}
		} else {
			// 动态关系检查单向
			if schema.FromType == fromType && schema.ToType == toType {
				result = append(result, schema.RelationType)
			}
		}
	}

	return result
}

// GetRelationsByCategory 获取指定类别的所有关系类型
func GetRelationsByCategory(category RelationCategory) []RelationType {
	var result []RelationType
	for _, schema := range schemaRegistry {
		if schema.Category == category {
			result = append(result, schema.RelationType)
		}
	}
	return result
}

// RegisterRelationSchema 动态注册新的关系类型
func RegisterRelationSchema(schema RelationSchema) {
	for _, existing := range schemaRegistry {
		if existing.RelationType == schema.RelationType {
			return
		}
	}
	schemaRegistry = append(schemaRegistry, schema)
}

// GetConnectedResourceTypes 获取与指定资源类型相连的所有资源类型
func GetConnectedResourceTypes(resourceType ResourceType) []ResourceType {
	seen := make(map[ResourceType]bool)
	var result []ResourceType

	for _, schema := range schemaRegistry {
		var connectedType ResourceType
		if schema.FromType == resourceType {
			connectedType = schema.ToType
		} else if schema.ToType == resourceType {
			connectedType = schema.FromType
		} else {
			continue
		}

		if !seen[connectedType] {
			seen[connectedType] = true
			result = append(result, connectedType)
		}
	}

	return result
}
