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
)

// ========================================
// 资源类型定义
// ========================================

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

// ========================================
// 关系类型定义
// ========================================

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

	// 动态关系（单向）
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

// ========================================
// 数据库字段常量
// ========================================

const (
	FieldID          = "id"
	FieldIn          = "in"  // 关系源端
	FieldOut         = "out" // 关系目标端
	FieldCreatedAt   = "created_at"
	FieldUpdatedAt   = "updated_at"
	FieldPeriodStart = "period_start"
	FieldPeriodEnd   = "period_end"
	FieldIsActive    = "is_active"
)

// ========================================
// 核心数据结构
// ========================================

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

// Overlap 计算两个 VisiblePeriod 的交集，如果无交集返回 nil
func (p *VisiblePeriod) Overlap(other *VisiblePeriod) *VisiblePeriod {
	start := p.Start
	if other.Start > start {
		start = other.Start
	}
	end := p.End
	if other.End < end {
		end = other.End
	}
	if start > end {
		return nil
	}
	return &VisiblePeriod{Start: start, End: end}
}

// ========================================
// ID 生成与解析
// ========================================

// GenerateResourceID 生成资源ID
// 格式: {resource_type}:⟨key1=value1,key2=value2,...⟩
func GenerateResourceID(resourceType ResourceType, labels map[string]string) string {
	if len(labels) == 0 {
		return string(resourceType) + ":⟨⟩"
	}

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

// GetLivenessTableName 获取资源类型的存活记录表名
func GetLivenessTableName(resourceType ResourceType) string {
	return string(resourceType) + "_liveness_record"
}

// GetRelationLivenessTableName 获取关系类型的存活记录表名
func GetRelationLivenessTableName(relationType RelationType) string {
	return string(relationType) + "_liveness_record"
}

// GetResourcePrimaryKeys 获取资源类型的主键字段
func GetResourcePrimaryKeys(resourceType ResourceType) []string {
	switch resourceType {
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
	case ResourceTypeSystem:
		return []string{"bk_cloud_id", "bk_target_ip"}
	case ResourceTypeK8sAddress:
		return []string{"bcs_cluster_id", "address"}
	case ResourceTypeDomain:
		return []string{"bcs_cluster_id", "domain"}
	case ResourceTypeAPMService:
		return []string{"apm_application_name", "apm_service_name"}
	case ResourceTypeAPMServiceInstance:
		return []string{"apm_application_name", "apm_service_name", "apm_service_instance_name"}
	case ResourceTypeDataSource:
		return []string{"bk_data_id"}
	case ResourceTypeBKLogConfig:
		return []string{"bklogconfig_namespace", "bklogconfig_name"}
	case ResourceTypeBiz:
		return []string{"bk_biz_id"}
	case ResourceTypeSet:
		return []string{"bk_set_id"}
	case ResourceTypeModule:
		return []string{"bk_module_id"}
	case ResourceTypeHost:
		return []string{"bk_host_id"}
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
