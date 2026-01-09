// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package graph

// ========================================
// 关系模式定义
// ========================================

// RelationSchema 关系模式定义
type RelationSchema struct {
	RelationType RelationType
	Category     RelationCategory
	FromType     ResourceType
	ToType       ResourceType
	IsBelongsTo  bool // 是否为归属关系
}

// schemaRegistry 关系模式注册表
var schemaRegistry = []RelationSchema{
	// 静态关系 - Kubernetes
	{RelationNodeWithSystem, RelationCategoryStatic, ResourceTypeNode, ResourceTypeSystem, false},
	{RelationNodeWithPod, RelationCategoryStatic, ResourceTypeNode, ResourceTypePod, false},
	{RelationJobWithPod, RelationCategoryStatic, ResourceTypeJob, ResourceTypePod, false},
	{RelationPodWithReplicaSet, RelationCategoryStatic, ResourceTypePod, ResourceTypeReplicaSet, true},
	{RelationPodWithStatefulSet, RelationCategoryStatic, ResourceTypePod, ResourceTypeStatefulSet, true},
	{RelationDaemonSetWithPod, RelationCategoryStatic, ResourceTypeDaemonSet, ResourceTypePod, true},
	{RelationDeploymentWithReplicaSet, RelationCategoryStatic, ResourceTypeDeployment, ResourceTypeReplicaSet, true},
	{RelationPodWithService, RelationCategoryStatic, ResourceTypePod, ResourceTypeService, false},
	{RelationIngressWithService, RelationCategoryStatic, ResourceTypeIngress, ResourceTypeService, false},
	{RelationK8sAddressWithService, RelationCategoryStatic, ResourceTypeK8sAddress, ResourceTypeService, false},
	{RelationDomainWithService, RelationCategoryStatic, ResourceTypeDomain, ResourceTypeService, false},
	{RelationAPMServiceInstanceWithPod, RelationCategoryStatic, ResourceTypeAPMServiceInstance, ResourceTypePod, false},
	{RelationAPMServiceInstanceWithSystem, RelationCategoryStatic, ResourceTypeAPMServiceInstance, ResourceTypeSystem, false},
	{RelationAPMServiceWithAPMServiceInstance, RelationCategoryStatic, ResourceTypeAPMService, ResourceTypeAPMServiceInstance, true},
	{RelationContainerWithPod, RelationCategoryStatic, ResourceTypeContainer, ResourceTypePod, true},
	{RelationDataSourceWithPod, RelationCategoryStatic, ResourceTypeDataSource, ResourceTypePod, false},
	{RelationDataSourceWithNode, RelationCategoryStatic, ResourceTypeDataSource, ResourceTypeNode, false},
	{RelationBKLogConfigWithDataSource, RelationCategoryStatic, ResourceTypeBKLogConfig, ResourceTypeDataSource, false},
	{RelationBizWithSet, RelationCategoryStatic, ResourceTypeBiz, ResourceTypeSet, true},
	{RelationModuleWithSet, RelationCategoryStatic, ResourceTypeModule, ResourceTypeSet, true},
	{RelationHostWithModule, RelationCategoryStatic, ResourceTypeHost, ResourceTypeModule, true},
	{RelationHostWithSystem, RelationCategoryStatic, ResourceTypeHost, ResourceTypeSystem, false},
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

// GetRelationSchema 获取指定关系类型的模式定义
func GetRelationSchema(relationType RelationType) *RelationSchema {
	for i := range schemaRegistry {
		if schemaRegistry[i].RelationType == relationType {
			return &schemaRegistry[i]
		}
	}
	return nil
}

// GetAllRelationTypesForResource 获取与指定资源类型相关的所有关系类型
func GetAllRelationTypesForResource(resourceType ResourceType) []RelationType {
	seen := make(map[RelationType]bool)
	var result []RelationType

	for _, schema := range schemaRegistry {
		if schema.FromType == resourceType || schema.ToType == resourceType {
			if !seen[schema.RelationType] {
				seen[schema.RelationType] = true
				result = append(result, schema.RelationType)
			}
		}
	}

	return result
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
