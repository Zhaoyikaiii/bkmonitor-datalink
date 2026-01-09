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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
)

func init() {
	// 初始化测试环境的配置
	mock.Init()
}

func TestNewTraversalConfig(t *testing.T) {
	sourceID := "pod:⟨bcs_cluster_id=cluster-1,namespace=default,pod=nginx⟩"
	sourceType := ResourceTypePod
	validityTime := time.Now().UnixMilli()

	cfg := NewTraversalConfig(sourceID, sourceType, validityTime)

	assert.Equal(t, sourceID, cfg.SourceID)
	assert.Equal(t, sourceType, cfg.SourceType)
	assert.Equal(t, validityTime, cfg.ValidityTime)
	assert.Equal(t, 1, cfg.MaxHops)
	assert.Equal(t, DirectionBoth, cfg.Direction)
	assert.Empty(t, cfg.AllowedCategories)
}

func TestBuildGeneralTraversal(t *testing.T) {
	qb := NewQueryBuilder()
	validityTime := int64(1704067200000)

	tests := []struct {
		name             string
		cfg              *TraversalConfig
		expectEmpty      bool
		mustContain      []string
		mustNotContain   []string
	}{
		{
			name:        "nil config returns empty",
			cfg:         nil,
			expectEmpty: true,
		},
		{
			name:        "unknown resource type returns empty",
			cfg:         NewTraversalConfig("unknown:⟨id=1⟩", ResourceType("unknown"), 0),
			expectEmpty: true,
		},
		{
			name: "pod type discovers all relations",
			cfg:  NewTraversalConfig("pod:⟨bcs_cluster_id=cluster-1,namespace=default,pod=nginx⟩", ResourceTypePod, validityTime),
			mustContain: []string{
				"Dynamic graph traversal query",
				"node_with_pod",
				"container_with_pod",
				"pod_with_service",
				"pod_with_replicaset",
				"pod_to_pod",
				"pod_to_system",
				"updated_at >= 1704067200000",
			},
		},
		{
			name: "node type discovers node relations",
			cfg:  NewTraversalConfig("node:⟨bcs_cluster_id=cluster-1,node=node-1⟩", ResourceTypeNode, validityTime),
			mustContain: []string{
				"node_with_pod",
				"node_with_system",
			},
		},
		{
			name: "service type discovers service relations",
			cfg:  NewTraversalConfig("service:⟨bcs_cluster_id=cluster-1,namespace=default,service=nginx-svc⟩", ResourceTypeService, validityTime),
			mustContain: []string{
				"pod_with_service",
				"service_to_service",
			},
		},
		{
			name: "system type discovers system relations",
			cfg:  NewTraversalConfig("system:⟨bk_target_ip=192.168.1.1,bk_cloud_id=0⟩", ResourceTypeSystem, validityTime),
			mustContain: []string{
				"node_with_system",
				"system_to_pod",
				"system_to_system",
				"pod_to_system",
			},
		},
		{
			name: "static category filter",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.AllowedCategories = []RelationCategory{RelationCategoryStatic}
				return c
			}(),
			mustContain:    []string{"node_with_pod", "container_with_pod"},
			mustNotContain: []string{"pod_to_pod", "pod_to_system"},
		},
		{
			name: "dynamic category filter",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.AllowedCategories = []RelationCategory{RelationCategoryDynamic}
				return c
			}(),
			mustContain:    []string{"pod_to_pod", "pod_to_system"},
			mustNotContain: []string{"node_with_pod", "container_with_pod"},
		},
		{
			name: "specific relation types filter",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.AllowedRelationTypes = []RelationType{RelationNodeWithPod, RelationContainerWithPod}
				return c
			}(),
			mustContain:    []string{"node_with_pod", "container_with_pod"},
			mustNotContain: []string{"pod_with_service", "pod_to_pod"},
		},
		{
			name: "outbound direction",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.Direction = DirectionOutbound
				return c
			}(),
			mustContain: []string{"in = 'pod:⟨test⟩'"},
		},
		{
			name: "inbound direction",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.Direction = DirectionInbound
				return c
			}(),
			mustContain: []string{"out = 'pod:⟨test⟩'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.BuildGeneralTraversal(tt.cfg)

			if tt.expectEmpty {
				assert.Empty(t, result)
				return
			}

			for _, s := range tt.mustContain {
				assert.Contains(t, result, s)
			}
			for _, s := range tt.mustNotContain {
				assert.NotContains(t, result, s)
			}
		})
	}
}

func TestBuildMultiHopTraversal(t *testing.T) {
	qb := NewQueryBuilder()
	validityTime := int64(1704067200000)

	tests := []struct {
		name        string
		cfg         *TraversalConfig
		expectEmpty bool
		mustContain []string
	}{
		{
			name:        "nil config returns empty",
			cfg:         nil,
			expectEmpty: true,
		},
		{
			name: "zero hops returns empty",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("test", ResourceTypePod, 0)
				c.MaxHops = 0
				return c
			}(),
			expectEmpty: true,
		},
		{
			name: "multi-hop with depth",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.MaxHops = 3
				return c
			}(),
			mustContain: []string{
				"Multi-hop graph traversal",
				"DEPTH 1..3",
				"updated_at >= 1704067200000",
			},
		},
		{
			name: "outbound direction uses ->",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.MaxHops = 2
				c.Direction = DirectionOutbound
				return c
			}(),
			mustContain: []string{"->"},
		},
		{
			name: "inbound direction uses <-",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.MaxHops = 2
				c.Direction = DirectionInbound
				return c
			}(),
			mustContain: []string{"<-"},
		},
		{
			name: "both direction uses <->",
			cfg: func() *TraversalConfig {
				c := NewTraversalConfig("pod:⟨test⟩", ResourceTypePod, validityTime)
				c.MaxHops = 2
				c.Direction = DirectionBoth
				return c
			}(),
			mustContain: []string{"<->"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.BuildMultiHopTraversal(tt.cfg)

			if tt.expectEmpty {
				assert.Empty(t, result)
				return
			}

			for _, s := range tt.mustContain {
				assert.Contains(t, result, s)
			}
		})
	}
}

func TestBuildDirectionClause(t *testing.T) {
	qb := NewQueryBuilder()
	sourceID := "test:⟨id=1⟩"

	tests := []struct {
		direction TraversalDirection
		expected  string
	}{
		{DirectionOutbound, "in = 'test:⟨id=1⟩'"},
		{DirectionInbound, "out = 'test:⟨id=1⟩'"},
		{DirectionBoth, "(in = 'test:⟨id=1⟩' OR out = 'test:⟨id=1⟩')"},
		{"", "(in = 'test:⟨id=1⟩' OR out = 'test:⟨id=1⟩')"},
	}

	for _, tt := range tests {
		t.Run(string(tt.direction), func(t *testing.T) {
			result := qb.buildDirectionClause(sourceID, tt.direction)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilterRelationsByCategory(t *testing.T) {
	qb := NewQueryBuilder()
	allRelations := []RelationType{
		RelationNodeWithPod,
		RelationContainerWithPod,
		RelationPodToPod,
		RelationPodToSystem,
	}

	tests := []struct {
		name       string
		categories []RelationCategory
		expected   int
	}{
		{"no filter returns all", nil, 4},
		{"static only", []RelationCategory{RelationCategoryStatic}, 2},
		{"dynamic only", []RelationCategory{RelationCategoryDynamic}, 2},
		{"both categories", []RelationCategory{RelationCategoryStatic, RelationCategoryDynamic}, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.filterRelationsByCategory(allRelations, tt.categories)
			assert.Len(t, result, tt.expected)
		})
	}
}

func TestGetApplicableRelationTypes(t *testing.T) {
	qb := NewQueryBuilder()

	tests := []struct {
		name        string
		cfg         *TraversalConfig
		minExpected int
		exactTypes  []RelationType
	}{
		{
			name: "specific types provided",
			cfg: &TraversalConfig{
				SourceType:           ResourceTypePod,
				AllowedRelationTypes: []RelationType{RelationNodeWithPod},
			},
			exactTypes: []RelationType{RelationNodeWithPod},
		},
		{
			name: "discovery for pod",
			cfg: &TraversalConfig{
				SourceType: ResourceTypePod,
			},
			minExpected: 4,
		},
		{
			name: "discovery for node",
			cfg: &TraversalConfig{
				SourceType: ResourceTypeNode,
			},
			minExpected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.getApplicableRelationTypes(tt.cfg)

			if tt.exactTypes != nil {
				assert.Equal(t, tt.exactTypes, result)
			} else {
				assert.GreaterOrEqual(t, len(result), tt.minExpected)
			}
		})
	}
}

func TestBuildRelationSubquery(t *testing.T) {
	qb := NewQueryBuilder()

	cfg := &TraversalConfig{
		SourceID:     "pod:⟨test⟩",
		ValidityTime: 1704067200000,
		Direction:    DirectionBoth,
	}

	result := qb.buildRelationSubquery(cfg, RelationNodeWithPod)

	assert.Contains(t, result, "node_with_pod")
	assert.Contains(t, result, "relation_type")
	assert.Contains(t, result, "from_id")
	assert.Contains(t, result, "to_id")
	assert.Contains(t, result, "updated_at >= 1704067200000")
}

func TestBuildDynamicTraversalQuery(t *testing.T) {
	qb := NewQueryBuilder()

	cfg := &TraversalConfig{
		SourceID:     "pod:⟨test⟩",
		ValidityTime: 1704067200000,
		Direction:    DirectionBoth,
	}

	tests := []struct {
		name          string
		relationTypes []RelationType
		expectEmpty   bool
		checkUnion    bool
	}{
		{
			name:          "empty relations returns empty",
			relationTypes: nil,
			expectEmpty:   true,
		},
		{
			name:          "single relation no union",
			relationTypes: []RelationType{RelationNodeWithPod},
			checkUnion:    false,
		},
		{
			name:          "multiple relations with union",
			relationTypes: []RelationType{RelationNodeWithPod, RelationContainerWithPod},
			checkUnion:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.buildDynamicTraversalQuery(cfg, tt.relationTypes)

			if tt.expectEmpty {
				assert.Empty(t, result)
				return
			}

			assert.Contains(t, result, "Dynamic graph traversal query")

			if tt.checkUnion {
				assert.Contains(t, result, "UNION ALL")
			} else {
				assert.NotContains(t, result, "UNION ALL")
			}
		})
	}
}

func TestBuildSingleHopQuery(t *testing.T) {
	qb := NewQueryBuilder()
	timestamp := int64(1704067200000)

	tests := []struct {
		name           string
		req            *HopQueryRequest
		mustContain    []string
		mustNotContain []string
	}{
		{
			name: "static relation between node and pod",
			req: &HopQueryRequest{
				Timestamp:            timestamp,
				SourceType:           ResourceTypeNode,
				SourceInfo:           map[string]string{"bcs_cluster_id": "cluster-1", "node": "node-1"},
				TargetType:           ResourceTypePod,
				AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
			},
			mustContain: []string{"node_with_pod"},
		},
		{
			name: "dynamic relation between pods",
			req: &HopQueryRequest{
				Timestamp:            timestamp,
				SourceType:           ResourceTypePod,
				SourceInfo:           map[string]string{"bcs_cluster_id": "cluster-1", "namespace": "default", "pod": "pod-1"},
				TargetType:           ResourceTypePod,
				AllowedRelationTypes: []RelationCategory{RelationCategoryDynamic},
			},
			mustContain: []string{"pod_to_pod"},
		},
		{
			name: "no target type triggers general traversal",
			req: &HopQueryRequest{
				Timestamp:  timestamp,
				SourceType: ResourceTypePod,
				SourceInfo: map[string]string{"bcs_cluster_id": "cluster-1", "namespace": "default", "pod": "pod-1"},
			},
			mustContain: []string{"Dynamic graph traversal query"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.BuildSingleHopQuery(tt.req)

			for _, s := range tt.mustContain {
				assert.Contains(t, result, s)
			}
			for _, s := range tt.mustNotContain {
				assert.NotContains(t, result, s)
			}
		})
	}
}

func TestBuildSourceEntityQuery(t *testing.T) {
	qb := NewQueryBuilder()

	req := &HopQueryRequest{
		SourceType: ResourceTypePod,
		SourceInfo: map[string]string{
			"bcs_cluster_id": "cluster-1",
			"namespace":      "default",
			"pod":            "nginx",
		},
	}

	result := qb.BuildSourceEntityQuery(req)

	assert.True(t, strings.HasPrefix(result, "SELECT * FROM pod WHERE id = '"))
	assert.Contains(t, result, "pod:⟨")
}

func TestBuildLivenessQuery(t *testing.T) {
	qb := NewQueryBuilder()

	result := qb.BuildLivenessQuery(ResourceTypePod, "pod:⟨test⟩", 1000, 2000)

	assert.Contains(t, result, "pod_liveness_record")
	assert.Contains(t, result, "pod_id = 'pod:⟨test⟩'")
	assert.Contains(t, result, "period_start <= 2000")
	assert.Contains(t, result, "period_end >= 1000")
}

func TestBuildResourceQuery(t *testing.T) {
	qb := NewQueryBuilder()
	now := time.Now()

	req := &ResourceQueryRequest{
		ResourceType: ResourceTypePod,
		Labels:       map[string]string{"namespace": "default"},
		StartTime:    now.Add(-time.Hour),
		EndTime:      now,
		Limit:        100,
		Offset:       10,
	}

	result := qb.BuildResourceQuery(req)

	assert.Contains(t, result, "SELECT * FROM pod")
	assert.Contains(t, result, "pod_liveness_record")
	assert.Contains(t, result, "namespace = 'default'")
	assert.Contains(t, result, "LIMIT 100")
	assert.Contains(t, result, "START 10")
}

func TestBuildRelationQuery(t *testing.T) {
	qb := NewQueryBuilder()
	now := time.Now()

	tests := []struct {
		name        string
		req         *RelationQueryRequest
		expectEmpty bool
		mustContain []string
	}{
		{
			name: "empty relation type returns empty",
			req: &RelationQueryRequest{
				StartTime: now.Add(-time.Hour),
				EndTime:   now,
			},
			expectEmpty: true,
		},
		{
			name: "valid relation query",
			req: &RelationQueryRequest{
				RelationType: RelationNodeWithPod,
				StartTime:    now.Add(-time.Hour),
				EndTime:      now,
				Limit:        50,
				Offset:       5,
			},
			mustContain: []string{
				"SELECT * FROM node_with_pod",
				"node_with_pod_liveness_record",
				"LIMIT 50",
				"START 5",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.BuildRelationQuery(tt.req)

			if tt.expectEmpty {
				assert.Empty(t, result)
				return
			}

			for _, s := range tt.mustContain {
				assert.Contains(t, result, s)
			}
		})
	}
}

func TestParseLookBackDelta(t *testing.T) {
	qb := NewQueryBuilder()

	tests := []struct {
		delta    string
		expected int64
	}{
		{"", 10 * 60 * 1000},       // default 10m
		{"5m", 5 * 60 * 1000},
		{"1h", 60 * 60 * 1000},
		{"30s", 30 * 1000},
		{"invalid", 10 * 60 * 1000}, // fallback to default
	}

	for _, tt := range tests {
		t.Run(tt.delta, func(t *testing.T) {
			result := qb.parseLookBackDelta(tt.delta)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetAllowedCategories(t *testing.T) {
	qb := NewQueryBuilder()

	tests := []struct {
		name     string
		input    []RelationCategory
		expected []RelationCategory
	}{
		{
			name:     "empty returns both",
			input:    nil,
			expected: []RelationCategory{RelationCategoryStatic, RelationCategoryDynamic},
		},
		{
			name:     "static only",
			input:    []RelationCategory{RelationCategoryStatic},
			expected: []RelationCategory{RelationCategoryStatic},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.getAllowedCategories(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContainsCategory(t *testing.T) {
	qb := NewQueryBuilder()

	tests := []struct {
		categories []RelationCategory
		target     RelationCategory
		expected   bool
	}{
		{[]RelationCategory{RelationCategoryStatic}, RelationCategoryStatic, true},
		{[]RelationCategory{RelationCategoryStatic}, RelationCategoryDynamic, false},
		{[]RelationCategory{RelationCategoryStatic, RelationCategoryDynamic}, RelationCategoryDynamic, true},
		{nil, RelationCategoryStatic, false},
	}

	for _, tt := range tests {
		result := qb.containsCategory(tt.categories, tt.target)
		assert.Equal(t, tt.expected, result)
	}
}

func TestBuildSourceInfoFilter(t *testing.T) {
	qb := NewQueryBuilder()

	tests := []struct {
		name       string
		sourceInfo map[string]string
		expected   string
	}{
		{"empty returns empty", nil, ""},
		{"empty map returns empty", map[string]string{}, ""},
		{
			"single key",
			map[string]string{"namespace": "default"},
			"namespace = 'default'",
		},
		{
			"multiple keys sorted",
			map[string]string{"namespace": "default", "bcs_cluster_id": "cluster-1"},
			"bcs_cluster_id = 'cluster-1' AND namespace = 'default'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qb.BuildSourceInfoFilter(ResourceTypePod, tt.sourceInfo)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSchemaRegistry(t *testing.T) {
	// Verify total count: 27 static + 5 dynamic = 32
	allRelations := GetAllRelationTypes()
	assert.Len(t, allRelations, 32, "should have 32 total relation types")

	staticRelations := GetRelationsByCategory(RelationCategoryStatic)
	assert.Len(t, staticRelations, 27, "should have 27 static relation types")

	dynamicRelations := GetRelationsByCategory(RelationCategoryDynamic)
	assert.Len(t, dynamicRelations, 5, "should have 5 dynamic relation types")
}

func TestGetStaticRelationsBetween(t *testing.T) {
	tests := []struct {
		name     string
		fromType ResourceType
		toType   ResourceType
		expected []RelationType
	}{
		{"node to pod", ResourceTypeNode, ResourceTypePod, []RelationType{RelationNodeWithPod}},
		{"pod to node (bidirectional)", ResourceTypePod, ResourceTypeNode, []RelationType{RelationNodeWithPod}},
		{"node to system", ResourceTypeNode, ResourceTypeSystem, []RelationType{RelationNodeWithSystem}},
		{"pod to service", ResourceTypePod, ResourceTypeService, []RelationType{RelationPodWithService}},
		{"pod to replicaset", ResourceTypePod, ResourceTypeReplicaSet, []RelationType{RelationPodWithReplicaSet}},
		{"deployment to replicaset", ResourceTypeDeployment, ResourceTypeReplicaSet, []RelationType{RelationDeploymentWithReplicaSet}},
		{"container to pod", ResourceTypeContainer, ResourceTypePod, []RelationType{RelationContainerWithPod}},
		{"ingress to service", ResourceTypeIngress, ResourceTypeService, []RelationType{RelationIngressWithService}},
		{"job to pod", ResourceTypeJob, ResourceTypePod, []RelationType{RelationJobWithPod}},
		{"pod to statefulset", ResourceTypePod, ResourceTypeStatefulSet, []RelationType{RelationPodWithStatefulSet}},
		{"daemonset to pod", ResourceTypeDaemonSet, ResourceTypePod, []RelationType{RelationDaemonSetWithPod}},
		{"apm_service_instance to pod", ResourceTypeAPMServiceInstance, ResourceTypePod, []RelationType{RelationAPMServiceInstanceWithPod}},
		{"host to module", ResourceTypeHost, ResourceTypeModule, []RelationType{RelationHostWithModule}},
		{"biz to set", ResourceTypeBiz, ResourceTypeSet, []RelationType{RelationBizWithSet}},
		{"no relation", ResourceTypePod, ResourceTypeBiz, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetStaticRelationsBetween(tt.fromType, tt.toType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetDynamicRelationsBetween(t *testing.T) {
	tests := []struct {
		name     string
		fromType ResourceType
		toType   ResourceType
		expected []RelationType
	}{
		{"pod to pod", ResourceTypePod, ResourceTypePod, []RelationType{RelationPodToPod}},
		{"pod to system", ResourceTypePod, ResourceTypeSystem, []RelationType{RelationPodToSystem}},
		{"system to pod", ResourceTypeSystem, ResourceTypePod, []RelationType{RelationSystemToPod}},
		{"system to system", ResourceTypeSystem, ResourceTypeSystem, []RelationType{RelationSystemToSystem}},
		{"service to service", ResourceTypeService, ResourceTypeService, []RelationType{RelationServiceToService}},
		{"no dynamic relation", ResourceTypeNode, ResourceTypePod, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetDynamicRelationsBetween(tt.fromType, tt.toType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetAllRelationTypesForResource(t *testing.T) {
	tests := []struct {
		name        string
		resourceType ResourceType
		minExpected int
	}{
		{"pod has many relations", ResourceTypePod, 10},
		{"node has relations", ResourceTypeNode, 3},
		{"service has relations", ResourceTypeService, 5},
		{"system has many relations", ResourceTypeSystem, 7},
		{"container has relations", ResourceTypeContainer, 3},
		{"deployment has relations", ResourceTypeDeployment, 1},
		{"replicaset has relations", ResourceTypeReplicaSet, 2},
		{"host has relations", ResourceTypeHost, 2},
		{"apm_service has relations", ResourceTypeAPMService, 1},
		{"unknown has no relations", ResourceType("unknown"), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetAllRelationTypesForResource(tt.resourceType)
			assert.GreaterOrEqual(t, len(result), tt.minExpected)
		})
	}
}

func TestGetResourcePrimaryKeys(t *testing.T) {
	tests := []struct {
		resourceType ResourceType
		expected     []string
	}{
		{ResourceTypePod, []string{"bcs_cluster_id", "namespace", "pod"}},
		{ResourceTypeNode, []string{"bcs_cluster_id", "node"}},
		{ResourceTypeContainer, []string{"bcs_cluster_id", "namespace", "pod", "container"}},
		{ResourceTypeSystem, []string{"bk_cloud_id", "bk_target_ip"}},
		{ResourceTypeAPMService, []string{"apm_application_name", "apm_service_name"}},
		{ResourceTypeHost, []string{"bk_host_id"}},
		{ResourceTypeAppVersion, []string{"app_name", "version"}},
		{ResourceType("unknown"), nil},
	}

	for _, tt := range tests {
		t.Run(string(tt.resourceType), func(t *testing.T) {
			result := GetResourcePrimaryKeys(tt.resourceType)
			assert.Equal(t, tt.expected, result)
		})
	}
}
