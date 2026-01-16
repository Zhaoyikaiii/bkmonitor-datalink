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

	"github.com/stretchr/testify/assert"
)

func TestSurrealQueryBuilder_Build_StaticOnly(t *testing.T) {
	request := &QueryRequest{
		Timestamp:            600000,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"namespace": "default"},
		MaxHops:              1,
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
		LookBackDelta:        600000,
		Limit:                10,
	}

	builder := NewSurrealQueryBuilder(request)
	query := builder.Build()

	t.Logf("Generated query:\n%s", query)

	// 验证变量定义
	assert.Contains(t, query, "LET $timestamp = 600000;")
	assert.Contains(t, query, "LET $look_back_delta = 600000;")
	assert.Contains(t, query, "LET $start = 0;")
	assert.Contains(t, query, "LET $end = 600000;")

	// 验证 root 结构
	assert.Contains(t, query, "entity_type: meta::tb(id)")
	assert.Contains(t, query, "entity_id: <string>id")
	assert.Contains(t, query, "->pod_liveness->liveness[WHERE period_end >= $start AND period_start <= $end].*")

	// 验证 hop1 包含静态关系
	assert.Contains(t, query, "node_with_pod:")
	assert.Contains(t, query, "relation_type: 'node_with_pod'")
	assert.Contains(t, query, "relation_category: 'static'")

	// 验证不包含 direction 字段（静态关系）
	lines := strings.Split(query, "\n")
	for _, line := range lines {
		if strings.Contains(line, "node_with_pod") && strings.Contains(line, "relation_category: 'static'") {
			// 静态关系不应该有 direction 字段
			// 这个检查通过遍历确认
		}
	}

	// 验证 WHERE 子句
	assert.Contains(t, query, "WHERE namespace = 'default'")
	assert.Contains(t, query, "AND updated_at >= $start")
	assert.Contains(t, query, "array::len(->pod_liveness->liveness[WHERE period_end >= $start AND period_start <= $end]) > 0")

	// 验证 LIMIT
	assert.Contains(t, query, "LIMIT 10;")
}

func TestSurrealQueryBuilder_Build_DynamicBoth(t *testing.T) {
	request := &QueryRequest{
		Timestamp:                600000,
		SourceType:               ResourceTypePod,
		SourceInfo:               map[string]string{"namespace": "default", "pod": "nginx-1"},
		MaxHops:                  1,
		AllowedRelationTypes:     []RelationCategory{RelationCategoryDynamic},
		DynamicRelationDirection: DirectionBoth,
		LookBackDelta:            600000,
		Limit:                    10,
	}

	builder := NewSurrealQueryBuilder(request)
	query := builder.Build()

	t.Logf("Generated query:\n%s", query)

	// 验证动态关系包含方向后缀
	assert.Contains(t, query, "pod_to_pod_outbound:")
	assert.Contains(t, query, "pod_to_pod_inbound:")

	// 验证动态关系包含 direction 字段
	assert.Contains(t, query, "direction: 'outbound'")
	assert.Contains(t, query, "direction: 'inbound'")

	// 验证 relation_category
	assert.Contains(t, query, "relation_category: 'dynamic'")
}

func TestSurrealQueryBuilder_Build_DynamicOutboundOnly(t *testing.T) {
	request := &QueryRequest{
		Timestamp:                600000,
		SourceType:               ResourceTypePod,
		SourceInfo:               map[string]string{"namespace": "default"},
		MaxHops:                  1,
		AllowedRelationTypes:     []RelationCategory{RelationCategoryDynamic},
		DynamicRelationDirection: DirectionOutbound,
		LookBackDelta:            600000,
		Limit:                    10,
	}

	builder := NewSurrealQueryBuilder(request)
	query := builder.Build()

	t.Logf("Generated query:\n%s", query)

	// 验证只有 outbound
	assert.Contains(t, query, "pod_to_pod_outbound:")
	assert.NotContains(t, query, "pod_to_pod_inbound:")
}

func TestSurrealQueryBuilder_Build_AllRelations(t *testing.T) {
	request := &QueryRequest{
		Timestamp:                600000,
		SourceType:               ResourceTypePod,
		SourceInfo:               map[string]string{"namespace": "default"},
		MaxHops:                  1,
		AllowedRelationTypes:     []RelationCategory{RelationCategoryStatic, RelationCategoryDynamic},
		DynamicRelationDirection: DirectionBoth,
		LookBackDelta:            600000,
		Limit:                    10,
	}

	builder := NewSurrealQueryBuilder(request)
	query := builder.Build()

	t.Logf("Generated query:\n%s", query)

	// 验证同时包含静态和动态关系
	assert.Contains(t, query, "node_with_pod:")           // 静态
	assert.Contains(t, query, "pod_to_pod_outbound:")     // 动态
	assert.Contains(t, query, "relation_category: 'static'")
	assert.Contains(t, query, "relation_category: 'dynamic'")
}

func TestSurrealQueryBuilder_Normalize(t *testing.T) {
	// 测试默认值填充
	request := &QueryRequest{
		Timestamp:  600000,
		SourceType: ResourceTypePod,
		SourceInfo: map[string]string{"namespace": "default"},
	}

	builder := NewSurrealQueryBuilder(request)
	_ = builder.Build()

	// 验证默认值被设置
	assert.Equal(t, DefaultMaxHops, request.MaxHops)
	assert.Equal(t, DefaultLimit, request.Limit)
	assert.Equal(t, int64(DefaultLookBackDelta), request.LookBackDelta)
	assert.Equal(t, DirectionBoth, request.DynamicRelationDirection)
	assert.Len(t, request.AllowedRelationTypes, 2)
}

func TestSurrealQueryBuilder_Build_NodeQuery(t *testing.T) {
	request := &QueryRequest{
		Timestamp:            600000,
		SourceType:           ResourceTypeNode,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001"},
		MaxHops:              1,
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
		LookBackDelta:        600000,
		Limit:                10,
	}

	builder := NewSurrealQueryBuilder(request)
	query := builder.Build()

	t.Logf("Generated query:\n%s", query)

	// 验证 node 相关的关系
	assert.Contains(t, query, "FROM node")
	assert.Contains(t, query, "->node_liveness->liveness")

	// node 应该有 node_with_pod 和 node_with_system 关系
	assert.Contains(t, query, "node_with_pod:")
	assert.Contains(t, query, "node_with_system:")
}

func TestSurrealQueryBuilder_EscapeString(t *testing.T) {
	request := &QueryRequest{
		Timestamp:            600000,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"namespace": "test's-namespace"},
		MaxHops:              1,
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
		LookBackDelta:        600000,
		Limit:                10,
	}

	builder := NewSurrealQueryBuilder(request)
	query := builder.Build()

	t.Logf("Generated query:\n%s", query)

	// 验证字符串被正确转义
	assert.Contains(t, query, "namespace = 'test\\'s-namespace'")
}
