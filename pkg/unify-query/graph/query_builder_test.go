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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuildRelatedResourcesQuery(t *testing.T) {
	testCases := []struct {
		name         string
		relationType RelationType
		resourceID   string
		direction    TraversalDirection
		startTime    int64
		endTime      int64
		tolerance    time.Duration
		maxLimit     int
		expected     string
	}{
		{
			// 场景：从 Pod 出发，查找 NodeWithPod 关系
			// Schema: Node(FromType) -> Pod(ToType)
			// Pod 是 ToType，所以 direction = Inbound
			// Inbound: 用 out 字段匹配当前资源，返回 in 字段作为目标
			name:         "inbound - pod finds related nodes via node_with_pod",
			relationType: RelationNodeWithPod,
			resourceID:   "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx⟩",
			direction:    DirectionInbound,
			startTime:    1736424000000,
			endTime:      1736510400000,
			tolerance:    time.Minute,
			maxLimit:     1000,
			expected: `SELECT 
    id AS relation_id,
    out AS from_id,
    in AS to_id,
    period_start,
    period_end,
    is_active
FROM node_with_pod 
WHERE out = 'pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx⟩' 
AND period_start <= 1736510460000 
AND period_end >= 1736423940000
ORDER BY period_start ASC
LIMIT 1000`,
		},
		{
			// 场景：从 Node 出发，查找 NodeWithPod 关系
			// Schema: Node(FromType) -> Pod(ToType)
			// Node 是 FromType，所以 direction = Outbound
			// Outbound: 用 in 字段匹配当前资源，返回 out 字段作为目标
			name:         "outbound - node finds related pods via node_with_pod",
			relationType: RelationNodeWithPod,
			resourceID:   "node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩",
			direction:    DirectionOutbound,
			startTime:    1736424000000,
			endTime:      1736510400000,
			tolerance:    time.Minute,
			maxLimit:     1000,
			expected: `SELECT 
    id AS relation_id,
    in AS from_id,
    out AS to_id,
    period_start,
    period_end,
    is_active
FROM node_with_pod 
WHERE in = 'node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩' 
AND period_start <= 1736510460000 
AND period_end >= 1736423940000
ORDER BY period_start ASC
LIMIT 1000`,
		},
		{
			// 场景：动态关系 - Pod 调用 Pod (Outbound)
			// Schema: Pod(FromType) -> Pod(ToType)
			// 查询"当前 Pod 调用了哪些 Pod"
			name:         "dynamic outbound - pod calls which pods",
			relationType: RelationPodToPod,
			resourceID:   "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=caller⟩",
			direction:    DirectionOutbound,
			startTime:    1736424000000,
			endTime:      1736510400000,
			tolerance:    time.Minute,
			maxLimit:     1000,
			expected: `SELECT 
    id AS relation_id,
    in AS from_id,
    out AS to_id,
    period_start,
    period_end,
    is_active
FROM pod_to_pod 
WHERE in = 'pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=caller⟩' 
AND period_start <= 1736510460000 
AND period_end >= 1736423940000
ORDER BY period_start ASC
LIMIT 1000`,
		},
		{
			// 场景：动态关系 - Pod 调用 Pod (Inbound)
			// Schema: Pod(FromType) -> Pod(ToType)
			// 查询"哪些 Pod 调用了当前 Pod"
			name:         "dynamic inbound - which pods call this pod",
			relationType: RelationPodToPod,
			resourceID:   "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=callee⟩",
			direction:    DirectionInbound,
			startTime:    1736424000000,
			endTime:      1736510400000,
			tolerance:    time.Minute,
			maxLimit:     1000,
			expected: `SELECT 
    id AS relation_id,
    out AS from_id,
    in AS to_id,
    period_start,
    period_end,
    is_active
FROM pod_to_pod 
WHERE out = 'pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=callee⟩' 
AND period_start <= 1736510460000 
AND period_end >= 1736423940000
ORDER BY period_start ASC
LIMIT 1000`,
		},
		{
			// 测试 SQL 注入防护
			name:         "sql injection prevention",
			relationType: RelationNodeWithPod,
			resourceID:   "pod:⟨test'; DROP TABLE pods; --⟩",
			direction:    DirectionInbound,
			startTime:    1736424000000,
			endTime:      1736510400000,
			tolerance:    time.Minute,
			maxLimit:     1000,
			expected: `SELECT 
    id AS relation_id,
    out AS from_id,
    in AS to_id,
    period_start,
    period_end,
    is_active
FROM node_with_pod 
WHERE out = 'pod:⟨test\'; DROP TABLE pods; --⟩' 
AND period_start <= 1736510460000 
AND period_end >= 1736423940000
ORDER BY period_start ASC
LIMIT 1000`,
		},
		{
			// 测试无 limit
			name:         "no limit",
			relationType: RelationNodeWithPod,
			resourceID:   "pod:⟨test⟩",
			direction:    DirectionInbound,
			startTime:    1736424000000,
			endTime:      1736510400000,
			tolerance:    time.Minute,
			maxLimit:     0, // 禁用 limit
			expected: `SELECT 
    id AS relation_id,
    out AS from_id,
    in AS to_id,
    period_start,
    period_end,
    is_active
FROM node_with_pod 
WHERE out = 'pod:⟨test⟩' 
AND period_start <= 1736510460000 
AND period_end >= 1736423940000
ORDER BY period_start ASC`,
		},
		{
			// 测试零 tolerance
			name:         "zero tolerance",
			relationType: RelationNodeWithPod,
			resourceID:   "node:⟨test-node⟩",
			direction:    DirectionOutbound,
			startTime:    1000,
			endTime:      2000,
			tolerance:    0,
			maxLimit:     0,
			expected: `SELECT 
    id AS relation_id,
    in AS from_id,
    out AS to_id,
    period_start,
    period_end,
    is_active
FROM node_with_pod 
WHERE in = 'node:⟨test-node⟩' 
AND period_start <= 2000 
AND period_end >= 1000
ORDER BY period_start ASC`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder()
			builder.SetTolerance(tc.tolerance)
			builder.SetMaxLimit(tc.maxLimit)

			query := builder.BuildRelatedResourcesQuery(
				tc.relationType,
				tc.resourceID,
				tc.direction,
				tc.startTime,
				tc.endTime,
			)

			assert.Equal(t, tc.expected, query)
		})
	}
}
