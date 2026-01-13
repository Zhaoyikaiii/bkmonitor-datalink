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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/graph/mock"
	unifyMock "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
)

func init() {
	unifyMock.Init()
}

// newMockInstance 创建使用 mock SurrealDB 的测试 Instance
func newMockInstance(ctx context.Context) (*Instance, error) {
	mock.RegisterHandler()

	cfg := &ClientConfig{
		Type:      ClientTypeNative,
		Address:   mock.SurrealDBUrl,
		Namespace: mock.DefaultNamespace,
		Database:  mock.DefaultDatabase,
	}

	return NewInstance(ctx, cfg)
}

// TestBuildLivenessGraph 使用表驱动测试 BuildLivenessGraph 的各种场景
func TestBuildLivenessGraph(t *testing.T) {
	ctx := context.Background()

	testCases := map[string]struct {
		// mockData 自定义的 mock 数据，为空则使用 mock.MockData
		mockData map[string]any
		// req 可以是 *QueryRequest 或 JSON 字符串
		req any
		// expected 预期结果
		expectedNodes int
		expectedEdges int
		// validate 自定义验证函数
		validate func(t *testing.T, graph *LivenessGraph)
	}{
		"single hop pod to node": {
			mockData: nil, // 使用默认 MockData
			req: `{
				"timestamp": 1736510400000,
				"source_type": "pod",
				"source_info": {
					"bcs_cluster_id": "BCS-K8S-00001",
					"namespace": "default",
					"pod": "nginx-pod-1"
				},
				"target_type": "node",
				"max_hops": 3,
				"look_back_delta": 86400000,
				"allowed_relation_types": ["static"]
			}`,
			expectedNodes: 2,
			expectedEdges: 1,
			validate: func(t *testing.T, graph *LivenessGraph) {
				podID := "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩"
				nodeID := "node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩"
				assert.NotNil(t, graph.GetNode(podID), "Pod 节点应存在")
				assert.NotNil(t, graph.GetNode(nodeID), "Node 节点应存在")
			},
		},
		"root node not exist": {
			mockData: nil, // 使用默认 MockData，non-existent-pod 会返回空结果
			req: `{
				"timestamp": 1736510400000,
				"source_type": "pod",
				"source_info": {
					"bcs_cluster_id": "BCS-K8S-00001",
					"namespace": "default",
					"pod": "non-existent-pod"
				},
				"target_type": "node",
				"max_hops": 3,
				"look_back_delta": 86400000,
				"allowed_relation_types": ["static"]
			}`,
			expectedNodes: 0,
			expectedEdges: 0,
			validate: func(t *testing.T, graph *LivenessGraph) {
				assert.Empty(t, graph.Nodes, "根节点不存在时应返回空图")
				assert.Empty(t, graph.Edges, "根节点不存在时应返回空图")
			},
		},
		"full graph traversal with 7 days lookback": {
			mockData: map[string]any{
				// INFO FOR DB
				"USE NS default DB test; INFO FOR DB": mock.InfoForDBResponse,

				// 查询 Pod liveness (7天, tolerance=600s)
				"USE NS default DB test; SELECT * FROM pod_liveness_record \nWHERE pod_id = 'pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩' \nAND period_start <= 1736511000000 \nAND period_end >= 1735905000000\nORDER BY period_start ASC\nLIMIT 1000": `[{"status":"OK","result":null},{"status":"OK","result":[{"id":"pod_liveness_record:BCS-K8S-00002:bkop:pod-0","pod_id":"pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩","period_start":1735905600000,"period_end":1736510400000}]}]`,

				// 查询 Pod 的关联关系 (node_with_pod)
				"USE NS default DB test; SELECT \n    id AS relation_id,\n    out AS from_id,\n    in AS to_id,\n    period_start,\n    period_end,\n    is_active\nFROM node_with_pod \nWHERE out = 'pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩' \nAND period_start <= 1736511000000 \nAND period_end >= 1735905000000\nORDER BY period_start ASC\nLIMIT 1000": `[{"status":"OK","result":null},{"status":"OK","result":[{"relation_id":"node_with_pod:1","from_id":"pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩","to_id":"node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩","period_start":1735905600000,"period_end":1736510400000,"is_active":true}]}]`,

				// 查询 Node liveness
				"USE NS default DB test; SELECT * FROM node_liveness_record \nWHERE node_id = 'node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩' \nAND period_start <= 1736511000000 \nAND period_end >= 1735905000000\nORDER BY period_start ASC\nLIMIT 1000": `[{"status":"OK","result":null},{"status":"OK","result":[{"id":"node_liveness_record:BCS-K8S-00002:node-0","node_id":"node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩","period_start":1735905600000,"period_end":1736510400000}]}]`,
			},
			req: `{
				"timestamp": 1736510400000,
				"source_type": "pod",
				"source_info": {
					"bcs_cluster_id": "BCS-K8S-00002",
					"namespace": "bkop",
					"pod": "pod-0"
				},
				"target_type": "node",
				"max_hops": 3,
				"look_back_delta": 604800000,
				"allowed_relation_types": ["static"]
			}`,
			expectedNodes: 2,
			expectedEdges: 1,
			validate: func(t *testing.T, graph *LivenessGraph) {
				podID := "pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩"
				nodeID := "node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩"
				assert.NotNil(t, graph.GetNode(podID), "Pod 节点应存在")
				assert.NotNil(t, graph.GetNode(nodeID), "Node 节点应存在")

				// 验证边的一致性
				for _, edge := range graph.Edges {
					assert.NotNil(t, graph.GetNode(edge.FromID), "边的源节点应存在")
					assert.NotNil(t, graph.GetNode(edge.ToID), "边的目标节点应存在")
				}
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 设置 mock 数据
			mock.SurrealDB.Clear()
			if tc.mockData != nil {
				mock.SurrealDB.Set(tc.mockData)
			} else {
				mock.SurrealDB.Set(mock.MockData)
			}
			defer mock.SurrealDB.Clear()

			// 创建测试 Instance
			instance, err := newMockInstance(ctx)
			require.NoError(t, err)
			defer instance.Close()

			// 解析请求
			var req *QueryRequest
			switch v := tc.req.(type) {
			case string:
				req = &QueryRequest{}
				err = json.Unmarshal([]byte(v), req)
				require.NoError(t, err, "解析 JSON 请求失败")
			case *QueryRequest:
				req = v
			default:
				t.Fatalf("不支持的请求类型: %T", tc.req)
			}

			// 执行 BuildLivenessGraph
			graph, err := instance.BuildLivenessGraph(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, graph)

			// 验证节点和边数量
			assert.Equal(t, tc.expectedNodes, len(graph.Nodes), "节点数量不匹配")
			assert.Equal(t, tc.expectedEdges, len(graph.Edges), "边数量不匹配")

			// 执行自定义验证
			if tc.validate != nil {
				tc.validate(t, graph)
			}

			// 输出图信息
			graphJSON, _ := json.MarshalIndent(graph, "", "  ")
			t.Logf("LivenessGraph:\n%s", string(graphJSON))
		})
	}
}

// TestBuildLivenessGraph_WithMock_TimeWindowNarrowing
// 测试多层遍历时时间段正确收窄
// 注意：此测试使用与 SingleHopPodToNode 相同的 MockData，主要验证时间段收窄逻辑
func TestBuildLivenessGraph_WithMock_TimeWindowNarrowing(t *testing.T) {
	ctx := context.Background()

	// 测试时间参数 - 使用与 MockData 一致的时间
	queryEnd := int64(1736510400000)
	queryStart := int64(1736424000000)

	// 资源 ID
	podID := "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩"

	// 使用预定义的 MockData
	mock.SurrealDB.Clear()
	mock.SurrealDB.Set(mock.MockData)
	defer mock.SurrealDB.Clear()

	// 创建测试 Instance
	instance, err := newMockInstance(ctx)
	require.NoError(t, err)
	defer instance.Close()

	// 构建查询请求
	req := &QueryRequest{
		Timestamp:  queryEnd,
		SourceType: ResourceTypePod,
		SourceInfo: map[string]string{
			"bcs_cluster_id": "BCS-K8S-00001",
			"namespace":      "default",
			"pod":            "nginx-pod-1",
		},
		TargetType:           ResourceTypeNode,
		MaxHops:              3,
		LookBackDelta:        24 * 60 * 60 * 1000,
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	t.Logf("场景: 时间段收窄测试")
	t.Logf("查询时间范围: [%d, %d]", queryStart, queryEnd)

	// 执行 BuildLivenessGraph
	graph, err := instance.BuildLivenessGraph(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, graph)

	t.Logf("节点数量: %d, 边数量: %d", len(graph.Nodes), len(graph.Edges))

	// 验证 Pod 节点的有效时间段
	podNode := graph.GetNode(podID)
	require.NotNil(t, podNode, "Pod 节点应存在")
	t.Logf("Pod 有效时间段数量: %d", len(podNode.EffectivePeriods))
	for i, p := range podNode.EffectivePeriods {
		t.Logf("  时间段[%d]: [%d, %d]", i, p.Start, p.End)
	}

	// 验证 Node 节点存在
	nodeNode := graph.GetNode(nodeID)
	assert.NotNil(t, nodeNode, "Node 节点应存在")

	// 验证有效时间段被正确设置
	assert.NotEmpty(t, podNode.EffectivePeriods, "Pod 应有有效时间段")

	// 输出图信息
	graphJSON, _ := json.MarshalIndent(graph, "", "  ")
	t.Logf("LivenessGraph:\n%s", string(graphJSON))
}
