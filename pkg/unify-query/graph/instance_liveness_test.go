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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
)

func init() {
	mock.Init()
}

// TestBuildLivenessGraph_根节点不可见_返回空图 测试根节点在查询时间范围内不可见时返回空图
func TestBuildLivenessGraph_根节点不可见_返回空图(t *testing.T) {
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			// 返回空结果，表示根节点在查询时间范围内不可见
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:     time.Now().UnixMilli(),
		SourceType:    ResourceTypePod,
		SourceInfo:    map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:    ResourceTypeNode,
		LookBackDelta: time.Hour.Milliseconds(),
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	assert.NotNil(t, graph)
	assert.Empty(t, graph.Nodes, "根节点不可见时应返回空图")
	assert.Empty(t, graph.Edges)
	assert.True(t, graph.IsComplete(), "空图应标记为完整")
}

// TestBuildLivenessGraph_单层遍历_Pod到Node 测试单层 BFS 遍历场景
func TestBuildLivenessGraph_单层遍历_Pod到Node(t *testing.T) {
	baseTime := time.Date(2026, 1, 13, 12, 0, 0, 0, time.UTC)
	queryStart := baseTime.Add(-1 * time.Hour).UnixMilli()
	queryEnd := baseTime.UnixMilli()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩"

	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			// 根据查询内容返回不同的 mock 数据
			if strings.Contains(query, "pod_liveness_record") {
				// Pod liveness 查询
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(queryEnd),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_liveness_record") {
				// Node liveness 查询
				return []any{
					map[string]any{
						"id":           "node-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(queryEnd),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				// 关系查询
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        nodeID,
						"period_start": float64(queryStart),
						"period_end":   float64(queryEnd),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            queryEnd,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:           ResourceTypeNode,
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)
	assert.Len(t, graph.Nodes, 2, "应包含 Pod 和 Node 两个节点")
	assert.Len(t, graph.Edges, 1, "应包含一条边")

	// 验证根节点
	podNode := graph.GetNode(podID)
	require.NotNil(t, podNode)
	assert.Equal(t, ResourceTypePod, podNode.ResourceType)
	assert.NotEmpty(t, podNode.EffectivePeriods, "根节点应有有效时间段")

	// 验证目标节点
	nodeNode := graph.GetNode(nodeID)
	require.NotNil(t, nodeNode)
	assert.Equal(t, ResourceTypeNode, nodeNode.ResourceType)
	assert.NotEmpty(t, nodeNode.EffectivePeriods, "目标节点应有有效时间段")

	// 验证边
	edge := graph.GetEdge("node_with_pod:pod-0|node-1")
	require.NotNil(t, edge)
	assert.Equal(t, podID, edge.FromID)
	assert.Equal(t, nodeID, edge.ToID)
}

// TestBuildLivenessGraph_多层遍历_时间段逐层收窄 测试多层遍历时时间段正确传递和收窄
func TestBuildLivenessGraph_多层遍历_时间段逐层收窄(t *testing.T) {
	// 时间设置：Pod 只在前半段可见，Node 全程可见，Service 全程可见
	// 预期：Service 的有效时间段应被 Pod 限制
	t0 := int64(0)
	t1 := int64(1000)
	t2 := int64(2000)

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩"

	queryCount := 0
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			queryCount++
			if strings.Contains(query, "pod_liveness_record") {
				// Pod 只在 [t0, t1] 可见
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(t0),
						"period_end":   float64(t1),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_liveness_record") {
				// Node 全程可见 [t0, t2]
				return []any{
					map[string]any{
						"id":           "node-liveness-1",
						"period_start": float64(t0),
						"period_end":   float64(t2),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				// 关系全程可见
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        nodeID,
						"period_start": float64(t0),
						"period_end":   float64(t2),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            t2,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:           ResourceTypeNode,
		LookBackDelta:        t2,
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)

	// 验证 Pod 的有效时间段
	podNode := graph.GetNode(podID)
	require.NotNil(t, podNode)
	require.Len(t, podNode.EffectivePeriods, 1)
	assert.Equal(t, t0, podNode.EffectivePeriods[0].Start)
	assert.Equal(t, t1, podNode.EffectivePeriods[0].End)

	// 验证 Node 的有效时间段被 Pod 限制
	nodeNode := graph.GetNode(nodeID)
	require.NotNil(t, nodeNode)
	require.Len(t, nodeNode.EffectivePeriods, 1)
	assert.Equal(t, t0, nodeNode.EffectivePeriods[0].Start, "Node 有效时间段应被 Pod 限制")
	assert.Equal(t, t1, nodeNode.EffectivePeriods[0].End, "Node 有效时间段应被 Pod 限制到 t1")

	// 验证边的有效时间段
	edge := graph.GetEdge("node_with_pod:pod-0|node-1")
	require.NotNil(t, edge)
	require.Len(t, edge.EffectivePeriods, 1)
	assert.Equal(t, t0, edge.EffectivePeriods[0].Start)
	assert.Equal(t, t1, edge.EffectivePeriods[0].End)
}

// TestBuildLivenessGraph_最大跳数限制_停止遍历 测试达到最大跳数时停止遍历
func TestBuildLivenessGraph_最大跳数限制_停止遍历(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩"
	systemID := "system:⟨bk_cloud_id=0,bk_target_ip=192.168.1.1⟩"

	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_liveness_record") {
				return []any{
					map[string]any{
						"id":           "node-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "system_liveness_record") {
				return []any{
					map[string]any{
						"id":           "system-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        nodeID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_system") {
				return []any{
					map[string]any{
						"relation_id":  "node_with_system:node-1|system-1",
						"from_id":      nodeID,
						"to_id":        systemID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            baseTime,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		MaxHops:              1, // 只允许 1 跳
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)

	// MaxHops=1 时，应该只能遍历到 Node，不能到达 System
	assert.NotNil(t, graph.GetNode(podID), "应包含根节点 Pod")
	assert.NotNil(t, graph.GetNode(nodeID), "应包含 Node（1跳可达）")
	assert.Nil(t, graph.GetNode(systemID), "不应包含 System（需要2跳）")
}

// TestBuildLivenessGraph_遍历错误_记录但继续 测试遍历过程中出错时记录错误但继续遍历
func TestBuildLivenessGraph_遍历错误_记录但继续(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"

	queryErrorCount := 0
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			// 关系查询返回错误
			if strings.Contains(query, "node_with_pod") {
				queryErrorCount++
				return nil, errors.New("database connection failed")
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            baseTime,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:           ResourceTypeNode,
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	// 函数应该成功返回，但图标记为不完整
	require.NoError(t, err)
	require.NotNil(t, graph)
	assert.NotNil(t, graph.GetNode(podID), "应包含根节点")
	assert.True(t, graph.HasErrors(), "应记录遍历错误")
	assert.False(t, graph.IsComplete(), "图应标记为不完整")
	assert.NotEmpty(t, graph.TraversalErrors)
}

// TestBuildLivenessGraph_关系类别过滤_只遍历静态关系 测试只遍历指定类别的关系
func TestBuildLivenessGraph_关系类别过滤_只遍历静态关系(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩"
	targetPodID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-1⟩"

	dynamicRelationQueried := false
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_liveness_record") {
				return []any{
					map[string]any{
						"id":           "node-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        nodeID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			// 动态关系 pod_to_pod
			if strings.Contains(query, "pod_to_pod") {
				dynamicRelationQueried = true
				return []any{
					map[string]any{
						"relation_id":  "pod_to_pod:pod-0|pod-1",
						"from_id":      podID,
						"to_id":        targetPodID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            baseTime,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic}, // 只允许静态关系
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)
	assert.False(t, dynamicRelationQueried, "不应查询动态关系")
	assert.Nil(t, graph.GetNode(targetPodID), "不应包含通过动态关系到达的节点")
}

// TestBuildLivenessGraph_目标节点无liveness_不添加边和节点 测试目标节点没有有效 liveness 时不添加到图中
func TestBuildLivenessGraph_目标节点无liveness_不添加边和节点(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩"

	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_liveness_record") {
				// Node 在查询时间范围内不可见
				return []any{}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        nodeID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            baseTime,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:           ResourceTypeNode,
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)
	assert.Len(t, graph.Nodes, 1, "只应包含根节点")
	assert.Empty(t, graph.Edges, "不应有边（目标节点不可见）")
	assert.Nil(t, graph.GetNode(nodeID), "不应包含不可见的目标节点")
}

// TestBuildLivenessGraph_边无有效时间段_不添加边 测试边没有有效时间段时不添加
func TestBuildLivenessGraph_边无有效时间段_不添加边(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"

	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				// 关系记录的时间段与查询范围无交集
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩",
						"period_start": float64(queryStart - 2*time.Hour.Milliseconds()),
						"period_end":   float64(queryStart - time.Hour.Milliseconds()),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            baseTime,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:           ResourceTypeNode,
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)
	assert.Len(t, graph.Nodes, 1, "只应包含根节点")
	assert.Empty(t, graph.Edges, "边时间段无交集时不应添加边")
}

// TestBuildLivenessGraph_到达目标类型_停止继续遍历 测试到达目标类型后不再继续遍历
func TestBuildLivenessGraph_到达目标类型_停止继续遍历(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩"
	systemID := "system:⟨bk_cloud_id=0,bk_target_ip=192.168.1.1⟩"

	systemLivenessQueried := false
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_liveness_record") {
				return []any{
					map[string]any{
						"id":           "node-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "system_liveness_record") {
				systemLivenessQueried = true
				return []any{
					map[string]any{
						"id":           "system-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        nodeID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_system") {
				return []any{
					map[string]any{
						"relation_id":  "node_with_system:node-1|system-1",
						"from_id":      nodeID,
						"to_id":        systemID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            baseTime,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:           ResourceTypeNode, // 目标是 Node
		MaxHops:              10,               // 足够大的跳数
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)
	assert.NotNil(t, graph.GetNode(podID))
	assert.NotNil(t, graph.GetNode(nodeID))
	// 到达目标类型 Node 后应停止遍历，不应继续查询 System
	assert.Nil(t, graph.GetNode(systemID), "到达目标类型后不应继续遍历")
	assert.False(t, systemLivenessQueried, "不应查询 System 的 liveness")
}

// TestBuildLivenessGraph_根节点获取失败_返回错误 测试获取根节点 liveness 失败时返回错误
func TestBuildLivenessGraph_根节点获取失败_返回错误(t *testing.T) {
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return nil, errors.New("database error")
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:     time.Now().UnixMilli(),
		SourceType:    ResourceTypePod,
		SourceInfo:    map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:    ResourceTypeNode,
		LookBackDelta: time.Hour.Milliseconds(),
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.Error(t, err)
	assert.Nil(t, graph)
	assert.Contains(t, err.Error(), "failed to get root liveness")
}

// TestBuildLivenessGraph_多条边到同一节点_只入队一次 测试多条边指向同一节点时只入队一次
func TestBuildLivenessGraph_多条边到同一节点_只入队一次(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	nodeID := "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-1⟩"

	nodeLivenessQueryCount := 0
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_liveness_record") {
				nodeLivenessQueryCount++
				return []any{
					map[string]any{
						"id":           "node-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			if strings.Contains(query, "node_with_pod") {
				// 返回两条边指向同一个 Node（模拟多个 Pod 关联同一 Node 的场景）
				return []any{
					map[string]any{
						"relation_id":  "node_with_pod:pod-0|node-1",
						"from_id":      podID,
						"to_id":        nodeID,
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:            baseTime,
		SourceType:           ResourceTypePod,
		SourceInfo:           map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:           ResourceTypeNode,
		LookBackDelta:        time.Hour.Milliseconds(),
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)
	// Node 的 liveness 应该只查询一次（使用缓存）
	assert.Equal(t, 1, nodeLivenessQueryCount, "同一节点的 liveness 应只查询一次")
	assert.Len(t, graph.Nodes, 2)
}

// TestBuildLivenessGraph_默认MaxHops_使用默认值3 测试未设置 MaxHops 时使用默认值
func TestBuildLivenessGraph_默认MaxHops_使用默认值3(t *testing.T) {
	baseTime := time.Now().UnixMilli()
	queryStart := baseTime - time.Hour.Milliseconds()

	podID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"

	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			if strings.Contains(query, "pod_liveness_record") {
				return []any{
					map[string]any{
						"id":           "pod-liveness-1",
						"period_start": float64(queryStart),
						"period_end":   float64(baseTime),
						"is_active":    true,
					},
				}, nil
			}
			return []any{}, nil
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:     baseTime,
		SourceType:    ResourceTypePod,
		SourceInfo:    map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		MaxHops:       0, // 未设置，应使用默认值 3
		LookBackDelta: time.Hour.Milliseconds(),
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, graph)
	assert.NotNil(t, graph.GetNode(podID))
}
