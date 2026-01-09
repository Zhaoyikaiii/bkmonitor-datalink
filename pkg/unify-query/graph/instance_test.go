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

// mockClient 测试用 mock 客户端
type mockClient struct {
	executeFunc func(ctx context.Context, query string, vars map[string]any) (any, error)
}

func (m *mockClient) Connect(ctx context.Context) error { return nil }
func (m *mockClient) Close() error                      { return nil }
func (m *mockClient) Health(ctx context.Context) error  { return nil }
func (m *mockClient) SetTimeout(d time.Duration)        {}

func (m *mockClient) Execute(ctx context.Context, query string, vars map[string]any) (any, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, query, vars)
	}
	return nil, nil
}

func newTestInstance(client Client) *Instance {
	builder := NewQueryBuilder()
	builder.SetTolerance(10 * time.Minute)

	return &Instance{
		ctx:        context.Background(),
		address:    "http://localhost:8000",
		clientType: ClientTypeBKBase,
		namespace:  "test",
		database:   "test",
		timeout:    30 * time.Second,
		maxLimit:   1000,
		tolerance:  10 * time.Minute,
		client:     client,
		builder:    builder,
		parser:     NewResponseParser(),
	}
}

// TestVisiblePeriod_Overlap 测试 VisiblePeriod 的交集计算
func TestVisiblePeriod_Overlap(t *testing.T) {
	tests := []struct {
		name   string
		p1     *VisiblePeriod
		p2     *VisiblePeriod
		expect *VisiblePeriod
	}{
		{
			name:   "完全重叠",
			p1:     &VisiblePeriod{Start: 100, End: 200},
			p2:     &VisiblePeriod{Start: 100, End: 200},
			expect: &VisiblePeriod{Start: 100, End: 200},
		},
		{
			name:   "p1 包含 p2",
			p1:     &VisiblePeriod{Start: 100, End: 300},
			p2:     &VisiblePeriod{Start: 150, End: 250},
			expect: &VisiblePeriod{Start: 150, End: 250},
		},
		{
			name:   "p2 包含 p1",
			p1:     &VisiblePeriod{Start: 150, End: 250},
			p2:     &VisiblePeriod{Start: 100, End: 300},
			expect: &VisiblePeriod{Start: 150, End: 250},
		},
		{
			name:   "部分重叠-左侧",
			p1:     &VisiblePeriod{Start: 100, End: 200},
			p2:     &VisiblePeriod{Start: 150, End: 250},
			expect: &VisiblePeriod{Start: 150, End: 200},
		},
		{
			name:   "部分重叠-右侧",
			p1:     &VisiblePeriod{Start: 150, End: 250},
			p2:     &VisiblePeriod{Start: 100, End: 200},
			expect: &VisiblePeriod{Start: 150, End: 200},
		},
		{
			name:   "边界相接",
			p1:     &VisiblePeriod{Start: 100, End: 200},
			p2:     &VisiblePeriod{Start: 200, End: 300},
			expect: &VisiblePeriod{Start: 200, End: 200},
		},
		{
			name:   "无交集",
			p1:     &VisiblePeriod{Start: 100, End: 200},
			p2:     &VisiblePeriod{Start: 300, End: 400},
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.p1.Overlap(tt.p2)
			if tt.expect == nil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Equal(t, tt.expect.Start, result.Start)
				assert.Equal(t, tt.expect.End, result.End)
			}
		})
	}
}

// TestLivenessGraph_ComputeEffectivePeriods 测试 LivenessGraph 的有效时间段计算
// 场景：Pod -> Node 关联查询
//
// 时间线（查询范围 t0.5 - t3.5）:
//
//	t0    t0.5   t1    t1.5   t2    t2.5   t3    t3.5   t4
//	|------|------|------|------|------|------|------|------|
//	[====Pod-1====]                     [====Pod-1====]
//	       ^                                   ^
//	       |                                   |
//	[=====Node-1======]               [=====Node-1=====]
//	[===Relation===]                  [===Relation===]
//
// Pod-1 liveness:  [t0, t1], [t3, t4]
// Node-1 liveness: [t0, t1.5], [t2.5, t4]
// Relation liveness: [t0, t1], [t3, t4]
//
// 查询范围: [t0.5, t3.5]
//
// 预期结果：
//   - Pod-1 RawPeriods: [t0.5, t1], [t3, t3.5]（裁剪到查询范围）
//   - Node-1 RawPeriods: [t0.5, t1.5], [t2.5, t3.5]（裁剪到查询范围）
//   - Relation RawPeriods: [t0.5, t1], [t3, t3.5]（裁剪到查询范围）
//   - Relation EffectivePeriods: [t0.5, t1], [t3, t3.5]（三者交集）
//   - Node-1 EffectivePeriods: [t0.5, t1], [t3, t3.5]（继承自边）
func TestLivenessGraph_ComputeEffectivePeriods(t *testing.T) {
	// 时间点定义（使用相对偏移，单位：小时）
	t0 := int64(0)
	t0_5 := int64(500)  // t0.5
	t1 := int64(1000)   // t1
	t1_5 := int64(1500) // t1.5
	t2_5 := int64(2500) // t2.5
	t3 := int64(3000)   // t3
	t3_5 := int64(3500) // t3.5
	t4 := int64(4000)   // t4

	tests := []struct {
		name string
		// 图结构
		nodes []struct {
			id         string
			rawPeriods []*VisiblePeriod
		}
		edges []struct {
			id         string
			fromID     string
			toID       string
			rawPeriods []*VisiblePeriod
		}
		rootID string
		// 预期结果
		expectNodeEffective map[string][]*VisiblePeriod
		expectEdgeEffective map[string][]*VisiblePeriod
		description         string
	}{
		{
			name: "单边：Pod -> Node",
			nodes: []struct {
				id         string
				rawPeriods []*VisiblePeriod
			}{
				{
					id: "pod:pod-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0_5, End: t1}, // [t0.5, t1]
						{Start: t3, End: t3_5}, // [t3, t3.5]
					},
				},
				{
					id: "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0_5, End: t1_5}, // [t0.5, t1.5]
						{Start: t2_5, End: t3_5}, // [t2.5, t3.5]
					},
				},
			},
			edges: []struct {
				id         string
				fromID     string
				toID       string
				rawPeriods []*VisiblePeriod
			}{
				{
					id:     "node_with_pod:pod-1|node-1",
					fromID: "pod:pod-1",
					toID:   "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0_5, End: t1}, // [t0.5, t1]
						{Start: t3, End: t3_5}, // [t3, t3.5]
					},
				},
			},
			rootID: "pod:pod-1",
			expectNodeEffective: map[string][]*VisiblePeriod{
				"pod:pod-1": {
					{Start: t0_5, End: t1}, // 根节点 = RawPeriods
					{Start: t3, End: t3_5},
				},
				"node:node-1": {
					{Start: t0_5, End: t1}, // 边的有效时间段
					{Start: t3, End: t3_5},
				},
			},
			expectEdgeEffective: map[string][]*VisiblePeriod{
				"node_with_pod:pod-1|node-1": {
					{Start: t0_5, End: t1}, // Pod ∩ Node ∩ Relation
					{Start: t3, End: t3_5},
				},
			},
			description: "Pod-1 [t0.5-t1, t3-t3.5] -> Node-1 [t0.5-t1.5, t2.5-t3.5]，边 [t0.5-t1, t3-t3.5]",
		},
		{
			name: "时间段收窄：父节点时间段限制子节点",
			nodes: []struct {
				id         string
				rawPeriods []*VisiblePeriod
			}{
				{
					id: "pod:pod-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0_5, End: t1}, // [t0.5, t1] - 父节点只有这一段
					},
				},
				{
					id: "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0, End: t4}, // [t0, t4] - 子节点覆盖全时间段
					},
				},
			},
			edges: []struct {
				id         string
				fromID     string
				toID       string
				rawPeriods []*VisiblePeriod
			}{
				{
					id:     "node_with_pod:pod-1|node-1",
					fromID: "pod:pod-1",
					toID:   "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0, End: t4}, // 边覆盖全时间段
					},
				},
			},
			rootID: "pod:pod-1",
			expectNodeEffective: map[string][]*VisiblePeriod{
				"pod:pod-1": {
					{Start: t0_5, End: t1},
				},
				"node:node-1": {
					{Start: t0_5, End: t1}, // 被父节点限制到 [t0.5, t1]
				},
			},
			expectEdgeEffective: map[string][]*VisiblePeriod{
				"node_with_pod:pod-1|node-1": {
					{Start: t0_5, End: t1}, // 被父节点限制
				},
			},
			description: "父节点 [t0.5-t1] 限制子节点 [t0-t4] -> 有效 [t0.5-t1]",
		},
		{
			name: "无交集：父子时间段不重叠",
			nodes: []struct {
				id         string
				rawPeriods []*VisiblePeriod
			}{
				{
					id: "pod:pod-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0_5, End: t1}, // [t0.5, t1]
					},
				},
				{
					id: "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t2_5, End: t3_5}, // [t2.5, t3.5] - 与父节点无交集
					},
				},
			},
			edges: []struct {
				id         string
				fromID     string
				toID       string
				rawPeriods []*VisiblePeriod
			}{
				{
					id:     "node_with_pod:pod-1|node-1",
					fromID: "pod:pod-1",
					toID:   "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0, End: t4},
					},
				},
			},
			rootID: "pod:pod-1",
			expectNodeEffective: map[string][]*VisiblePeriod{
				"pod:pod-1": {
					{Start: t0_5, End: t1},
				},
				"node:node-1": nil, // 无交集
			},
			expectEdgeEffective: map[string][]*VisiblePeriod{
				"node_with_pod:pod-1|node-1": nil, // 无交集
			},
			description: "父节点 [t0.5-t1] 与子节点 [t2.5-t3.5] 无交集",
		},
		{
			name: "多层传递：Pod -> Node -> Service",
			nodes: []struct {
				id         string
				rawPeriods []*VisiblePeriod
			}{
				{
					id: "pod:pod-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0_5, End: t1_5}, // [t0.5, t1.5]
					},
				},
				{
					id: "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0_5, End: t3}, // [t0.5, t3]
					},
				},
				{
					id: "service:svc-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0, End: t4}, // [t0, t4]
					},
				},
			},
			edges: []struct {
				id         string
				fromID     string
				toID       string
				rawPeriods []*VisiblePeriod
			}{
				{
					id:     "node_with_pod:pod-1|node-1",
					fromID: "pod:pod-1",
					toID:   "node:node-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0, End: t4},
					},
				},
				{
					id:     "pod_with_service:node-1|svc-1",
					fromID: "node:node-1",
					toID:   "service:svc-1",
					rawPeriods: []*VisiblePeriod{
						{Start: t0, End: t4},
					},
				},
			},
			rootID: "pod:pod-1",
			expectNodeEffective: map[string][]*VisiblePeriod{
				"pod:pod-1": {
					{Start: t0_5, End: t1_5}, // 根节点
				},
				"node:node-1": {
					{Start: t0_5, End: t1_5}, // Pod ∩ Node = [t0.5, t1.5]
				},
				"service:svc-1": {
					{Start: t0_5, End: t1_5}, // Node.Effective ∩ Service = [t0.5, t1.5]
				},
			},
			expectEdgeEffective: map[string][]*VisiblePeriod{
				"node_with_pod:pod-1|node-1": {
					{Start: t0_5, End: t1_5},
				},
				"pod_with_service:node-1|svc-1": {
					{Start: t0_5, End: t1_5}, // 继承自 node-1 的有效时间段
				},
			},
			description: "三层传递：Pod [t0.5-t1.5] -> Node [t0.5-t3] -> Service [t0-t4]，逐层收窄到 [t0.5-t1.5]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构建 LivenessGraph
			g := NewLivenessGraph(t0_5, t3_5)

			// 添加节点
			for _, n := range tt.nodes {
				g.AddNode(&NodeLiveness{
					ResourceID: n.id,
					RawPeriods: n.rawPeriods,
				})
			}

			// 添加边
			for _, e := range tt.edges {
				g.AddEdge(&EdgeLiveness{
					RelationID: e.id,
					FromID:     e.fromID,
					ToID:       e.toID,
					RawPeriods: e.rawPeriods,
				})
			}

			// 计算有效时间段
			g.ComputeEffectivePeriods(tt.rootID)

			// 验证节点有效时间段
			for nodeID, expectPeriods := range tt.expectNodeEffective {
				node := g.GetNode(nodeID)
				require.NotNil(t, node, "Node %s should exist", nodeID)

				if expectPeriods == nil {
					assert.Empty(t, node.EffectivePeriods, "Node %s should have no effective periods", nodeID)
				} else {
					require.Len(t, node.EffectivePeriods, len(expectPeriods),
						"Node %s effective periods count mismatch", nodeID)
					for i, expect := range expectPeriods {
						assert.Equal(t, expect.Start, node.EffectivePeriods[i].Start,
							"Node %s period %d start mismatch", nodeID, i)
						assert.Equal(t, expect.End, node.EffectivePeriods[i].End,
							"Node %s period %d end mismatch", nodeID, i)
					}
				}
			}

			// 验证边有效时间段
			for edgeID, expectPeriods := range tt.expectEdgeEffective {
				edge := g.GetEdge(edgeID)
				require.NotNil(t, edge, "Edge %s should exist", edgeID)

				if expectPeriods == nil {
					assert.Empty(t, edge.EffectivePeriods, "Edge %s should have no effective periods", edgeID)
				} else {
					require.Len(t, edge.EffectivePeriods, len(expectPeriods),
						"Edge %s effective periods count mismatch", edgeID)
					for i, expect := range expectPeriods {
						assert.Equal(t, expect.Start, edge.EffectivePeriods[i].Start,
							"Edge %s period %d start mismatch", edgeID, i)
						assert.Equal(t, expect.End, edge.EffectivePeriods[i].End,
							"Edge %s period %d end mismatch", edgeID, i)
					}
				}
			}
		})
	}
}

// TestInstance_GetVisiblePeriods 测试获取资源在查询时间范围内的可见时间段
// 核心逻辑：
//   - 从 SurrealDB 获取与查询范围有交集的 liveness 记录
//   - 对每条记录计算与查询范围的交集，裁剪掉查询范围外的部分
//   - 返回可见时间段列表：[max(period_start, query_start), min(period_end, query_end)]
func TestInstance_GetVisiblePeriods(t *testing.T) {
	// 基准时间点：2026-01-09 12:00:00
	baseTime := time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		// 资源的 liveness 记录（可能有多条）
		livenessRecords []struct {
			periodStart time.Time
			periodEnd   time.Time
		}
		// 查询时间范围
		queryStart time.Time
		queryEnd   time.Time
		// 预期的可见时间段
		expectPeriods []struct {
			start time.Time
			end   time.Time
		}
		description string
	}{
		{
			name: "单个周期-资源周期完全包含查询范围",
			livenessRecords: []struct {
				periodStart time.Time
				periodEnd   time.Time
			}{
				{baseTime.Add(-2 * time.Hour), baseTime.Add(2 * time.Hour)}, // 10:00-14:00
			},
			queryStart: baseTime.Add(-1 * time.Hour), // 11:00
			queryEnd:   baseTime.Add(1 * time.Hour),  // 13:00
			expectPeriods: []struct {
				start time.Time
				end   time.Time
			}{
				{baseTime.Add(-1 * time.Hour), baseTime.Add(1 * time.Hour)}, // 11:00-13:00
			},
			description: "资源 10:00-14:00，查询 11:00-13:00，可见 11:00-13:00（裁剪到查询范围）",
		},
		{
			name: "单个周期-查询范围完全包含资源周期",
			livenessRecords: []struct {
				periodStart time.Time
				periodEnd   time.Time
			}{
				{baseTime.Add(-30 * time.Minute), baseTime.Add(30 * time.Minute)}, // 11:30-12:30
			},
			queryStart: baseTime.Add(-1 * time.Hour), // 11:00
			queryEnd:   baseTime.Add(1 * time.Hour),  // 13:00
			expectPeriods: []struct {
				start time.Time
				end   time.Time
			}{
				{baseTime.Add(-30 * time.Minute), baseTime.Add(30 * time.Minute)}, // 11:30-12:30
			},
			description: "资源 11:30-12:30，查询 11:00-13:00，可见 11:30-12:30（完整资源周期）",
		},
		{
			name: "单个周期-左侧部分重叠",
			livenessRecords: []struct {
				periodStart time.Time
				periodEnd   time.Time
			}{
				{baseTime.Add(-2 * time.Hour), baseTime}, // 10:00-12:00
			},
			queryStart: baseTime.Add(-1 * time.Hour), // 11:00
			queryEnd:   baseTime.Add(1 * time.Hour),  // 13:00
			expectPeriods: []struct {
				start time.Time
				end   time.Time
			}{
				{baseTime.Add(-1 * time.Hour), baseTime}, // 11:00-12:00
			},
			description: "资源 10:00-12:00，查询 11:00-13:00，可见 11:00-12:00（裁剪左侧）",
		},
		{
			name: "单个周期-右侧部分重叠",
			livenessRecords: []struct {
				periodStart time.Time
				periodEnd   time.Time
			}{
				{baseTime, baseTime.Add(2 * time.Hour)}, // 12:00-14:00
			},
			queryStart: baseTime.Add(-1 * time.Hour), // 11:00
			queryEnd:   baseTime.Add(1 * time.Hour),  // 13:00
			expectPeriods: []struct {
				start time.Time
				end   time.Time
			}{
				{baseTime, baseTime.Add(1 * time.Hour)}, // 12:00-13:00
			},
			description: "资源 12:00-14:00，查询 11:00-13:00，可见 12:00-13:00（裁剪右侧）",
		},
		{
			name: "无交集-查询范围在资源周期之前",
			livenessRecords: []struct {
				periodStart time.Time
				periodEnd   time.Time
			}{},
			queryStart: baseTime.Add(-3 * time.Hour), // 09:00
			queryEnd:   baseTime.Add(-2 * time.Hour), // 10:00
			expectPeriods: []struct {
				start time.Time
				end   time.Time
			}{},
			description: "资源不在查询范围内，返回空列表",
		},
		{
			name: "多个周期-中间有 gap",
			livenessRecords: []struct {
				periodStart time.Time
				periodEnd   time.Time
			}{
				{baseTime.Add(-2 * time.Hour), baseTime.Add(-1 * time.Hour)},              // 10:00-11:00
				{baseTime.Add(-30 * time.Minute), baseTime.Add(30 * time.Minute)},         // 11:30-12:30
				{baseTime.Add(1*time.Hour + 30*time.Minute), baseTime.Add(2 * time.Hour)}, // 13:30-14:00
			},
			queryStart: baseTime.Add(-90 * time.Minute), // 10:30
			queryEnd:   baseTime.Add(90 * time.Minute),  // 13:30
			expectPeriods: []struct {
				start time.Time
				end   time.Time
			}{
				{baseTime.Add(-90 * time.Minute), baseTime.Add(-1 * time.Hour)},   // 10:30-11:00（第一段裁剪）
				{baseTime.Add(-30 * time.Minute), baseTime.Add(30 * time.Minute)}, // 11:30-12:30（完整）
				{baseTime.Add(90 * time.Minute), baseTime.Add(90 * time.Minute)},  // 13:30-13:30（边界点）
			},
			description: "多个 liveness 记录，中间有 gap，分别裁剪后返回",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构造 mock 返回
			client := &mockClient{
				executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
					// 模拟数据库行为：返回与查询范围有交集的 liveness 记录
					var results []any
					for _, record := range tt.livenessRecords {
						// 检查是否有交集
						if record.periodStart.UnixMilli() <= tt.queryEnd.UnixMilli() &&
							record.periodEnd.UnixMilli() >= tt.queryStart.UnixMilli() {
							results = append(results, map[string]any{
								"id":           "liveness-record",
								"period_start": float64(record.periodStart.UnixMilli()),
								"period_end":   float64(record.periodEnd.UnixMilli()),
								"is_active":    true,
								"created_at":   float64(record.periodStart.UnixMilli()),
								"updated_at":   float64(record.periodEnd.UnixMilli()),
							})
						}
					}
					return results, nil
				},
			}

			instance := newTestInstance(client)
			resourceID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"

			periods, err := instance.GetVisiblePeriods(
				context.Background(),
				resourceID,
				tt.queryStart.UnixMilli(),
				tt.queryEnd.UnixMilli(),
			)

			require.NoError(t, err)
			require.Len(t, periods, len(tt.expectPeriods), tt.description)

			for i, expect := range tt.expectPeriods {
				assert.Equal(t, expect.start.UnixMilli(), periods[i].Start,
					"Period %d start mismatch: expected %v, got %v",
					i, expect.start, time.UnixMilli(periods[i].Start))
				assert.Equal(t, expect.end.UnixMilli(), periods[i].End,
					"Period %d end mismatch: expected %v, got %v",
					i, expect.end, time.UnixMilli(periods[i].End))
			}
		})
	}
}

// TestBuildLivenessGraph_SkipEdgeWhenTargetLivenessFails
// 目标节点 liveness 查询失败时：
//   - 边不会加入图，保持节点/边一致性
//   - 错误会被记录在 TraversalErrors 中
func TestBuildLivenessGraph_SkipEdgeWhenTargetLivenessFails(t *testing.T) {
	rootID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"
	targetID := "system:⟨bk_cloud_id=0,bk_target_ip=1.1.1.1⟩"

	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			switch {
			case strings.Contains(query, "pod_liveness_record"):
				return []any{
					map[string]any{
						"id":           "root-live",
						"period_start": float64(0),
						"period_end":   float64(1000),
						"is_active":    true,
						"created_at":   float64(0),
						"updated_at":   float64(1000),
					},
				}, nil
			case strings.Contains(query, "pod_to_system"):
				return []any{
					map[string]any{
						"relation_id":  "pod_to_system:1",
						"from_id":      rootID,
						"to_id":        targetID,
						"period_start": float64(0),
						"period_end":   float64(1000),
					},
				}, nil
			case strings.Contains(query, targetID):
				return nil, errors.New("query failed")
			default:
				return nil, nil
			}
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:     1000,
		LookBackDelta: 1000,
		SourceType:    ResourceTypePod,
		SourceInfo:    map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:    ResourceTypeSystem,
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)
	require.NoError(t, err)

	assert.True(t, graph.HasErrors(), "expected traversal errors to be recorded")
	assert.Empty(t, graph.Edges, "edge should not be added when target liveness fails")
	assert.NotNil(t, graph.GetNode(rootID), "root node should exist")
	assert.Nil(t, graph.GetNode(targetID), "target node should not be added when liveness fails")
}

// TestBuildLivenessGraph_RecordRelationQueryError
// 关系查询失败时应记录错误，且不会添加边
func TestBuildLivenessGraph_RecordRelationQueryError(t *testing.T) {
	rootID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"

	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			switch {
			case strings.Contains(query, "pod_liveness_record"):
				return []any{
					map[string]any{
						"id":           "root-live",
						"period_start": float64(0),
						"period_end":   float64(1000),
						"is_active":    true,
						"created_at":   float64(0),
						"updated_at":   float64(1000),
					},
				}, nil
			case strings.Contains(query, "pod_to_system"):
				return nil, errors.New("relation query failed")
			default:
				return nil, nil
			}
		},
	}

	instance := newTestInstance(client)
	req := &QueryRequest{
		Timestamp:     1000,
		LookBackDelta: 1000,
		SourceType:    ResourceTypePod,
		SourceInfo:    map[string]string{"bcs_cluster_id": "BCS-K8S-001", "namespace": "default", "pod": "pod-0"},
		TargetType:    ResourceTypeSystem,
	}

	graph, err := instance.BuildLivenessGraph(context.Background(), req)
	require.NoError(t, err)

	assert.True(t, graph.HasErrors(), "relation query error should be recorded")
	assert.Empty(t, graph.Edges, "edges should not be added when relation query fails")
	assert.NotNil(t, graph.GetNode(rootID))
}

// TestQueryBuilder_EscapeIdentifiers 确保查询字符串对资源ID进行了转义
func TestQueryBuilder_EscapeIdentifiers(t *testing.T) {
	builder := NewQueryBuilder()
	builder.DisableLimit()

	resourceID := "pod:⟨name=o'clock\\path⟩"

	liveSQL := builder.BuildLivenessQuery(ResourceTypePod, resourceID, 0, 1000)
	assert.Contains(t, liveSQL, "pod_id = 'pod:⟨name=o\\'clock\\\\path⟩'")
	assert.NotContains(t, liveSQL, "o'clock\\path")

	relationSQL := builder.BuildRelatedResourcesQuery(RelationPodToSystem, resourceID, DirectionOutbound, 0, 1000)
	assert.Contains(t, relationSQL, "WHERE in = 'pod:⟨name=o\\'clock\\\\path⟩'")
}
