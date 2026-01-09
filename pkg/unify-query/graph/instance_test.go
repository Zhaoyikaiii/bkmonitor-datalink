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

func TestInstance_QuerySingleHop(t *testing.T) {
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			return []any{
				map[string]any{
					"id":         "node_with_pod:⟨bcs_cluster_id=BCS-K8S-001,node=node-0|bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩",
					"in":         "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-0⟩",
					"out":        "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩",
					"updated_at": float64(time.Now().UnixMilli()),
				},
			}, nil
		},
	}

	instance := newTestInstance(client)

	req := &HopQueryRequest{
		Timestamp:  time.Now().UnixMilli(),
		SourceType: ResourceTypeNode,
		SourceInfo: map[string]string{
			"bcs_cluster_id": "BCS-K8S-001",
			"node":           "node-0",
		},
		TargetType:           ResourceTypePod,
		AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
	}

	resp, err := instance.QuerySingleHop(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, req.Timestamp, resp.Timestamp)
	assert.Equal(t, req.SourceType, resp.SourceType)
	assert.Equal(t, req.TargetType, resp.TargetType)
}

func TestInstance_QueryResources(t *testing.T) {
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			return []any{
				map[string]any{
					"id":             "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩",
					"bcs_cluster_id": "BCS-K8S-001",
					"namespace":      "default",
					"pod":            "pod-0",
					"updated_at":     float64(time.Now().UnixMilli()),
				},
			}, nil
		},
	}

	instance := newTestInstance(client)

	req := &ResourceQueryRequest{
		ResourceType: ResourceTypePod,
		Labels: map[string]string{
			"namespace": "default",
		},
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
	}

	resp, err := instance.QueryResources(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Len(t, resp.Resources, 1)
	assert.Equal(t, ResourceTypePod, resp.Resources[0].Type)
}

func TestInstance_QueryRelations(t *testing.T) {
	client := &mockClient{
		executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
			return []any{
				map[string]any{
					"id":         "node_with_pod:⟨...|...⟩",
					"in":         "node:⟨bcs_cluster_id=BCS-K8S-001,node=node-0⟩",
					"out":        "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩",
					"updated_at": float64(time.Now().UnixMilli()),
				},
			}, nil
		},
	}

	instance := newTestInstance(client)

	req := &RelationQueryRequest{
		FromType:     ResourceTypeNode,
		ToType:       ResourceTypePod,
		RelationType: RelationNodeWithPod,
		StartTime:    time.Now().Add(-1 * time.Hour),
		EndTime:      time.Now(),
	}

	resp, err := instance.QueryRelations(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Len(t, resp.Relations, 1)
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
				{baseTime.Add(-2 * time.Hour), baseTime.Add(-1 * time.Hour)},            // 10:00-11:00
				{baseTime.Add(-30 * time.Minute), baseTime.Add(30 * time.Minute)},       // 11:30-12:30
				{baseTime.Add(1*time.Hour + 30*time.Minute), baseTime.Add(2*time.Hour)}, // 13:30-14:00
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

func TestInstance_CheckLiveness(t *testing.T) {
	// 简单测试：有记录返回 true，无记录返回 false
	t.Run("有 liveness 记录返回 true", func(t *testing.T) {
		client := &mockClient{
			executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
				return []any{
					map[string]any{
						"id":           "liveness-1",
						"period_start": float64(time.Now().Add(-1 * time.Hour).UnixMilli()),
						"period_end":   float64(time.Now().UnixMilli()),
						"is_active":    true,
					},
				}, nil
			},
		}

		instance := newTestInstance(client)
		alive, err := instance.CheckLiveness(
			context.Background(),
			"pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩",
			time.Now().Add(-30*time.Minute).UnixMilli(),
			time.Now().UnixMilli(),
		)

		require.NoError(t, err)
		assert.True(t, alive)
	})

	t.Run("无 liveness 记录返回 false", func(t *testing.T) {
		client := &mockClient{
			executeFunc: func(ctx context.Context, query string, vars map[string]any) (any, error) {
				return []any{}, nil
			},
		}

		instance := newTestInstance(client)
		alive, err := instance.CheckLiveness(
			context.Background(),
			"pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩",
			time.Now().Add(-30*time.Minute).UnixMilli(),
			time.Now().UnixMilli(),
		)

		require.NoError(t, err)
		assert.False(t, alive)
	})
}

func TestInstance_Health(t *testing.T) {
	client := &mockClient{}
	instance := newTestInstance(client)

	err := instance.Health(context.Background())
	assert.NoError(t, err)
}

func TestInstance_SetTolerance(t *testing.T) {
	client := &mockClient{}
	instance := newTestInstance(client)

	instance.SetTolerance(5 * time.Minute)
	assert.Equal(t, 5*time.Minute, instance.tolerance)
}
