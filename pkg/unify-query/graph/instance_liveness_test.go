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

func TestBuildLivenessGraph(t *testing.T) {
	ctx := context.Background()

	testCases := map[string]struct {
		mockData map[string]any
		req      any
		expected string
	}{
		"single hop pod to node": {
			mockData: nil,
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
			expected: `{
				"query_start": 1736424000000,
				"query_end": 1736510400000,
				"nodes": {
					"pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩": {
						"resource_id": "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩",
						"resource_type": "pod",
						"labels": {"bcs_cluster_id": "BCS-K8S-00001", "namespace": "default", "pod": "nginx-pod-1"},
						"raw_periods": [{"start": 1736424000000, "end": 1736510400000}],
						"effective_periods": [{"start": 1736424000000, "end": 1736510400000}]
					},
					"node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩": {
						"resource_id": "node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩",
						"resource_type": "node",
						"raw_periods": [{"start": 1736424000000, "end": 1736510400000}],
						"effective_periods": [{"start": 1736424000000, "end": 1736510400000}]
					}
				},
				"edges": {
					"node_with_pod:1": {
						"relation_id": "node_with_pod:1",
						"relation_type": "node_with_pod",
						"category": "static",
						"direction": "inbound",
						"from_id": "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩",
						"to_id": "node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩",
						"raw_periods": [{"start": 1736424000000, "end": 1736510400000}],
						"effective_periods": [{"start": 1736424000000, "end": 1736510400000}]
					}
				},
				"adjacency": {
					"pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩": ["node_with_pod:1"],
					"node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩": []
				}
			}`,
		},
		"root node not exist": {
			mockData: nil,
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
			expected: `{
				"query_start": 1736424000000,
				"query_end": 1736510400000,
				"nodes": {},
				"edges": {},
				"adjacency": {}
			}`,
		},
		"full graph traversal with 7 days lookback": {
			mockData: map[string]any{
				"USE NS default DB test; INFO FOR DB": mock.InfoForDBResponse,
				"USE NS default DB test; SELECT * FROM pod_liveness_record \nWHERE pod_id = 'pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩' \nAND period_start <= 1736511000000 \nAND period_end >= 1735905000000\nORDER BY period_start ASC\nLIMIT 1000":                                                                                                          `[{"status":"OK","result":null},{"status":"OK","result":[{"id":"pod_liveness_record:BCS-K8S-00002:bkop:pod-0","pod_id":"pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩","period_start":1735905600000,"period_end":1736510400000}]}]`,
				"USE NS default DB test; SELECT \n    id AS relation_id,\n    out AS from_id,\n    in AS to_id,\n    period_start,\n    period_end,\n    is_active\nFROM node_with_pod \nWHERE out = 'pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩' \nAND period_start <= 1736511000000 \nAND period_end >= 1735905000000\nORDER BY period_start ASC\nLIMIT 1000": `[{"status":"OK","result":null},{"status":"OK","result":[{"relation_id":"node_with_pod:1","from_id":"pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩","to_id":"node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩","period_start":1735905600000,"period_end":1736510400000,"is_active":true}]}]`,
				"USE NS default DB test; SELECT * FROM node_liveness_record \nWHERE node_id = 'node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩' \nAND period_start <= 1736511000000 \nAND period_end >= 1735905000000\nORDER BY period_start ASC\nLIMIT 1000":                                                                                                                    `[{"status":"OK","result":null},{"status":"OK","result":[{"id":"node_liveness_record:BCS-K8S-00002:node-0","node_id":"node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩","period_start":1735905600000,"period_end":1736510400000}]}]`,
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
			expected: `{
				"query_start": 1735905600000,
				"query_end": 1736510400000,
				"nodes": {
					"pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩": {
						"resource_id": "pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩",
						"resource_type": "pod",
						"labels": {"bcs_cluster_id": "BCS-K8S-00002", "namespace": "bkop", "pod": "pod-0"},
						"raw_periods": [{"start": 1735905600000, "end": 1736510400000}],
						"effective_periods": [{"start": 1735905600000, "end": 1736510400000}]
					},
					"node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩": {
						"resource_id": "node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩",
						"resource_type": "node",
						"raw_periods": [{"start": 1735905600000, "end": 1736510400000}],
						"effective_periods": [{"start": 1735905600000, "end": 1736510400000}]
					}
				},
				"edges": {
					"node_with_pod:1": {
						"relation_id": "node_with_pod:1",
						"relation_type": "node_with_pod",
						"category": "static",
						"direction": "inbound",
						"from_id": "pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩",
						"to_id": "node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩",
						"raw_periods": [{"start": 1735905600000, "end": 1736510400000}],
						"effective_periods": [{"start": 1735905600000, "end": 1736510400000}]
					}
				},
				"adjacency": {
					"pod:⟨bcs_cluster_id=BCS-K8S-00002,namespace=bkop,pod=pod-0⟩": ["node_with_pod:1"],
					"node:⟨bcs_cluster_id=BCS-K8S-00002,node=node-0⟩": []
				}
			}`,
		},
		"query with struct request": {
			mockData: nil,
			req: &QueryRequest{
				Timestamp:  1736510400000,
				SourceType: ResourceTypePod,
				SourceInfo: map[string]string{
					"bcs_cluster_id": "BCS-K8S-00001",
					"namespace":      "default",
					"pod":            "nginx-pod-1",
				},
				TargetType:           ResourceTypeNode,
				MaxHops:              3,
				LookBackDelta:        86400000,
				AllowedRelationTypes: []RelationCategory{RelationCategoryStatic},
			},
			expected: `{
				"query_start": 1736424000000,
				"query_end": 1736510400000,
				"nodes": {
					"pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩": {
						"resource_id": "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩",
						"resource_type": "pod",
						"labels": {"bcs_cluster_id": "BCS-K8S-00001", "namespace": "default", "pod": "nginx-pod-1"},
						"raw_periods": [{"start": 1736424000000, "end": 1736510400000}],
						"effective_periods": [{"start": 1736424000000, "end": 1736510400000}]
					},
					"node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩": {
						"resource_id": "node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩",
						"resource_type": "node",
						"raw_periods": [{"start": 1736424000000, "end": 1736510400000}],
						"effective_periods": [{"start": 1736424000000, "end": 1736510400000}]
					}
				},
				"edges": {
					"node_with_pod:1": {
						"relation_id": "node_with_pod:1",
						"relation_type": "node_with_pod",
						"category": "static",
						"direction": "inbound",
						"from_id": "pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩",
						"to_id": "node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩",
						"raw_periods": [{"start": 1736424000000, "end": 1736510400000}],
						"effective_periods": [{"start": 1736424000000, "end": 1736510400000}]
					}
				},
				"adjacency": {
					"pod:⟨bcs_cluster_id=BCS-K8S-00001,namespace=default,pod=nginx-pod-1⟩": ["node_with_pod:1"],
					"node:⟨bcs_cluster_id=BCS-K8S-00001,node=node-1⟩": []
				}
			}`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			mock.SurrealDB.Clear()
			if tc.mockData != nil {
				mock.SurrealDB.Set(tc.mockData)
			} else {
				mock.SurrealDB.Set(mock.MockData)
			}
			defer mock.SurrealDB.Clear()

			instance, err := newMockInstance(ctx)
			require.NoError(t, err)
			defer instance.Close()

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

			graph, err := instance.BuildLivenessGraph(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, graph)

			actual, err := json.Marshal(graph)
			require.NoError(t, err)
			assert.JSONEq(t, tc.expected, string(actual))
		})
	}
}
