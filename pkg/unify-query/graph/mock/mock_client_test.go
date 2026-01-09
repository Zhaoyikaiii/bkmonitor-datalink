// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package mock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/graph"
)

func TestMockClient_Connect(t *testing.T) {
	client := NewMockClient()
	err := client.Connect(context.Background())
	assert.NoError(t, err)
}

func TestMockClient_Health(t *testing.T) {
	client := NewMockClient()
	err := client.Health(context.Background())
	assert.NoError(t, err)
}

func TestMockClient_Execute(t *testing.T) {
	client := NewMockClient()

	// Test INFO FOR DB
	result, err := client.Execute(context.Background(), "INFO FOR DB", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify call was recorded
	calls := client.GetExecuteCalls()
	assert.Len(t, calls, 1)
	assert.Equal(t, "INFO FOR DB", calls[0].Query)
}

func TestMockClient_CustomExecuteFunc(t *testing.T) {
	client := NewMockClient()

	expectedResult := map[string]any{"custom": "result"}
	client.SetExecuteFunc(func(ctx context.Context, query string, vars map[string]any) (any, error) {
		return expectedResult, nil
	})

	result, err := client.Execute(context.Background(), "SELECT * FROM test", nil)
	require.NoError(t, err)
	assert.Equal(t, expectedResult, result)
}

func TestMockClient_AddResource(t *testing.T) {
	client := NewMockClient()

	pod := GenerateMockPod("BCS-K8S-001", "default", "pod-0")
	client.AddResource(pod)

	// Query should return the resource
	result, err := client.Execute(context.Background(), "SELECT * FROM pod", nil)
	require.NoError(t, err)

	data, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, data, 1)
}

func TestMockClient_AddRelation(t *testing.T) {
	client := NewMockClient()

	node := GenerateMockNode("BCS-K8S-001", "node-0")
	pod := GenerateMockPod("BCS-K8S-001", "default", "pod-0")
	relation := GenerateMockRelation(graph.RelationNodeWithPod, node, pod)

	client.AddResource(node)
	client.AddResource(pod)
	client.AddRelation(relation)

	// Query should return the relation
	result, err := client.Execute(context.Background(), "SELECT * FROM node_with_pod", nil)
	require.NoError(t, err)

	data, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, data, 1)
}

func TestMockClient_AddLivenessRecord(t *testing.T) {
	client := NewMockClient()

	now := time.Now()
	resourceID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"

	record := GenerateMockLivenessRecord(resourceID, now.Add(-1*time.Hour).UnixMilli(), now.UnixMilli(), true)
	client.AddLivenessRecord(resourceID, record)

	// Query should return the liveness record
	result, err := client.Execute(context.Background(), "SELECT * FROM pod_liveness_record", nil)
	require.NoError(t, err)

	data, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, data, 1)
}

func TestMockClient_Reset(t *testing.T) {
	client := NewMockClient()

	pod := GenerateMockPod("BCS-K8S-001", "default", "pod-0")
	client.AddResource(pod)

	// Execute a query to record a call
	_, _ = client.Execute(context.Background(), "SELECT * FROM pod", nil)

	// Reset
	client.Reset()

	// Verify everything is cleared
	calls := client.GetExecuteCalls()
	assert.Len(t, calls, 0)

	result, err := client.Execute(context.Background(), "SELECT * FROM pod", nil)
	require.NoError(t, err)
	data, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, data, 0)
}

func TestBuildK8sClusterScenario(t *testing.T) {
	client := NewMockClient()

	BuildK8sClusterScenario(client, "BCS-K8S-001", 3, 10, 2)

	// Verify nodes
	nodeResult, err := client.Execute(context.Background(), "SELECT * FROM node", nil)
	require.NoError(t, err)
	nodeData, ok := nodeResult.([]any)
	require.True(t, ok)
	assert.Len(t, nodeData, 3)

	// Verify pods
	podResult, err := client.Execute(context.Background(), "SELECT * FROM pod", nil)
	require.NoError(t, err)
	podData, ok := podResult.([]any)
	require.True(t, ok)
	assert.Len(t, podData, 10)

	// Verify services
	serviceResult, err := client.Execute(context.Background(), "SELECT * FROM service", nil)
	require.NoError(t, err)
	serviceData, ok := serviceResult.([]any)
	require.True(t, ok)
	assert.Len(t, serviceData, 2)

	// Verify node_with_pod relations
	relationResult, err := client.Execute(context.Background(), "SELECT * FROM node_with_pod", nil)
	require.NoError(t, err)
	relationData, ok := relationResult.([]any)
	require.True(t, ok)
	assert.Len(t, relationData, 10) // Each pod has one node relation
}

func TestGenerateMockPod(t *testing.T) {
	pod := GenerateMockPod("BCS-K8S-001", "default", "pod-0")

	assert.Equal(t, graph.ResourceTypePod, pod.Type)
	assert.Equal(t, "BCS-K8S-001", pod.Labels["bcs_cluster_id"])
	assert.Equal(t, "default", pod.Labels["namespace"])
	assert.Equal(t, "pod-0", pod.Labels["pod"])
	assert.Contains(t, pod.ID, "pod:⟨")
}

func TestGenerateMockNode(t *testing.T) {
	node := GenerateMockNode("BCS-K8S-001", "node-0")

	assert.Equal(t, graph.ResourceTypeNode, node.Type)
	assert.Equal(t, "BCS-K8S-001", node.Labels["bcs_cluster_id"])
	assert.Equal(t, "node-0", node.Labels["node"])
	assert.Contains(t, node.ID, "node:⟨")
}

func TestGenerateMockService(t *testing.T) {
	service := GenerateMockService("BCS-K8S-001", "default", "service-0")

	assert.Equal(t, graph.ResourceTypeService, service.Type)
	assert.Equal(t, "BCS-K8S-001", service.Labels["bcs_cluster_id"])
	assert.Equal(t, "default", service.Labels["namespace"])
	assert.Equal(t, "service-0", service.Labels["service"])
	assert.Contains(t, service.ID, "service:⟨")
}

func TestGenerateMockRelation(t *testing.T) {
	node := GenerateMockNode("BCS-K8S-001", "node-0")
	pod := GenerateMockPod("BCS-K8S-001", "default", "pod-0")

	relation := GenerateMockRelation(graph.RelationNodeWithPod, node, pod)

	assert.Equal(t, graph.RelationNodeWithPod, relation.Type)
	assert.Equal(t, node.ID, relation.FromID)
	assert.Equal(t, pod.ID, relation.ToID)
	assert.Contains(t, relation.ID, "node_with_pod:⟨")
}

func TestGenerateMockLivenessRecord(t *testing.T) {
	now := time.Now().UnixMilli()
	resourceID := "pod:⟨bcs_cluster_id=BCS-K8S-001,namespace=default,pod=pod-0⟩"

	record := GenerateMockLivenessRecord(resourceID, now-3600000, now, true)

	assert.Equal(t, resourceID, record.ResourceID)
	assert.Equal(t, now-3600000, record.PeriodStart)
	assert.Equal(t, now, record.PeriodEnd)
	assert.True(t, record.IsActive)
}
