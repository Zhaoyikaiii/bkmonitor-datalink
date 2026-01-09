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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/graph"
)

// MockClient implements the graph.Client interface for testing
type MockClient struct {
	mu sync.RWMutex

	// Mock data storage
	resources       map[string]*graph.Resource
	relations       map[string]*graph.Relation
	livenessRecords map[string][]*graph.LivenessRecord

	// Custom behavior functions
	executeFunc func(ctx context.Context, query string, vars map[string]any) (any, error)

	// Call tracking
	executeCalls []ExecuteCall
}

// ExecuteCall records a call to Execute
type ExecuteCall struct {
	Query string
	Vars  map[string]any
}

// NewMockClient creates a new MockClient
func NewMockClient() *MockClient {
	return &MockClient{
		resources:       make(map[string]*graph.Resource),
		relations:       make(map[string]*graph.Relation),
		livenessRecords: make(map[string][]*graph.LivenessRecord),
		executeCalls:    make([]ExecuteCall, 0),
	}
}

// Connect implements graph.Client
func (c *MockClient) Connect(ctx context.Context) error {
	return nil
}

// Close implements graph.Client
func (c *MockClient) Close() error {
	return nil
}

// Execute implements graph.Client
func (c *MockClient) Execute(ctx context.Context, query string, vars map[string]any) (any, error) {
	c.mu.Lock()
	c.executeCalls = append(c.executeCalls, ExecuteCall{Query: query, Vars: vars})
	c.mu.Unlock()

	if c.executeFunc != nil {
		return c.executeFunc(ctx, query, vars)
	}

	// Default behavior: parse query and return mock data
	return c.handleQuery(query, vars)
}

// Health implements graph.Client
func (c *MockClient) Health(ctx context.Context) error {
	return nil
}

// SetExecuteFunc sets a custom execute function
func (c *MockClient) SetExecuteFunc(fn func(ctx context.Context, query string, vars map[string]any) (any, error)) {
	c.executeFunc = fn
}

// AddResource adds a resource to the mock data
func (c *MockClient) AddResource(resource *graph.Resource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resources[resource.ID] = resource
}

// AddRelation adds a relation to the mock data
func (c *MockClient) AddRelation(relation *graph.Relation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.relations[relation.ID] = relation
}

// AddLivenessRecord adds a liveness record to the mock data
func (c *MockClient) AddLivenessRecord(resourceID string, record *graph.LivenessRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.livenessRecords[resourceID] = append(c.livenessRecords[resourceID], record)
}

// GetExecuteCalls returns all recorded Execute calls
func (c *MockClient) GetExecuteCalls() []ExecuteCall {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]ExecuteCall{}, c.executeCalls...)
}

// Reset clears all mock data and call history
func (c *MockClient) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resources = make(map[string]*graph.Resource)
	c.relations = make(map[string]*graph.Relation)
	c.livenessRecords = make(map[string][]*graph.LivenessRecord)
	c.executeCalls = make([]ExecuteCall, 0)
	c.executeFunc = nil
}

// handleQuery handles a query based on its content
func (c *MockClient) handleQuery(query string, vars map[string]any) (any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	query = strings.ToLower(query)

	// Handle INFO FOR DB
	if strings.Contains(query, "info for db") {
		return map[string]any{"tables": []string{
			string(graph.ResourceTypePod),
			string(graph.ResourceTypeNode),
			string(graph.ResourceTypeService),
		}}, nil
	}

	// Handle SELECT queries
	if strings.Contains(query, "select") {
		return c.handleSelectQuery(query, vars)
	}

	return nil, nil
}

// handleSelectQuery handles SELECT queries
func (c *MockClient) handleSelectQuery(query string, vars map[string]any) (any, error) {
	// Check if it's a liveness query
	if strings.Contains(query, "liveness_record") {
		return c.handleLivenessQuery(query, vars)
	}

	// Check if it's a relation query - use registered relation types from schema
	for _, relType := range graph.GetAllRelationTypes() {
		if strings.Contains(query, string(relType)) {
			return c.handleRelationQuery(query, string(relType))
		}
	}

	// Check if it's a resource query - use known resource types
	resourceTypes := []graph.ResourceType{
		graph.ResourceTypePod,
		graph.ResourceTypeNode,
		graph.ResourceTypeContainer,
		graph.ResourceTypeService,
		graph.ResourceTypeDeployment,
		graph.ResourceTypeReplicaSet,
		graph.ResourceTypeStatefulSet,
		graph.ResourceTypeDaemonSet,
		graph.ResourceTypeJob,
		graph.ResourceTypeIngress,
		graph.ResourceTypeSystem,
		graph.ResourceTypeAPMService,
		graph.ResourceTypeAPMServiceInstance,
		graph.ResourceTypeHost,
		graph.ResourceTypeBiz,
		graph.ResourceTypeSet,
		graph.ResourceTypeModule,
	}
	for _, resType := range resourceTypes {
		if strings.Contains(query, "from "+string(resType)) {
			return c.handleResourceQuery(query, string(resType))
		}
	}

	return []any{}, nil
}

// handleLivenessQuery handles liveness record queries
func (c *MockClient) handleLivenessQuery(query string, vars map[string]any) (any, error) {
	var results []any

	for _, records := range c.livenessRecords {
		for _, record := range records {
			results = append(results, map[string]any{
				graph.FieldID:          record.ID,
				graph.FieldPeriodStart: float64(record.PeriodStart),
				graph.FieldPeriodEnd:   float64(record.PeriodEnd),
				graph.FieldIsActive:    record.IsActive,
				graph.FieldCreatedAt:   float64(record.CreatedAt),
				graph.FieldUpdatedAt:   float64(record.UpdatedAt),
			})
		}
	}

	return results, nil
}

// handleRelationQuery handles relation queries
func (c *MockClient) handleRelationQuery(query string, relationType string) (any, error) {
	var results []any

	for _, relation := range c.relations {
		if string(relation.Type) == relationType {
			results = append(results, map[string]any{
				graph.FieldID:        relation.ID,
				graph.FieldIn:        relation.FromID,
				graph.FieldOut:       relation.ToID,
				graph.FieldUpdatedAt: float64(relation.UpdatedAt.UnixMilli()),
			})
		}
	}

	return results, nil
}

// handleResourceQuery handles resource queries
func (c *MockClient) handleResourceQuery(query string, resourceType string) (any, error) {
	var results []any

	for _, resource := range c.resources {
		if string(resource.Type) == resourceType {
			result := map[string]any{
				graph.FieldID:        resource.ID,
				graph.FieldUpdatedAt: float64(resource.UpdatedAt.UnixMilli()),
			}
			if resource.CreatedAt != nil {
				result[graph.FieldCreatedAt] = float64(resource.CreatedAt.UnixMilli())
			}
			for k, v := range resource.Labels {
				result[k] = v
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// =============================================================================
// Test data generators
// =============================================================================

// GenerateMockPod generates a mock pod resource
func GenerateMockPod(clusterID, namespace, podName string) *graph.Resource {
	now := time.Now()
	labels := map[string]string{
		"bcs_cluster_id": clusterID,
		"namespace":      namespace,
		"pod":            podName,
	}
	return &graph.Resource{
		ID:        graph.GenerateResourceID(graph.ResourceTypePod, labels),
		Type:      graph.ResourceTypePod,
		Labels:    labels,
		CreatedAt: &now,
		UpdatedAt: now,
	}
}

// GenerateMockNode generates a mock node resource
func GenerateMockNode(clusterID, nodeName string) *graph.Resource {
	now := time.Now()
	labels := map[string]string{
		"bcs_cluster_id": clusterID,
		"node":           nodeName,
	}
	return &graph.Resource{
		ID:        graph.GenerateResourceID(graph.ResourceTypeNode, labels),
		Type:      graph.ResourceTypeNode,
		Labels:    labels,
		CreatedAt: &now,
		UpdatedAt: now,
	}
}

// GenerateMockService generates a mock service resource
func GenerateMockService(clusterID, namespace, serviceName string) *graph.Resource {
	now := time.Now()
	labels := map[string]string{
		"bcs_cluster_id": clusterID,
		"namespace":      namespace,
		"service":        serviceName,
	}
	return &graph.Resource{
		ID:        graph.GenerateResourceID(graph.ResourceTypeService, labels),
		Type:      graph.ResourceTypeService,
		Labels:    labels,
		CreatedAt: &now,
		UpdatedAt: now,
	}
}

// GenerateMockRelation generates a mock relation
func GenerateMockRelation(relationType graph.RelationType, fromResource, toResource *graph.Resource) *graph.Relation {
	now := time.Now()
	return &graph.Relation{
		ID:        graph.GenerateRelationID(relationType, fromResource.ID, toResource.ID),
		Type:      relationType,
		FromID:    fromResource.ID,
		ToID:      toResource.ID,
		CreatedAt: &now,
		UpdatedAt: now,
	}
}

// GenerateMockLivenessRecord generates a mock liveness record
func GenerateMockLivenessRecord(resourceID string, periodStart, periodEnd int64, isActive bool) *graph.LivenessRecord {
	now := time.Now().UnixMilli()
	return &graph.LivenessRecord{
		ID:          fmt.Sprintf("liveness:%s:%d", resourceID, periodStart),
		ResourceID:  resourceID,
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		IsActive:    isActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// =============================================================================
// Test scenario builders
// =============================================================================

// BuildK8sClusterScenario builds a mock K8s cluster scenario with nodes, pods, and services
func BuildK8sClusterScenario(client *MockClient, clusterID string, numNodes, numPods, numServices int) {
	// Generate nodes
	nodes := make([]*graph.Resource, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = GenerateMockNode(clusterID, fmt.Sprintf("node-%d", i))
		client.AddResource(nodes[i])
	}

	// Generate pods and assign to nodes
	pods := make([]*graph.Resource, numPods)
	for i := 0; i < numPods; i++ {
		pods[i] = GenerateMockPod(clusterID, "default", fmt.Sprintf("pod-%d", i))
		client.AddResource(pods[i])

		// Create node-pod relation
		nodeIdx := i % numNodes
		relation := GenerateMockRelation(graph.RelationNodeWithPod, nodes[nodeIdx], pods[i])
		client.AddRelation(relation)

		// Add liveness record
		now := time.Now().UnixMilli()
		record := GenerateMockLivenessRecord(pods[i].ID, now-3600000, now, true)
		client.AddLivenessRecord(pods[i].ID, record)
	}

	// Generate services and assign pods
	for i := 0; i < numServices; i++ {
		service := GenerateMockService(clusterID, "default", fmt.Sprintf("service-%d", i))
		client.AddResource(service)

		// Create pod-service relations
		for j := 0; j < numPods/numServices; j++ {
			podIdx := i*(numPods/numServices) + j
			if podIdx < numPods {
				relation := GenerateMockRelation(graph.RelationPodWithService, pods[podIdx], service)
				client.AddRelation(relation)
			}
		}
	}
}
