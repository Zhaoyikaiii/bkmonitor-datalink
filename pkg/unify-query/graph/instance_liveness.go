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
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

// ========================================
// Liveness 查询方法
// ========================================

// GetLivenessRecords 获取资源的存活记录
func (i *Instance) GetLivenessRecords(ctx context.Context, resourceID string, startTime, endTime int64) ([]*LivenessRecord, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-get-liveness-records")
	defer span.End(&err)

	span.Set("resource-id", resourceID)
	span.Set("start-time", startTime)
	span.Set("end-time", endTime)

	resourceType, _, err := ParseResourceID(resourceID)
	if err != nil {
		return nil, err
	}

	query := i.builder.BuildLivenessQuery(resourceType, resourceID, startTime, endTime)
	span.Set("query", query)

	result, err := i.client.Execute(ctx, query, nil)
	if err != nil {
		return nil, err
	}

	records, err := i.parser.ParseLivenessRecords(result)
	if err != nil {
		return nil, err
	}

	span.Set("result-count", len(records))
	return records, nil
}

// GetVisiblePeriods 获取资源在查询时间范围内的可见时间段
// 返回的时间段是查询范围与资源存活周期的交集，裁剪掉查询范围外的部分
func (i *Instance) GetVisiblePeriods(ctx context.Context, resourceID string, queryStart, queryEnd int64) ([]*VisiblePeriod, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-get-visible-periods")
	defer span.End(&err)

	span.Set("resource-id", resourceID)
	span.Set("query-start", queryStart)
	span.Set("query-end", queryEnd)

	records, err := i.GetLivenessRecords(ctx, resourceID, queryStart, queryEnd)
	if err != nil {
		return nil, err
	}

	periods := make([]*VisiblePeriod, 0, len(records))
	for _, record := range records {
		// 计算交集：取两个区间的重叠部分
		// 可见开始时间 = max(period_start, query_start)
		// 可见结束时间 = min(period_end, query_end)
		visibleStart := record.PeriodStart
		if queryStart > visibleStart {
			visibleStart = queryStart
		}

		visibleEnd := record.PeriodEnd
		if queryEnd < visibleEnd {
			visibleEnd = queryEnd
		}

		periods = append(periods, &VisiblePeriod{
			Start: visibleStart,
			End:   visibleEnd,
		})
	}

	span.Set("visible-periods-count", len(periods))
	return periods, nil
}

// ========================================
// LivenessGraph 构建核心方法
// ========================================

// BuildLivenessGraph 从查询请求构建 LivenessGraph
// 核心流程：
//  1. 从 SourceInfo 确定根节点，查询其 liveness
//  2. BFS 遍历到 TargetType，查询路径上所有资源的 liveness
//  3. 查询路径上所有关系（静态+动态）的 liveness
//  4. ComputeEffectivePeriods() 计算交集
func (i *Instance) BuildLivenessGraph(ctx context.Context, req *QueryRequest) (*LivenessGraph, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-build-liveness-graph")
	defer span.End(&err)

	// 1. 计算查询时间范围
	queryStart, queryEnd := req.GetQueryRange()
	span.Set("query-start", queryStart)
	span.Set("query-end", queryEnd)

	// 2. 创建 LivenessGraph
	graph := NewLivenessGraph(queryStart, queryEnd)

	// 3. 获取根节点 ID 和 liveness
	rootID := req.GetSourceResourceID()
	span.Set("root-id", rootID)

	rootPeriods, err := i.GetVisiblePeriods(ctx, rootID, queryStart, queryEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to get root liveness: %w", err)
	}

	// 如果根节点在查询时间范围内不可见，直接返回空图
	if len(rootPeriods) == 0 {
		span.Set("result", "root not visible")
		return graph, nil
	}

	// 添加根节点
	graph.AddNode(&NodeLiveness{
		ResourceID:   rootID,
		ResourceType: req.SourceType,
		Labels:       req.SourceInfo,
		RawPeriods:   rootPeriods,
	})

	// 4. BFS 遍历查找到 TargetType 的路径
	err = i.bfsTraversal(ctx, graph, req, rootID)
	if err != nil {
		return nil, fmt.Errorf("failed to traverse graph: %w", err)
	}

	// 5. 计算有效时间段（子节点与父节点的交集）
	graph.ComputeEffectivePeriods(rootID)

	span.Set("nodes-count", len(graph.Nodes))
	span.Set("edges-count", len(graph.Edges))
	return graph, nil
}

// bfsTraversal BFS 遍历图，查询所有涉及的资源和关系的 liveness
func (i *Instance) bfsTraversal(ctx context.Context, graph *LivenessGraph, req *QueryRequest, rootID string) error {
	queryStart := graph.QueryStart
	queryEnd := graph.QueryEnd
	maxHops := req.MaxHops
	if maxHops <= 0 {
		maxHops = 3 // 默认最大跳数
	}

	// BFS 队列：(resourceID, currentHop)
	type queueItem struct {
		resourceID string
		hop        int
	}
	queue := []queueItem{{rootID, 0}}
	visited := make(map[string]bool)
	visited[rootID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// 超过最大跳数，停止
		if current.hop >= maxHops {
			continue
		}

		currentNode := graph.GetNode(current.resourceID)
		if currentNode == nil {
			continue
		}

		// 如果已经到达目标类型，不再继续遍历
		if req.TargetType != "" && currentNode.ResourceType == req.TargetType {
			continue
		}

		// 获取当前资源类型可以遍历的关系
		relationTypes := GetAllRelationTypesForResource(currentNode.ResourceType)

		for _, relationType := range relationTypes {
			schema := GetRelationSchema(relationType)
			if schema == nil {
				continue
			}

			// 检查关系类别是否允许
			if !req.IsRelationCategoryAllowed(schema.Category) {
				continue
			}

			// 确定遍历方向和目标类型
			var targetType ResourceType
			var direction TraversalDirection
			if schema.FromType == currentNode.ResourceType {
				targetType = schema.ToType
				direction = DirectionOutbound
			} else {
				targetType = schema.FromType
				direction = DirectionInbound
			}

			// 对于动态关系，检查方向是否匹配
			if schema.Category == RelationCategoryDynamic {
				if req.DynamicRelationDirection != "" && req.DynamicRelationDirection != DirectionBoth {
					if req.DynamicRelationDirection != direction {
						continue
					}
				}
			}

			// 查询该关系类型下与当前资源相关的所有边和目标节点
			edges, targetNodes, err := i.queryRelatedResources(
				ctx, current.resourceID, relationType, direction, queryStart, queryEnd,
			)
			if err != nil {
				// 记录错误但继续遍历
				continue
			}

			// 添加边和目标节点到图中
			for idx, edge := range edges {
				if len(edge.RawPeriods) == 0 {
					continue // 关系在查询时间范围内不可见
				}

				targetNode := targetNodes[idx]
				if len(targetNode.RawPeriods) == 0 {
					continue // 目标资源在查询时间范围内不可见
				}

				// 设置边的方向和类别
				edge.Direction = direction
				edge.Category = schema.Category

				// 添加边
				graph.AddEdge(edge)

				// 如果目标节点未访问过，添加并加入队列
				if !visited[targetNode.ResourceID] {
					visited[targetNode.ResourceID] = true
					targetNode.ResourceType = targetType
					graph.AddNode(targetNode)
					queue = append(queue, queueItem{targetNode.ResourceID, current.hop + 1})
				}
			}
		}
	}

	return nil
}

// queryRelatedResources 查询与指定资源相关的所有边和目标节点
// 返回边列表和对应的目标节点列表（一一对应）
func (i *Instance) queryRelatedResources(
	ctx context.Context,
	resourceID string,
	relationType RelationType,
	direction TraversalDirection,
	queryStart, queryEnd int64,
) ([]*EdgeLiveness, []*NodeLiveness, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-query-related-resources")
	defer span.End(&err)

	span.Set("resource-id", resourceID)
	span.Set("relation-type", string(relationType))
	span.Set("direction", string(direction))

	// 构建查询：查找与 resourceID 相关的所有关系记录
	query := i.builder.BuildRelatedResourcesQuery(relationType, resourceID, direction, queryStart, queryEnd)
	span.Set("query", query)

	result, err := i.client.Execute(ctx, query, nil)
	if err != nil {
		return nil, nil, err
	}

	// 解析结果
	edges, targetNodes, err := i.parser.ParseRelatedResources(result, relationType, direction, queryStart, queryEnd)
	if err != nil {
		return nil, nil, err
	}

	span.Set("edges-count", len(edges))
	return edges, targetNodes, nil
}
