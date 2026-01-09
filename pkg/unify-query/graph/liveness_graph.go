// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package graph

// ========================================
// LivenessGraph 中间结构
// ========================================

// LivenessGraph 存活图
// 包含查询涉及的所有资源和关联的 liveness 信息
// 用于构建时间段感知的关联查询结果
//
// 核心流程：QueryRequest -> LivenessGraph
//  1. 查询所有涉及资源的 liveness（裁剪到查询范围）
//  2. 查询所有涉及关联的 liveness（静态+动态，裁剪到查询范围）
//  3. 计算有效时间段：子节点的 liveness 必须与父节点重叠
type LivenessGraph struct {
	// 查询时间范围
	QueryStart int64 `json:"query_start"`
	QueryEnd   int64 `json:"query_end"`

	// 节点：资源ID -> 资源存活信息
	Nodes map[string]*NodeLiveness `json:"nodes"`

	// 边：关联ID -> 关联存活信息
	Edges map[string]*EdgeLiveness `json:"edges"`

	// 邻接表：资源ID -> 关联ID列表（出边）
	Adjacency map[string][]string `json:"adjacency"`
}

// NodeLiveness 节点存活信息
type NodeLiveness struct {
	ResourceID   string            `json:"resource_id"`
	ResourceType ResourceType      `json:"resource_type"`
	Labels       map[string]string `json:"labels,omitempty"`

	// 原始存活时间段（从数据库查询，已裁剪到查询范围）
	RawPeriods []*VisiblePeriod `json:"raw_periods"`

	// 有效可见时间段（与父节点重叠后的结果）
	// 如果是根节点，EffectivePeriods = RawPeriods
	EffectivePeriods []*VisiblePeriod `json:"effective_periods"`
}

// EdgeLiveness 边存活信息
type EdgeLiveness struct {
	RelationID   string             `json:"relation_id"`
	RelationType RelationType       `json:"relation_type"`
	Category     RelationCategory   `json:"category"`
	Direction    TraversalDirection `json:"direction,omitempty"`

	// 源节点和目标节点
	FromID string `json:"from_id"`
	ToID   string `json:"to_id"`

	// 原始存活时间段（从数据库查询，已裁剪到查询范围）
	RawPeriods []*VisiblePeriod `json:"raw_periods"`

	// 有效可见时间段 = RawPeriods ∩ FromNode.EffectivePeriods ∩ ToNode.RawPeriods
	EffectivePeriods []*VisiblePeriod `json:"effective_periods"`
}

// ========================================
// LivenessGraph 构造和基础方法
// ========================================

// NewLivenessGraph 创建空的 LivenessGraph
func NewLivenessGraph(queryStart, queryEnd int64) *LivenessGraph {
	return &LivenessGraph{
		QueryStart: queryStart,
		QueryEnd:   queryEnd,
		Nodes:      make(map[string]*NodeLiveness),
		Edges:      make(map[string]*EdgeLiveness),
		Adjacency:  make(map[string][]string),
	}
}

// AddNode 添加节点
func (g *LivenessGraph) AddNode(node *NodeLiveness) {
	g.Nodes[node.ResourceID] = node
	if _, exists := g.Adjacency[node.ResourceID]; !exists {
		g.Adjacency[node.ResourceID] = []string{}
	}
}

// AddEdge 添加边
func (g *LivenessGraph) AddEdge(edge *EdgeLiveness) {
	g.Edges[edge.RelationID] = edge
	g.Adjacency[edge.FromID] = append(g.Adjacency[edge.FromID], edge.RelationID)
}

// GetNode 获取节点
func (g *LivenessGraph) GetNode(resourceID string) *NodeLiveness {
	return g.Nodes[resourceID]
}

// GetEdge 获取边
func (g *LivenessGraph) GetEdge(relationID string) *EdgeLiveness {
	return g.Edges[relationID]
}

// GetOutEdges 获取节点的出边
func (g *LivenessGraph) GetOutEdges(resourceID string) []*EdgeLiveness {
	relationIDs := g.Adjacency[resourceID]
	edges := make([]*EdgeLiveness, 0, len(relationIDs))
	for _, rid := range relationIDs {
		if edge := g.Edges[rid]; edge != nil {
			edges = append(edges, edge)
		}
	}
	return edges
}

// ========================================
// 有效时间段计算
// ========================================

// ComputeEffectivePeriods 计算有效可见时间段
// 从根节点开始，BFS 遍历计算每个节点和边的有效可见时间段
// 规则：
//   - 根节点：EffectivePeriods = RawPeriods
//   - 边：EffectivePeriods = RawPeriods ∩ FromNode.EffectivePeriods ∩ ToNode.RawPeriods
//   - 子节点：EffectivePeriods = 边的 EffectivePeriods（继承自父边）
func (g *LivenessGraph) ComputeEffectivePeriods(rootID string) {
	root := g.Nodes[rootID]
	if root == nil {
		return
	}

	// 根节点的有效时间段 = 原始时间段
	root.EffectivePeriods = root.RawPeriods

	// BFS 遍历计算
	visited := make(map[string]bool)
	queue := []string{rootID}
	visited[rootID] = true

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		currentNode := g.Nodes[currentID]
		if currentNode == nil {
			continue
		}

		// 遍历当前节点的所有出边
		for _, relationID := range g.Adjacency[currentID] {
			edge := g.Edges[relationID]
			if edge == nil {
				continue
			}

			targetNode := g.Nodes[edge.ToID]
			if targetNode == nil {
				continue
			}

			// 计算边的有效时间段
			// = 边的原始时间段 ∩ 源节点有效时间段 ∩ 目标节点原始时间段
			edge.EffectivePeriods = ComputeOverlapPeriods(
				edge.RawPeriods,
				currentNode.EffectivePeriods,
				targetNode.RawPeriods,
			)

			// 如果目标节点未访问过，设置其有效时间段并加入队列
			if !visited[edge.ToID] {
				visited[edge.ToID] = true
				// 目标节点的有效时间段 = 边的有效时间段
				targetNode.EffectivePeriods = edge.EffectivePeriods
				queue = append(queue, edge.ToID)
			}
		}
	}
}

// ComputeOverlapPeriods 计算多个时间段列表的交集
func ComputeOverlapPeriods(periodLists ...[]*VisiblePeriod) []*VisiblePeriod {
	if len(periodLists) == 0 {
		return nil
	}

	// 从第一个列表开始
	result := periodLists[0]

	// 依次与后续列表求交集
	for i := 1; i < len(periodLists); i++ {
		result = OverlapTwoPeriodLists(result, periodLists[i])
		if len(result) == 0 {
			return nil
		}
	}

	return result
}

// OverlapTwoPeriodLists 计算两个时间段列表的交集
func OverlapTwoPeriodLists(list1, list2 []*VisiblePeriod) []*VisiblePeriod {
	var result []*VisiblePeriod

	for _, p1 := range list1 {
		for _, p2 := range list2 {
			if overlap := p1.Overlap(p2); overlap != nil {
				result = append(result, overlap)
			}
		}
	}

	return result
}
