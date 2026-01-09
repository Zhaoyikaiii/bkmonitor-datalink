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

	// 遍历过程中遇到的错误（非致命错误，图可能不完整）
	// 调用者应检查此字段以判断图是否完整
	TraversalErrors []string `json:"traversal_errors,omitempty"`
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
// 如果边已存在（相同 RelationID），会更新边但不会重复添加到邻接表
func (g *LivenessGraph) AddEdge(edge *EdgeLiveness) {
	// 检查边是否已存在
	_, exists := g.Edges[edge.RelationID]
	g.Edges[edge.RelationID] = edge

	// 只有新边才添加到邻接表
	if !exists {
		g.Adjacency[edge.FromID] = append(g.Adjacency[edge.FromID], edge.RelationID)
	}
}

// GetNode 获取节点
func (g *LivenessGraph) GetNode(resourceID string) *NodeLiveness {
	return g.Nodes[resourceID]
}

// GetEdge 获取边
func (g *LivenessGraph) GetEdge(relationID string) *EdgeLiveness {
	return g.Edges[relationID]
}

// AddTraversalError 记录遍历过程中的错误
func (g *LivenessGraph) AddTraversalError(errMsg string) {
	g.TraversalErrors = append(g.TraversalErrors, errMsg)
}

// HasErrors 检查图是否有遍历错误
func (g *LivenessGraph) HasErrors() bool {
	return len(g.TraversalErrors) > 0
}

// IsComplete 检查图是否完整（没有遍历错误）
func (g *LivenessGraph) IsComplete() bool {
	return len(g.TraversalErrors) == 0
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
// 从根节点开始，迭代计算每个节点和边的有效可见时间段
// 规则：
//   - 根节点：EffectivePeriods = RawPeriods
//   - 边：EffectivePeriods = RawPeriods ∩ FromNode.EffectivePeriods ∩ ToNode.RawPeriods
//   - 子节点：EffectivePeriods = 所有入边的 EffectivePeriods 的并集（Union）
//
// 多路径处理：使用迭代收敛算法，确保所有入边都被考虑
// 当一个节点可通过多条边到达时，取所有边的 effective periods 的并集
//
// 注意：此算法假设图是从根节点可达的 DAG 或树结构
// 对于环形图，算法会在 maxIterations 次迭代后停止
func (g *LivenessGraph) ComputeEffectivePeriods(rootID string) {
	root := g.Nodes[rootID]
	if root == nil {
		return
	}

	// 根节点的有效时间段 = 原始时间段
	root.EffectivePeriods = root.RawPeriods

	// 构建反向邻接表：目标节点ID -> 入边列表
	inEdges := make(map[string][]*EdgeLiveness)
	for _, edge := range g.Edges {
		inEdges[edge.ToID] = append(inEdges[edge.ToID], edge)
	}

	// 最大迭代次数 = 节点数 + 1，足以让信息传播到所有可达节点
	// 对于 DAG，最多需要 |V| 次迭代；额外 1 次用于确认收敛
	maxIterations := len(g.Nodes) + 1
	if maxIterations < 2 {
		maxIterations = 2
	}

	// 迭代收敛算法：重复处理直到没有变化或达到最大迭代次数
	for iteration := 0; iteration < maxIterations; iteration++ {
		changed := false

		// 遍历所有边，更新边的 EffectivePeriods
		for _, edge := range g.Edges {
			fromNode := g.Nodes[edge.FromID]
			toNode := g.Nodes[edge.ToID]
			if fromNode == nil || toNode == nil {
				continue
			}

			// 计算边的有效时间段
			// = 边的原始时间段 ∩ 源节点有效时间段 ∩ 目标节点原始时间段
			// 注意：如果源节点的 EffectivePeriods 为空，结果也为空
			var newEdgePeriods []*VisiblePeriod
			if len(fromNode.EffectivePeriods) > 0 {
				newEdgePeriods = ComputeOverlapPeriods(
					edge.RawPeriods,
					fromNode.EffectivePeriods,
					toNode.RawPeriods,
				)
			}

			// 检查边的 EffectivePeriods 是否有变化
			if !periodsEqual(edge.EffectivePeriods, newEdgePeriods) {
				edge.EffectivePeriods = newEdgePeriods
				changed = true
			}
		}

		// 遍历所有非根节点，更新节点的 EffectivePeriods（所有入边的并集）
		for nodeID, node := range g.Nodes {
			if nodeID == rootID {
				continue
			}

			edges := inEdges[nodeID]
			var unionPeriods []*VisiblePeriod
			for _, edge := range edges {
				unionPeriods = UnionPeriodLists(unionPeriods, edge.EffectivePeriods)
			}

			// 规范化后比较，避免顺序敏感导致的假阳性变化检测
			normalizedUnion := normalizePeriods(unionPeriods)
			normalizedCurrent := normalizePeriods(node.EffectivePeriods)

			// 检查节点的 EffectivePeriods 是否有变化
			if !periodsEqual(normalizedCurrent, normalizedUnion) {
				node.EffectivePeriods = normalizedUnion
				changed = true
			}
		}

		// 如果没有任何变化，算法收敛，退出
		if !changed {
			break
		}
	}
}

// periodsEqual 检查两个时间段列表是否相等（顺序无关）
// 假设两个列表都已经过 normalizePeriods 处理（排序+合并）
func periodsEqual(a, b []*VisiblePeriod) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Start != b[i].Start || a[i].End != b[i].End {
			return false
		}
	}
	return true
}

// normalizePeriods 对时间段列表进行规范化：排序并合并重叠区间
// 返回一个新的规范化列表，确保比较时顺序一致
func normalizePeriods(periods []*VisiblePeriod) []*VisiblePeriod {
	if len(periods) == 0 {
		return nil
	}
	if len(periods) == 1 {
		return []*VisiblePeriod{{Start: periods[0].Start, End: periods[0].End}}
	}

	// 复制并按开始时间排序
	sorted := make([]*VisiblePeriod, len(periods))
	copy(sorted, periods)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].Start < sorted[i].Start ||
				(sorted[j].Start == sorted[i].Start && sorted[j].End < sorted[i].End) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// 合并重叠或相邻的时间段
	result := make([]*VisiblePeriod, 0, len(sorted))
	current := &VisiblePeriod{Start: sorted[0].Start, End: sorted[0].End}

	for i := 1; i < len(sorted); i++ {
		if sorted[i].Start <= current.End {
			// 重叠或相邻，扩展当前时间段
			if sorted[i].End > current.End {
				current.End = sorted[i].End
			}
		} else {
			// 不重叠，保存当前时间段，开始新的
			result = append(result, current)
			current = &VisiblePeriod{Start: sorted[i].Start, End: sorted[i].End}
		}
	}
	result = append(result, current)

	return result
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

// UnionPeriodLists 计算两个时间段列表的并集
// 结果会合并重叠或相邻的时间段
func UnionPeriodLists(list1, list2 []*VisiblePeriod) []*VisiblePeriod {
	if len(list1) == 0 {
		return list2
	}
	if len(list2) == 0 {
		return list1
	}

	// 合并两个列表
	all := make([]*VisiblePeriod, 0, len(list1)+len(list2))
	all = append(all, list1...)
	all = append(all, list2...)

	// 按开始时间排序
	for i := 0; i < len(all)-1; i++ {
		for j := i + 1; j < len(all); j++ {
			if all[j].Start < all[i].Start {
				all[i], all[j] = all[j], all[i]
			}
		}
	}

	// 合并重叠或相邻的时间段
	result := make([]*VisiblePeriod, 0, len(all))
	current := &VisiblePeriod{Start: all[0].Start, End: all[0].End}

	for i := 1; i < len(all); i++ {
		if all[i].Start <= current.End {
			// 重叠或相邻，扩展当前时间段
			if all[i].End > current.End {
				current.End = all[i].End
			}
		} else {
			// 不重叠，保存当前时间段，开始新的
			result = append(result, current)
			current = &VisiblePeriod{Start: all[i].Start, End: all[i].End}
		}
	}
	result = append(result, current)

	return result
}
