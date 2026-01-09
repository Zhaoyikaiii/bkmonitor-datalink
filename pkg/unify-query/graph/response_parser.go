// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package graph

// ResponseParser 解析数据库响应
type ResponseParser struct{}

// NewResponseParser 创建响应解析器
func NewResponseParser() *ResponseParser {
	return &ResponseParser{}
}

// ParseLivenessRecords 解析存活记录列表
func (p *ResponseParser) ParseLivenessRecords(result any) ([]*LivenessRecord, error) {
	data, ok := result.([]any)
	if !ok {
		return []*LivenessRecord{}, nil
	}

	records := make([]*LivenessRecord, 0, len(data))
	for _, item := range data {
		itemMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		record := &LivenessRecord{}

		if id, ok := itemMap[FieldID].(string); ok {
			record.ID = id
		}

		if periodStart, ok := itemMap[FieldPeriodStart].(float64); ok {
			record.PeriodStart = int64(periodStart)
		}

		if periodEnd, ok := itemMap[FieldPeriodEnd].(float64); ok {
			record.PeriodEnd = int64(periodEnd)
		}

		if isActive, ok := itemMap[FieldIsActive].(bool); ok {
			record.IsActive = isActive
		}

		if createdAt, ok := itemMap[FieldCreatedAt].(float64); ok {
			record.CreatedAt = int64(createdAt)
		}

		if updatedAt, ok := itemMap[FieldUpdatedAt].(float64); ok {
			record.UpdatedAt = int64(updatedAt)
		}

		records = append(records, record)
	}

	return records, nil
}

// ParseRelatedResources 解析关系查询结果，返回边和目标节点列表
// 边和目标节点一一对应
func (p *ResponseParser) ParseRelatedResources(
	result any,
	relationType RelationType,
	direction TraversalDirection,
	queryStart, queryEnd int64,
) ([]*EdgeLiveness, []*NodeLiveness, error) {
	data, ok := result.([]any)
	if !ok {
		return nil, nil, nil
	}

	// 按目标资源ID分组，合并同一资源的多个时间段
	edgeMap := make(map[string]*EdgeLiveness)
	nodeMap := make(map[string]*NodeLiveness)

	for _, item := range data {
		itemMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		// 解析关系记录
		relationID, _ := itemMap["relation_id"].(string)
		fromID, _ := itemMap["from_id"].(string)
		toID, _ := itemMap["to_id"].(string)
		periodStart, _ := itemMap[FieldPeriodStart].(float64)
		periodEnd, _ := itemMap[FieldPeriodEnd].(float64)

		if relationID == "" || toID == "" {
			continue
		}

		// 裁剪时间段到查询范围
		visibleStart := int64(periodStart)
		if queryStart > visibleStart {
			visibleStart = queryStart
		}
		visibleEnd := int64(periodEnd)
		if queryEnd < visibleEnd {
			visibleEnd = queryEnd
		}
		if visibleStart > visibleEnd {
			continue // 无交集
		}

		period := &VisiblePeriod{Start: visibleStart, End: visibleEnd}

		// 确定目标节点ID
		var targetID string
		if direction == DirectionOutbound {
			targetID = toID
		} else {
			targetID = fromID
		}

		// 合并边的时间段
		if edge, exists := edgeMap[relationID]; exists {
			edge.RawPeriods = append(edge.RawPeriods, period)
		} else {
			edgeMap[relationID] = &EdgeLiveness{
				RelationID:   relationID,
				RelationType: relationType,
				FromID:       fromID,
				ToID:         toID,
				RawPeriods:   []*VisiblePeriod{period},
			}
		}

		// 合并目标节点的时间段（关系的 liveness 作为目标节点的初始 liveness）
		if node, exists := nodeMap[targetID]; exists {
			node.RawPeriods = append(node.RawPeriods, period)
		} else {
			nodeMap[targetID] = &NodeLiveness{
				ResourceID: targetID,
				RawPeriods: []*VisiblePeriod{period},
			}
		}
	}

	// 转换为切片
	edges := make([]*EdgeLiveness, 0, len(edgeMap))
	nodes := make([]*NodeLiveness, 0, len(nodeMap))

	for _, edge := range edgeMap {
		edges = append(edges, edge)
	}
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}

	return edges, nodes, nil
}
