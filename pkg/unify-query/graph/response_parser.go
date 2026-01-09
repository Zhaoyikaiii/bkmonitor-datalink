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

// RelatedResource 关联资源结果，确保边和目标节点的确定性配对
type RelatedResource struct {
	Edge     *EdgeLiveness // 边的 liveness
	TargetID string        // 目标节点 ID
}

// ParseRelatedResources 解析关系查询结果
// 返回确定性配对的边和目标节点ID列表
func (p *ResponseParser) ParseRelatedResources(
	result any,
	relationType RelationType,
	direction TraversalDirection,
	queryStart, queryEnd int64,
) ([]*RelatedResource, error) {
	data, ok := result.([]any)
	if !ok {
		return nil, nil
	}

	// 按关系ID分组，合并同一关系的多个时间段
	edgeMap := make(map[string]*RelatedResource)

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

		if relationID == "" {
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

		if targetID == "" {
			continue
		}

		// 合并边的时间段，保持与目标节点的配对
		if rr, exists := edgeMap[relationID]; exists {
			rr.Edge.RawPeriods = append(rr.Edge.RawPeriods, period)
		} else {
			edgeMap[relationID] = &RelatedResource{
				Edge: &EdgeLiveness{
					RelationID:   relationID,
					RelationType: relationType,
					FromID:       fromID,
					ToID:         toID,
					RawPeriods:   []*VisiblePeriod{period},
				},
				TargetID: targetID,
			}
		}
	}

	// 转换为切片（顺序不重要，因为每个元素自包含配对信息）
	results := make([]*RelatedResource, 0, len(edgeMap))
	for _, rr := range edgeMap {
		results = append(results, rr)
	}

	return results, nil
}
