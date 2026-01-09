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
	"time"
)

// ResponseParser 解析 SurrealDB 响应
type ResponseParser struct{}

// NewResponseParser 创建响应解析器
func NewResponseParser() *ResponseParser {
	return &ResponseParser{}
}

// ParseResources 解析资源列表
func (p *ResponseParser) ParseResources(result any) ([]*Resource, error) {
	data, ok := result.([]any)
	if !ok {
		return []*Resource{}, nil
	}

	resources := make([]*Resource, 0, len(data))
	for _, item := range data {
		itemMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		resource := &Resource{
			Labels: make(map[string]string),
		}

		if id, ok := itemMap[FieldID].(string); ok {
			resource.ID = id
			resourceType, labels, err := ParseResourceID(id)
			if err == nil {
				resource.Type = resourceType
				resource.Labels = labels
			}
		}

		if updatedAt, ok := itemMap[FieldUpdatedAt].(float64); ok {
			t := time.UnixMilli(int64(updatedAt))
			resource.UpdatedAt = t
		}

		if createdAt, ok := itemMap[FieldCreatedAt].(float64); ok {
			t := time.UnixMilli(int64(createdAt))
			resource.CreatedAt = &t
		}

		resources = append(resources, resource)
	}

	return resources, nil
}

// ParseRelations 解析关系列表
func (p *ResponseParser) ParseRelations(result any) ([]*Relation, error) {
	data, ok := result.([]any)
	if !ok {
		return []*Relation{}, nil
	}

	relations := make([]*Relation, 0, len(data))
	for _, item := range data {
		itemMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		relation := &Relation{}

		if id, ok := itemMap[FieldID].(string); ok {
			relation.ID = id
		}

		// SurrealDB 用 "in" 表示源端，"out" 表示目标端
		if in, ok := itemMap[FieldIn].(string); ok {
			relation.FromID = in
		}

		if out, ok := itemMap[FieldOut].(string); ok {
			relation.ToID = out
		}

		if updatedAt, ok := itemMap[FieldUpdatedAt].(float64); ok {
			t := time.UnixMilli(int64(updatedAt))
			relation.UpdatedAt = t
		}

		if createdAt, ok := itemMap[FieldCreatedAt].(float64); ok {
			t := time.UnixMilli(int64(createdAt))
			relation.CreatedAt = &t
		}

		relations = append(relations, relation)
	}

	return relations, nil
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

// ParseSingleHopResponse 解析单跳查询响应
func (p *ResponseParser) ParseSingleHopResponse(result any, req *HopQueryRequest) (*HopQueryResponse, error) {
	resp := &HopQueryResponse{
		Timestamp:  req.Timestamp,
		SourceType: req.SourceType,
		SourceInfo: req.SourceInfo,
		TargetType: req.TargetType,
		MaxHops:    req.MaxHops,
	}

	// Parse relations from result
	relations, err := p.ParseRelations(result)
	if err != nil {
		return resp, err
	}

	// Build target list from relations
	targetMap := make(map[string]*TargetResult)
	sourceID := GenerateResourceID(req.SourceType, req.SourceInfo)

	for _, rel := range relations {
		// Determine target ID based on source
		var targetID string
		if rel.FromID == sourceID {
			targetID = rel.ToID
		} else {
			targetID = rel.FromID
		}

		if targetID == "" {
			continue
		}

		// Create or get target result
		target, ok := targetMap[targetID]
		if !ok {
			target = &TargetResult{
				Paths: []*PathResult{},
			}
			targetMap[targetID] = target
		}

		// Parse target entity info
		targetType, targetLabels, _ := ParseResourceID(targetID)

		// Build path
		path := &PathResult{
			PathID: rel.ID,
			Hops:   1,
			Path: []*PathElement{
				// Source entity
				{
					EntityID:   sourceID,
					EntityType: req.SourceType,
					EntityData: req.SourceInfo,
				},
				// Relation
				{
					RelationType: rel.Type,
					RelationID:   rel.ID,
					CreatedAt:    getTimestampMs(rel.CreatedAt),
					UpdatedAt:    rel.UpdatedAt.UnixMilli(),
				},
				// Target entity
				{
					EntityID:   targetID,
					EntityType: targetType,
					EntityData: targetLabels,
				},
			},
		}

		target.Paths = append(target.Paths, path)
	}

	// Convert map to slice
	for _, target := range targetMap {
		resp.TargetList = append(resp.TargetList, target)
	}

	resp.Total = int64(len(resp.TargetList))
	return resp, nil
}

// getTimestampMs 将时间指针转换为毫秒时间戳
func getTimestampMs(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}
