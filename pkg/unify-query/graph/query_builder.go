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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/tsdb"
)

type QueryBuilder struct {
	tolerance time.Duration
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		tolerance: tsdb.GraphTolerance,
	}
}

func (b *QueryBuilder) SetTolerance(d time.Duration) {
	b.tolerance = d
}

func (b *QueryBuilder) BuildSourceEntityQuery(req *HopQueryRequest) string {
	tableName := string(req.SourceType)
	sourceID := GenerateResourceID(req.SourceType, req.SourceInfo)
	return fmt.Sprintf("SELECT * FROM %s WHERE id = '%s'", tableName, sourceID)
}

// BuildSingleHopQuery 构建单跳关联查询
func (b *QueryBuilder) BuildSingleHopQuery(req *HopQueryRequest) string {
	// Calculate the tolerance time for validity check
	toleranceMs := b.parseLookBackDelta(req.LookBackDelta)
	validityTime := req.Timestamp - toleranceMs

	sourceID := GenerateResourceID(req.SourceType, req.SourceInfo)

	// Determine which relation types to use
	allowedCategories := b.getAllowedCategories(req.AllowedRelationTypes)

	var queries []string

	// For single-hop query with target type
	if req.TargetType != "" {
		// Get static relations if allowed
		if b.containsCategory(allowedCategories, RelationCategoryStatic) {
			staticRelations := GetStaticRelationsBetween(req.SourceType, req.TargetType)
			for _, relType := range staticRelations {
				query := b.buildRelationTraversalQuery(sourceID, relType, req.TargetType, validityTime, DirectionBoth)
				queries = append(queries, query)
			}
		}

		// Get dynamic relations if allowed
		if b.containsCategory(allowedCategories, RelationCategoryDynamic) {
			dynamicRelations := GetDynamicRelationsBetween(req.SourceType, req.TargetType)
			direction := TraversalDirection(req.DynamicRelationDirection)
			if direction == "" {
				direction = DirectionBoth
			}
			for _, relType := range dynamicRelations {
				query := b.buildRelationTraversalQuery(sourceID, relType, req.TargetType, validityTime, direction)
				queries = append(queries, query)
			}
		}
	}

	if len(queries) == 0 {
		// If no specific relations found, do a general traversal
		return b.buildGeneralTraversalQuery(sourceID, req.SourceType, validityTime, 1)
	}

	// Combine queries with UNION or execute separately
	return strings.Join(queries, ";\n")
}

// buildRelationTraversalQuery 构建特定关系类型的遍历查询
func (b *QueryBuilder) buildRelationTraversalQuery(sourceID string, relationType RelationType, _ ResourceType, validityTime int64, direction TraversalDirection) string {
	relationTable := string(relationType)

	// Determine the direction of traversal
	var directionClause string
	switch direction {
	case DirectionOutbound:
		directionClause = fmt.Sprintf("%s = '%s'", FieldIn, sourceID)
	case DirectionInbound:
		directionClause = fmt.Sprintf("%s = '%s'", FieldOut, sourceID)
	default: // DirectionBoth
		directionClause = fmt.Sprintf("(%s = '%s' OR %s = '%s')", FieldIn, sourceID, FieldOut, sourceID)
	}

	// Query relations with time validity check
	query := fmt.Sprintf(`
SELECT 
    %s as relation_id,
    '%s' as %s,
    %s as %s,
    %s as %s,
    %s,
    %s
FROM %s 
WHERE %s 
AND %s >= %d
`, FieldID, relationType, FieldRelationType, FieldIn, FieldFromID, FieldOut, FieldToID,
		FieldCreatedAt, FieldUpdatedAt, relationTable, directionClause, FieldUpdatedAt, validityTime)

	return query
}

// TraversalConfig 通用遍历配置
type TraversalConfig struct {
	SourceID             string             // 遍历起点
	SourceType           ResourceType       // 源资源类型
	ValidityTime         int64              // 时间有效性过滤的最小 updated_at
	MaxHops              int                // 最大遍历深度
	AllowedCategories    []RelationCategory // 允许的关系类别
	AllowedRelationTypes []RelationType     // 允许的关系类型
	Direction            TraversalDirection // 遍历方向
	IncludeEntityData    bool               // 是否包含完整实体数据
}

// NewTraversalConfig 创建默认遍历配置
// validityTime 为 updated_at 的最小值
func NewTraversalConfig(sourceID string, sourceType ResourceType, validityTime int64) *TraversalConfig {
	return &TraversalConfig{
		SourceID:          sourceID,
		SourceType:        sourceType,
		ValidityTime:      validityTime,
		MaxHops:           1,
		AllowedCategories: nil, // all categories
		Direction:         DirectionBoth,
		IncludeEntityData: false,
	}
}

// buildGeneralTraversalQuery 构建通用图遍历查询
func (b *QueryBuilder) buildGeneralTraversalQuery(sourceID string, sourceType ResourceType, validityTime int64, maxHops int) string {
	cfg := NewTraversalConfig(sourceID, sourceType, validityTime)
	cfg.MaxHops = maxHops
	return b.BuildGeneralTraversal(cfg)
}

// BuildGeneralTraversal 根据配置构建通用图遍历查询
func (b *QueryBuilder) BuildGeneralTraversal(cfg *TraversalConfig) string {
	if cfg == nil {
		return ""
	}

	// Get all applicable relation types for the source type
	relationTypes := b.getApplicableRelationTypes(cfg)
	if len(relationTypes) == 0 {
		return ""
	}

	// Build the traversal query using dynamic relation discovery
	return b.buildDynamicTraversalQuery(cfg, relationTypes)
}

// getApplicableRelationTypes 获取适用的关系类型
func (b *QueryBuilder) getApplicableRelationTypes(cfg *TraversalConfig) []RelationType {
	// If specific relation types are provided, use them
	if len(cfg.AllowedRelationTypes) > 0 {
		return b.filterRelationsByCategory(cfg.AllowedRelationTypes, cfg.AllowedCategories)
	}

	// Otherwise, discover all relation types connected to the source type
	var allRelations []RelationType

	// Get all registered relation types from the schema registry
	registeredRelations := GetAllRelationTypesForResource(cfg.SourceType)
	allRelations = append(allRelations, registeredRelations...)

	// Filter by allowed categories
	return b.filterRelationsByCategory(allRelations, cfg.AllowedCategories)
}

// filterRelationsByCategory 按类别过滤关系类型
func (b *QueryBuilder) filterRelationsByCategory(relations []RelationType, allowedCategories []RelationCategory) []RelationType {
	if len(allowedCategories) == 0 {
		return relations
	}

	categorySet := make(map[RelationCategory]bool)
	for _, cat := range allowedCategories {
		categorySet[cat] = true
	}

	var filtered []RelationType
	for _, rel := range relations {
		if categorySet[GetRelationCategory(rel)] {
			filtered = append(filtered, rel)
		}
	}
	return filtered
}

// buildDynamicTraversalQuery 构建多关系类型的遍历查询
func (b *QueryBuilder) buildDynamicTraversalQuery(cfg *TraversalConfig, relationTypes []RelationType) string {
	if len(relationTypes) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("-- Dynamic graph traversal query\n")
	sb.WriteString("-- Generated for source: " + cfg.SourceID + "\n")
	sb.WriteString("SELECT * FROM (\n")

	for i, relType := range relationTypes {
		if i > 0 {
			sb.WriteString("    UNION ALL\n")
		}
		sb.WriteString(b.buildRelationSubquery(cfg, relType))
	}

	sb.WriteString(")")

	return sb.String()
}

// buildRelationSubquery 构建单个关系类型的子查询
func (b *QueryBuilder) buildRelationSubquery(cfg *TraversalConfig, relationType RelationType) string {
	tableName := string(relationType)
	directionClause := b.buildDirectionClause(cfg.SourceID, cfg.Direction)

	return fmt.Sprintf(`    SELECT 
        %s,
        '%s' as %s,
        %s as %s,
        %s as %s,
        %s,
        %s
    FROM %s 
    WHERE %s 
    AND %s >= %d
`, FieldID, relationType, FieldRelationType, FieldIn, FieldFromID, FieldOut, FieldToID,
		FieldCreatedAt, FieldUpdatedAt, tableName, directionClause, FieldUpdatedAt, cfg.ValidityTime)
}

// buildDirectionClause 构建遍历方向的 WHERE 子句
func (b *QueryBuilder) buildDirectionClause(sourceID string, direction TraversalDirection) string {
	switch direction {
	case DirectionOutbound:
		return fmt.Sprintf("%s = '%s'", FieldIn, sourceID)
	case DirectionInbound:
		return fmt.Sprintf("%s = '%s'", FieldOut, sourceID)
	default: // DirectionBoth
		return fmt.Sprintf("(%s = '%s' OR %s = '%s')", FieldIn, sourceID, FieldOut, sourceID)
	}
}

// BuildMultiHopTraversal 构建多跳遍历查询
func (b *QueryBuilder) BuildMultiHopTraversal(cfg *TraversalConfig) string {
	if cfg == nil || cfg.MaxHops < 1 {
		return ""
	}

	// Get applicable relation types
	relationTypes := b.getApplicableRelationTypes(cfg)
	if len(relationTypes) == 0 {
		return ""
	}

	// Build relation type list for graph traversal
	relationTables := make([]string, len(relationTypes))
	for i, rt := range relationTypes {
		relationTables[i] = string(rt)
	}

	// Construct the graph traversal expression
	// SurrealDB syntax: SELECT * FROM source_id->(relation1, relation2, ...)->*
	var sb strings.Builder
	sb.WriteString("-- Multi-hop graph traversal\n")

	// Build the traversal path based on direction
	relationList := strings.Join(relationTables, ", ")
	switch cfg.Direction {
	case DirectionOutbound:
		sb.WriteString(fmt.Sprintf("SELECT * FROM '%s'->(%s)", cfg.SourceID, relationList))
	case DirectionInbound:
		sb.WriteString(fmt.Sprintf("SELECT * FROM '%s'<-(%s)", cfg.SourceID, relationList))
	default: // DirectionBoth
		sb.WriteString(fmt.Sprintf("SELECT * FROM '%s'<->(%s)", cfg.SourceID, relationList))
	}

	// Add depth limit
	if cfg.MaxHops > 1 {
		sb.WriteString(fmt.Sprintf(" DEPTH 1..%d", cfg.MaxHops))
	}

	// Add time validity filter
	sb.WriteString(fmt.Sprintf(" WHERE %s >= %d", FieldUpdatedAt, cfg.ValidityTime))

	return sb.String()
}

// BuildLivenessQuery 构建存活记录查询
func (b *QueryBuilder) BuildLivenessQuery(resourceType ResourceType, resourceID string, startTime, endTime int64) string {
	tableName := GetLivenessTableName(resourceType)
	fieldName := string(resourceType) + "_id"

	return fmt.Sprintf(`
SELECT * FROM %s 
WHERE %s = '%s' 
AND period_start <= %d 
AND period_end >= %d
ORDER BY period_start ASC
`, tableName, fieldName, resourceID, endTime, startTime)
}

// BuildResourceQuery 构建资源查询
func (b *QueryBuilder) BuildResourceQuery(req *ResourceQueryRequest) string {
	tableName := string(req.ResourceType)
	livenessTable := GetLivenessTableName(req.ResourceType)

	startMs := req.StartTime.UnixMilli()
	endMs := req.EndTime.UnixMilli()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`
SELECT * FROM %s WHERE id IN (
    SELECT %s_id FROM %s 
    WHERE period_start <= %d AND period_end >= %d
)
`, tableName, req.ResourceType, livenessTable, endMs, startMs))

	// Add label filters
	for k, v := range req.Labels {
		sb.WriteString(fmt.Sprintf(" AND %s = '%s'", k, v))
	}

	// Add pagination
	if req.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", req.Limit))
	}
	if req.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" START %d", req.Offset))
	}

	return sb.String()
}

// BuildRelationQuery 构建关系查询
func (b *QueryBuilder) BuildRelationQuery(req *RelationQueryRequest) string {
	if req.RelationType == "" {
		return ""
	}

	tableName := string(req.RelationType)
	livenessTable := GetRelationLivenessTableName(req.RelationType)

	startMs := req.StartTime.UnixMilli()
	endMs := req.EndTime.UnixMilli()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`
SELECT * FROM %s WHERE id IN (
    SELECT relation_id FROM %s 
    WHERE period_start <= %d AND period_end >= %d
)
`, tableName, livenessTable, endMs, startMs))

	if req.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", req.Limit))
	}
	if req.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" START %d", req.Offset))
	}

	return sb.String()
}

// parseLookBackDelta 解析回溯时间窗口字符串，返回毫秒数
func (b *QueryBuilder) parseLookBackDelta(delta string) int64 {
	if delta == "" {
		return b.tolerance.Milliseconds()
	}

	d, err := time.ParseDuration(delta)
	if err != nil {
		return b.tolerance.Milliseconds()
	}

	return d.Milliseconds()
}

// getAllowedCategories 获取允许的关系类别
func (b *QueryBuilder) getAllowedCategories(allowed []RelationCategory) []RelationCategory {
	if len(allowed) == 0 {
		return []RelationCategory{RelationCategoryStatic, RelationCategoryDynamic}
	}
	return allowed
}

// containsCategory 检查类别是否在列表中
func (b *QueryBuilder) containsCategory(categories []RelationCategory, target RelationCategory) bool {
	for _, c := range categories {
		if c == target {
			return true
		}
	}
	return false
}

// BuildSourceInfoFilter 构建源信息过滤条件
func (b *QueryBuilder) BuildSourceInfoFilter(sourceType ResourceType, sourceInfo map[string]string) string {
	if len(sourceInfo) == 0 {
		return ""
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(sourceInfo))
	for k := range sourceInfo {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var filters []string
	for _, k := range keys {
		filters = append(filters, fmt.Sprintf("%s = '%s'", k, sourceInfo[k]))
	}

	return strings.Join(filters, " AND ")
}
