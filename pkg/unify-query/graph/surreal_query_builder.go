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
)

// SurrealQueryBuilder 构建 SurrealQL 关联查询
type SurrealQueryBuilder struct {
	request *QueryRequest
}

// NewSurrealQueryBuilder 创建查询构建器
func NewSurrealQueryBuilder(request *QueryRequest) *SurrealQueryBuilder {
	// 规范化请求参数
	request.Normalize()
	return &SurrealQueryBuilder{request: request}
}

// Build 构建完整的 SurrealQL 查询
func (b *SurrealQueryBuilder) Build() string {
	var sb strings.Builder

	// 1. 变量定义
	sb.WriteString(b.buildVariables())
	sb.WriteString("\n\n")

	// 2. 主查询
	sb.WriteString(b.buildMainQuery())

	return sb.String()
}

// buildVariables 构建变量定义部分
func (b *SurrealQueryBuilder) buildVariables() string {
	start, end := b.request.GetQueryRange()
	return fmt.Sprintf(`LET $timestamp = %d;
LET $look_back_delta = %d;
LET $start = %d;
LET $end = %d;`,
		b.request.Timestamp,
		b.request.LookBackDelta,
		start,
		end)
}

// buildMainQuery 构建主查询
func (b *SurrealQueryBuilder) buildMainQuery() string {
	var sb strings.Builder

	// SELECT 子句
	sb.WriteString("SELECT {\n")

	// Root 部分
	sb.WriteString("    root: ")
	sb.WriteString(b.buildRootSelect())
	sb.WriteString(",\n\n")

	// Hop1 部分
	sb.WriteString("    hop1: ")
	sb.WriteString(b.buildHopSelect(1, b.request.SourceType))
	sb.WriteString("\n")

	sb.WriteString("} AS result\n")

	// FROM 子句
	sb.WriteString(fmt.Sprintf("FROM %s\n", b.request.SourceType))

	// WHERE 子句
	sb.WriteString(b.buildWhereClause())
	sb.WriteString("\n")

	// LIMIT 子句
	sb.WriteString(fmt.Sprintf("LIMIT %d;", b.request.Limit))

	return sb.String()
}

// buildRootSelect 构建 Root 实体的 SELECT 结构
func (b *SurrealQueryBuilder) buildRootSelect() string {
	sourceType := b.request.SourceType
	primaryKeys := GetResourcePrimaryKeys(sourceType)
	livenessEdgeTable := GetLivenessEdgeTableName(sourceType)

	// 构建 entity_data
	entityDataFields := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		entityDataFields = append(entityDataFields, fmt.Sprintf("%s: %s", key, key))
	}

	return fmt.Sprintf(`{
        entity_type: meta::tb(id),
        entity_id: <string>id,
        entity_data: { %s },
        created_at: created_at,
        updated_at: updated_at,
        liveness: ->%s->liveness[WHERE period_end >= $start AND period_start <= $end].*
    }`,
		strings.Join(entityDataFields, ", "),
		livenessEdgeTable)
}

// buildHopSelect 构建指定跳数的 SELECT 结构
func (b *SurrealQueryBuilder) buildHopSelect(hop int, currentType ResourceType) string {
	if hop > b.request.MaxHops {
		return "{}"
	}

	// 获取当前资源类型的所有相关关系
	relations := b.getRelationsForType(currentType)
	if len(relations) == 0 {
		return "{}"
	}

	var sb strings.Builder
	sb.WriteString("{\n")

	first := true
	for _, rel := range relations {
		if !first {
			sb.WriteString(",\n")
		}
		first = false
		sb.WriteString(b.buildRelationQuery(hop, currentType, rel))
	}

	sb.WriteString("\n    }")

	return sb.String()
}

// RelationQueryInfo 关系查询信息
type RelationQueryInfo struct {
	Schema        *RelationSchema
	Direction     TraversalDirection // 遍历方向
	KeySuffix     string             // 键名后缀（动态关系才有）
	TraversalExpr string             // SurrealQL 遍历表达式 (-> 或 <-)
	TargetField   string             // 目标字段 (in 或 out)
	TargetType    ResourceType       // 目标资源类型
}

// getRelationsForType 获取指定资源类型的所有可用关系查询
func (b *SurrealQueryBuilder) getRelationsForType(resourceType ResourceType) []*RelationQueryInfo {
	var results []*RelationQueryInfo

	for i := range schemaRegistry {
		schema := &schemaRegistry[i]

		// 检查关系类别是否允许
		if !b.request.IsRelationCategoryAllowed(schema.Category) {
			continue
		}

		// 检查资源类型是否匹配
		if schema.FromType != resourceType && schema.ToType != resourceType {
			continue
		}

		if schema.Category == RelationCategoryStatic {
			// 静态关系：根据 Schema 确定方向
			info := b.buildStaticRelationInfo(schema, resourceType)
			if info != nil {
				results = append(results, info)
			}
		} else {
			// 动态关系：根据 DynamicRelationDirection 确定方向
			infos := b.buildDynamicRelationInfos(schema, resourceType)
			results = append(results, infos...)
		}
	}

	return results
}

// buildStaticRelationInfo 构建静态关系查询信息
func (b *SurrealQueryBuilder) buildStaticRelationInfo(schema *RelationSchema, currentType ResourceType) *RelationQueryInfo {
	info := &RelationQueryInfo{
		Schema:    schema,
		KeySuffix: "", // 静态关系不需要后缀
	}

	if schema.FromType == currentType {
		// 当前类型是 From，正向遍历到 To
		info.Direction = DirectionOutbound
		info.TraversalExpr = "->"
		info.TargetField = "out"
		info.TargetType = schema.ToType
	} else {
		// 当前类型是 To，反向遍历到 From
		info.Direction = DirectionInbound
		info.TraversalExpr = "<-"
		info.TargetField = "in"
		info.TargetType = schema.FromType
	}

	return info
}

// buildDynamicRelationInfos 构建动态关系查询信息（可能返回多个）
func (b *SurrealQueryBuilder) buildDynamicRelationInfos(schema *RelationSchema, currentType ResourceType) []*RelationQueryInfo {
	var results []*RelationQueryInfo
	direction := b.request.DynamicRelationDirection

	// 检查当前类型是否可以作为 From（outbound）
	canOutbound := schema.FromType == currentType
	// 检查当前类型是否可以作为 To（inbound）
	canInbound := schema.ToType == currentType

	// 根据请求的方向生成查询
	if (direction == DirectionOutbound || direction == DirectionBoth) && canOutbound {
		results = append(results, &RelationQueryInfo{
			Schema:        schema,
			Direction:     DirectionOutbound,
			KeySuffix:     "_outbound",
			TraversalExpr: "->",
			TargetField:   "out",
			TargetType:    schema.ToType,
		})
	}

	if (direction == DirectionInbound || direction == DirectionBoth) && canInbound {
		results = append(results, &RelationQueryInfo{
			Schema:        schema,
			Direction:     DirectionInbound,
			KeySuffix:     "_inbound",
			TraversalExpr: "<-",
			TargetField:   "in",
			TargetType:    schema.FromType,
		})
	}

	return results
}

// buildRelationQuery 构建单个关系的查询
func (b *SurrealQueryBuilder) buildRelationQuery(hop int, _ ResourceType, rel *RelationQueryInfo) string {
	relationType := rel.Schema.RelationType
	relationTable := string(relationType)
	relationLivenessEdgeTable := GetRelationLivenessEdgeTableName(relationType)
	targetLivenessEdgeTable := GetLivenessEdgeTableName(rel.TargetType)

	// 键名：静态关系直接用关系名，动态关系加方向后缀
	keyName := relationTable + rel.KeySuffix

	// 构建 target 的 entity_data
	targetPrimaryKeys := GetResourcePrimaryKeys(rel.TargetType)
	targetDataFields := make([]string, 0, len(targetPrimaryKeys))
	for _, key := range targetPrimaryKeys {
		targetDataFields = append(targetDataFields, fmt.Sprintf("%s: %s.%s", key, rel.TargetField, key))
	}

	// 构建基础字段
	var fieldsBuilder strings.Builder
	fieldsBuilder.WriteString(fmt.Sprintf(`
            hop: %d,
            relation_type: '%s',
            relation_category: '%s',`, hop, relationType, rel.Schema.Category))

	// 动态关系需要 direction 字段
	if rel.Schema.Category == RelationCategoryDynamic {
		fieldsBuilder.WriteString(fmt.Sprintf(`
            direction: '%s',`, rel.Direction))
	}

	fieldsBuilder.WriteString(fmt.Sprintf(`
            relation_id: <string>id,
            relation_liveness: ->%s->liveness[WHERE period_end >= $start AND period_start <= $end].*,
            target: {
                entity_type: '%s',
                entity_id: <string>%s,
                entity_data: { %s },
                liveness: %s->%s->liveness[WHERE period_end >= $start AND period_start <= $end].*
            }`,
		relationLivenessEdgeTable,
		rel.TargetType,
		rel.TargetField,
		strings.Join(targetDataFields, ", "),
		rel.TargetField,
		targetLivenessEdgeTable))

	// 如果还有下一跳，递归构建
	if hop < b.request.MaxHops {
		nextHopKey := fmt.Sprintf("hop%d", hop+1)
		// TODO: 递归构建下一跳需要更复杂的逻辑，这里先留空
		// 实际实现需要在 target 的 context 中展开
		fieldsBuilder.WriteString(fmt.Sprintf(`,
            %s: {}`, nextHopKey))
	}

	return fmt.Sprintf(`        %s: (SELECT {%s
        } FROM %s%s WHERE updated_at >= $start)`,
		keyName,
		fieldsBuilder.String(),
		rel.TraversalExpr,
		relationTable)
}

// buildWhereClause 构建 WHERE 子句
func (b *SurrealQueryBuilder) buildWhereClause() string {
	var conditions []string

	// 根据 source_info 构建过滤条件
	if len(b.request.SourceInfo) > 0 {
		// 按键排序保证确定性
		keys := make([]string, 0, len(b.request.SourceInfo))
		for k := range b.request.SourceInfo {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			v := b.request.SourceInfo[k]
			conditions = append(conditions, fmt.Sprintf("%s = '%s'", k, escapeSurrealString(v)))
		}
	}

	// 时间过滤
	conditions = append(conditions, "updated_at >= $start")

	// Liveness 过滤（使用边表）
	livenessEdgeTable := GetLivenessEdgeTableName(b.request.SourceType)
	conditions = append(conditions, fmt.Sprintf(
		"array::len(->%s->liveness[WHERE period_end >= $start AND period_start <= $end]) > 0",
		livenessEdgeTable))

	return "WHERE " + strings.Join(conditions, "\n  AND ")
}

// escapeSurrealString 转义 SurrealQL 字符串中的特殊字符
func escapeSurrealString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}
