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
	"strings"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/tsdb"
)

// escapeSurrealString 转义 SurrealQL 字符串中的特殊字符
// 防止 SQL 注入攻击
func escapeSurrealString(s string) string {
	// 转义单引号和反斜杠
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

// QueryBuilder 查询构建器
type QueryBuilder struct {
	tolerance time.Duration
	maxLimit  int
}

// NewQueryBuilder 创建查询构建器
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		tolerance: tsdb.GraphTolerance,
		maxLimit:  tsdb.GraphMaxLimit,
	}
}

// SetTolerance 设置时间容忍度
func (b *QueryBuilder) SetTolerance(d time.Duration) {
	b.tolerance = d
}

// GetTolerance 获取时间容忍度
func (b *QueryBuilder) GetTolerance() time.Duration {
	return b.tolerance
}

// SetMaxLimit 设置查询结果最大行数
// 设置为 0 或负数可禁用限制
func (b *QueryBuilder) SetMaxLimit(limit int) {
	b.maxLimit = limit
}

// DisableLimit 禁用查询结果限制
func (b *QueryBuilder) DisableLimit() {
	b.maxLimit = 0
}

// GetMaxLimit 获取查询结果最大行数
func (b *QueryBuilder) GetMaxLimit() int {
	return b.maxLimit
}

// applyTolerance 应用时间容忍度，扩展查询时间窗口
// 返回扩展后的 startTime 和 endTime
func (b *QueryBuilder) applyTolerance(startTime, endTime int64) (int64, int64) {
	toleranceMs := b.tolerance.Milliseconds()
	adjustedStart := startTime - toleranceMs
	if adjustedStart < 0 {
		adjustedStart = 0
	}
	adjustedEnd := endTime + toleranceMs
	return adjustedStart, adjustedEnd
}

// BuildLivenessQuery 构建存活记录查询
// 查询条件：period_start <= endTime AND period_end >= startTime
// 时间窗口会应用 tolerance 进行扩展
// 注意：当设置了 maxLimit 时，结果可能被截断
func (b *QueryBuilder) BuildLivenessQuery(resourceType ResourceType, resourceID string, startTime, endTime int64) string {
	tableName := GetLivenessTableName(resourceType)
	fieldName := string(resourceType) + "_id"

	// 应用 tolerance 扩展时间窗口
	adjustedStart, adjustedEnd := b.applyTolerance(startTime, endTime)

	// 转义 resourceID 防止 SQL 注入
	escapedID := escapeSurrealString(resourceID)

	query := fmt.Sprintf(`SELECT * FROM %s 
WHERE %s = '%s' 
AND period_start <= %d 
AND period_end >= %d
ORDER BY period_start ASC`, tableName, fieldName, escapedID, adjustedEnd, adjustedStart)

	if b.maxLimit > 0 {
		query += fmt.Sprintf("\nLIMIT %d", b.maxLimit)
	}

	return query
}

// BuildRelationLivenessQuery 构建关联存活记录查询
// 时间窗口会应用 tolerance 进行扩展
// 注意：当设置了 maxLimit 时，结果可能被截断
func (b *QueryBuilder) BuildRelationLivenessQuery(relationType RelationType, relationID string, startTime, endTime int64) string {
	tableName := GetRelationLivenessTableName(relationType)

	// 应用 tolerance 扩展时间窗口
	adjustedStart, adjustedEnd := b.applyTolerance(startTime, endTime)

	// 转义 relationID 防止 SQL 注入
	escapedID := escapeSurrealString(relationID)

	query := fmt.Sprintf(`SELECT * FROM %s 
WHERE relation_id = '%s' 
AND period_start <= %d 
AND period_end >= %d
ORDER BY period_start ASC`, tableName, escapedID, adjustedEnd, adjustedStart)

	if b.maxLimit > 0 {
		query += fmt.Sprintf("\nLIMIT %d", b.maxLimit)
	}

	return query
}

// BuildRelatedResourcesQuery 构建查询与指定资源相关的所有关系和目标资源
// 返回关系记录及其关联的目标资源 liveness
// 时间窗口会应用 tolerance 进行扩展
// 注意：当设置了 maxLimit 时，结果可能被截断
func (b *QueryBuilder) BuildRelatedResourcesQuery(
	relationType RelationType,
	resourceID string,
	direction TraversalDirection,
	startTime, endTime int64,
) string {
	relationTable := string(relationType)

	// 根据方向确定查询字段
	// outbound: 从 in 端查找 out 端
	// inbound: 从 out 端查找 in 端
	var matchField, targetField string
	if direction == DirectionOutbound {
		matchField = FieldIn
		targetField = FieldOut
	} else {
		matchField = FieldOut
		targetField = FieldIn
	}

	// 应用 tolerance 扩展时间窗口
	adjustedStart, adjustedEnd := b.applyTolerance(startTime, endTime)

	// 转义 resourceID 防止 SQL 注入
	escapedID := escapeSurrealString(resourceID)

	// SurrealDB 查询：查找关系及其 liveness，同时获取目标资源的 liveness
	query := fmt.Sprintf(`SELECT 
    id AS relation_id,
    %s AS from_id,
    %s AS to_id,
    period_start,
    period_end,
    is_active
FROM %s 
WHERE %s = '%s' 
AND period_start <= %d 
AND period_end >= %d
ORDER BY period_start ASC`, matchField, targetField, relationTable, matchField, escapedID, adjustedEnd, adjustedStart)

	if b.maxLimit > 0 {
		query += fmt.Sprintf("\nLIMIT %d", b.maxLimit)
	}

	return query
}
