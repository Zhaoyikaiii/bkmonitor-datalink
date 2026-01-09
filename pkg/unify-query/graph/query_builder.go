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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/tsdb"
)

// QueryBuilder 查询构建器
type QueryBuilder struct {
	tolerance time.Duration
}

// NewQueryBuilder 创建查询构建器
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		tolerance: tsdb.GraphTolerance,
	}
}

// SetTolerance 设置时间容忍度
func (b *QueryBuilder) SetTolerance(d time.Duration) {
	b.tolerance = d
}

// BuildLivenessQuery 构建存活记录查询
// 查询条件：period_start <= endTime AND period_end >= startTime
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

// BuildRelationLivenessQuery 构建关联存活记录查询
func (b *QueryBuilder) BuildRelationLivenessQuery(relationType RelationType, relationID string, startTime, endTime int64) string {
	tableName := GetRelationLivenessTableName(relationType)

	return fmt.Sprintf(`
SELECT * FROM %s 
WHERE relation_id = '%s' 
AND period_start <= %d 
AND period_end >= %d
ORDER BY period_start ASC
`, tableName, relationID, endTime, startTime)
}

// BuildRelatedResourcesQuery 构建查询与指定资源相关的所有关系和目标资源
// 返回关系记录及其关联的目标资源 liveness
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

	// SurrealDB 查询：查找关系及其 liveness，同时获取目标资源的 liveness
	return fmt.Sprintf(`
SELECT 
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
ORDER BY period_start ASC
`, matchField, targetField, relationTable, matchField, resourceID, endTime, startTime)
}
