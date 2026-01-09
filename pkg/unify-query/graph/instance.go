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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/tsdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

// Instance 图数据库实例
// 架构模式：Client（连接执行）+ QueryBuilder（构建查询）+ ResponseParser（解析响应）
type Instance struct {
	ctx        context.Context
	address    string
	clientType ClientType
	namespace  string
	database   string
	timeout    time.Duration
	maxLimit   int
	tolerance  time.Duration

	client  Client
	builder *QueryBuilder
	parser  *ResponseParser
}

// NewInstance 创建图数据库实例
func NewInstance(ctx context.Context, cfg *ClientConfig) (*Instance, error) {
	client, err := NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	timeout := tsdb.GraphTimeout
	maxLimit := tsdb.GraphMaxLimit
	tolerance := tsdb.GraphTolerance

	builder := NewQueryBuilder()
	builder.SetTolerance(tolerance)

	return &Instance{
		ctx:        ctx,
		address:    cfg.Address,
		clientType: cfg.Type,
		namespace:  cfg.Namespace,
		database:   cfg.Database,
		timeout:    timeout,
		maxLimit:   maxLimit,
		tolerance:  tolerance,
		client:     client,
		builder:    builder,
		parser:     NewResponseParser(),
	}, nil
}

// Close 关闭实例
func (i *Instance) Close() error {
	if i.client != nil {
		return i.client.Close()
	}
	return nil
}

// SetTimeout 设置查询超时时间
func (i *Instance) SetTimeout(d time.Duration) {
	i.timeout = d
}

// SetTolerance 设置时间有效性检查的回溯容忍度
func (i *Instance) SetTolerance(d time.Duration) {
	i.tolerance = d
	i.builder.SetTolerance(d)
}

// QuerySingleHop 执行单跳关联查询
func (i *Instance) QuerySingleHop(ctx context.Context, req *HopQueryRequest) (*HopQueryResponse, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-query-single-hop")
	defer span.End(&err)

	span.Set("source-type", string(req.SourceType))
	span.Set("source-info", fmt.Sprintf("%+v", req.SourceInfo))
	span.Set("target-type", string(req.TargetType))
	span.Set("timestamp", req.Timestamp)

	// Set default values
	if req.MaxHops <= 0 {
		req.MaxHops = 1
	}
	if req.LookBackDelta == "" {
		req.LookBackDelta = i.tolerance.String()
	}

	// Build the query
	query := i.builder.BuildSingleHopQuery(req)
	span.Set("query", query)

	// Execute the query
	result, err := i.client.Execute(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Parse the response
	resp, err := i.parser.ParseSingleHopResponse(result, req)
	if err != nil {
		return nil, err
	}

	span.Set("result-total", resp.Total)
	return resp, nil
}

// QueryBatch 执行批量查询
func (i *Instance) QueryBatch(ctx context.Context, req *BatchQueryRequest) (*BatchQueryResponse, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-query-batch")
	defer span.End(&err)

	span.Set("query-count", len(req.QueryList))

	resp := &BatchQueryResponse{
		Code:    0,
		Message: "success",
		Data: &BatchQueryData{
			QueryList: make([]*SingleHopQueryResult, 0, len(req.QueryList)),
		},
	}

	for idx, queryReq := range req.QueryList {
		result, queryErr := i.QuerySingleHop(ctx, queryReq)
		if queryErr != nil {
			// Continue with other queries, but record the error
			result = &HopQueryResponse{
				Timestamp:  queryReq.Timestamp,
				SourceType: queryReq.SourceType,
				SourceInfo: queryReq.SourceInfo,
				Total:      0,
			}
		}

		resp.Data.QueryList = append(resp.Data.QueryList, &SingleHopQueryResult{
			QueryIndex:       idx,
			HopQueryResponse: result,
		})
	}

	span.Set("result-count", len(resp.Data.QueryList))
	return resp, nil
}

// Execute 执行原始 SurrealQL 查询
func (i *Instance) Execute(ctx context.Context, query string, vars map[string]any) (any, error) {
	return i.client.Execute(ctx, query, vars)
}

// QueryResources 查询资源
func (i *Instance) QueryResources(ctx context.Context, req *ResourceQueryRequest) (*ResourceQueryResponse, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-query-resources")
	defer span.End(&err)

	span.Set("resource-type", string(req.ResourceType))
	span.Set("labels", fmt.Sprintf("%+v", req.Labels))
	span.Set("limit", req.Limit)
	span.Set("offset", req.Offset)

	query := i.builder.BuildResourceQuery(req)
	span.Set("query", query)

	result, err := i.client.Execute(ctx, query, nil)
	if err != nil {
		return nil, err
	}

	resources, err := i.parser.ParseResources(result)
	if err != nil {
		return nil, err
	}

	span.Set("result-total", len(resources))
	return &ResourceQueryResponse{
		Resources: resources,
		Total:     int64(len(resources)),
	}, nil
}

// QueryRelations 查询关系
func (i *Instance) QueryRelations(ctx context.Context, req *RelationQueryRequest) (*RelationQueryResponse, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-query-relations")
	defer span.End(&err)

	span.Set("relation-type", string(req.RelationType))
	span.Set("limit", req.Limit)
	span.Set("offset", req.Offset)

	query := i.builder.BuildRelationQuery(req)
	if query == "" {
		return &RelationQueryResponse{Relations: []*Relation{}, Total: 0}, nil
	}
	span.Set("query", query)

	result, err := i.client.Execute(ctx, query, nil)
	if err != nil {
		return nil, err
	}

	relations, err := i.parser.ParseRelations(result)
	if err != nil {
		return nil, err
	}

	span.Set("result-total", len(relations))
	return &RelationQueryResponse{
		Relations: relations,
		Total:     int64(len(relations)),
	}, nil
}

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

// CheckLiveness 检查资源在指定时间范围内是否存活
func (i *Instance) CheckLiveness(ctx context.Context, resourceID string, startTime, endTime int64) (bool, error) {
	records, err := i.GetLivenessRecords(ctx, resourceID, startTime, endTime)
	if err != nil {
		return false, err
	}
	return len(records) > 0, nil
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

// Health 检查数据库连接健康状态
func (i *Instance) Health(ctx context.Context) error {
	return i.client.Health(ctx)
}
