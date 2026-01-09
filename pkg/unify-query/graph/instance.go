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
)

// Instance 图数据库实例
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

// Health 检查数据库连接健康状态
func (i *Instance) Health(ctx context.Context) error {
	return i.client.Health(ctx)
}
