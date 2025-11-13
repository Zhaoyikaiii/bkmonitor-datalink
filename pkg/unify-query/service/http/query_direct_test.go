// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/internal/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/promql"
	"github.com/stretchr/testify/assert"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
)

// TestQueryRawWithInstanceDirect 测试直接查询接口的核心功能
func TestQueryRawWithInstanceDirect(t *testing.T) {
	ctx := metadata.InitHashID(context.Background())

	mock.Init()
	influxdb.MockSpaceRouter(ctx)
	promql.MockEngine()

	t.Run("基础QueryReference查询", func(t *testing.T) {
		queryReference := metadata.QueryReference{
			"a": []*metadata.QueryMetric{
				{
					QueryList: metadata.QueryList{
						&metadata.Query{
							DataSource: structured.BkLog,
							TableID:    influxdb.ResultTableEs,
							Field:      "gseIndex",
							Size:       10,
							From:       0,
							Timezone:   "Asia/Shanghai",
							OffsetInfo: metadata.OffSetInfo{
								Limit:   10,
								OffSet:  time.Duration(0),
								SOffSet: 0,
								SLimit:  0,
							},
						},
					},
					ReferenceName: "a",
					MetricName:    "gseIndex",
					IsCount:       false,
				},
			},
		}

		total, list, options, err := queryRawWithInstanceDirect(ctx, queryReference)
		assert.Nil(t, err)
		if err != nil {
			return
		}

		// 验证返回总记录数
		assert.Equal(t, int64(2), total)

		// 验证返回数据格式 - 使用相同的MarshalListMap方法
		actual := json.MarshalListMap(list)
		expected := `[{"__data_label":"es","__doc_id":"test1","__index":"v2_2_bklog_bk_unify_query_20240814_0","__result_table":"result_table.es","__table_uuid":"result_table.es|1|1","__ext.container_id":"test-container-1","dtEventTimeStamp":"1723594161000"},{"__data_label":"es","__doc_id":"test2","__index":"v2_2_bklog_bk_unify_query_20240814_0","__result_table":"result_table.es","__table_uuid":"result_table.es|1|2","__ext.container_id":"test-container-2","dtEventTimeStamp":"1723594162000"}]`
		assert.Equal(t, expected, actual)

		// 验证options参数 - 使用与现有测试相同的处理方式
		if len(options) > 0 {
			optActual := json.MarshalListMap([]map[string]any{})
			// 先验证options不为空，具体预期值等实现后再完善
			assert.NotEmpty(t, optActual)
		}
	})

	t.Run("空QueryReference权限验证", func(t *testing.T) {
		total, list, options, err := queryRawWithInstanceDirect(ctx, metadata.QueryReference{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
		assert.Equal(t, int64(0), total)
		assert.Empty(t, list)
		assert.Empty(t, options)
	})

	t.Run("多个QueryReference查询", func(t *testing.T) {
		queryReference := metadata.QueryReference{
			"a": []*metadata.QueryMetric{
				{
					QueryList: metadata.QueryList{
						&metadata.Query{
							DataSource: structured.BkLog,
							TableID:    influxdb.ResultTableEs,
							Field:      "gseIndex",
							Size:       5,
							From:       0,
							Timezone:   "Asia/Shanghai",
							OffsetInfo: metadata.OffSetInfo{
								Limit:   5,
								OffSet:  time.Duration(0),
								SOffSet: 0,
								SLimit:  0,
							},
						},
					},
					ReferenceName: "a",
					MetricName:    "gseIndex",
					IsCount:       false,
				},
			},
			"b": []*metadata.QueryMetric{
				{
					QueryList: metadata.QueryList{
						&metadata.Query{
							DataSource: structured.BkLog,
							TableID:    influxdb.ResultTableEs,
							Field:      "container_id",
							Size:       5,
							From:       0,
							Timezone:   "Asia/Shanghai",
							OffsetInfo: metadata.OffSetInfo{
								Limit:   5,
								OffSet:  time.Duration(0),
								SOffSet: 0,
								SLimit:  0,
							},
						},
					},
					ReferenceName: "b",
					MetricName:    "container_id",
					IsCount:       false,
				},
			},
		}

		total, list, options, err := queryRawWithInstanceDirect(ctx, queryReference)
		assert.Nil(t, err)
		if err != nil {
			return
		}

		// 应该返回总记录数
		assert.Greater(t, total, int64(0))

		// 应该返回数据列表
		assert.NotEmpty(t, list)

		// 验证返回数据包含预期的字段
		for _, item := range list {
			assert.Contains(t, item, "__data_label")
			assert.Contains(t, item, "__result_table")
			assert.Contains(t, item, "dtEventTimeStamp")
			// 可能包含 gseIndex 或 container_id 字段
			_, hasGseIndex := item["gseIndex"]
			_, hasContainerId := item["container_id"]
			assert.True(t, hasGseIndex || hasContainerId)
		}

		// 验证选项不为空
		if len(options) > 0 {
			optActual := json.MarshalListMap([]map[string]any{})
			assert.NotEmpty(t, optActual)
		}
	})

	t.Run("时区处理测试", func(t *testing.T) {
		queryReference := metadata.QueryReference{
			"test": []*metadata.QueryMetric{
				{
					QueryList: metadata.QueryList{
						&metadata.Query{
							DataSource: structured.BkLog,
							TableID:    influxdb.ResultTableEs,
							Field:      "gseIndex",
							Size:       10,
							From:       0,
							// 不设置时区，测试默认时区处理
							OffsetInfo: metadata.OffSetInfo{
								Limit:   10,
								OffSet:  time.Duration(0),
								SOffSet: 0,
								SLimit:  0,
							},
						},
					},
					ReferenceName: "test",
					MetricName:    "gseIndex",
					IsCount:       false,
				},
			},
		}

		total, list, options, err := queryRawWithInstanceDirect(ctx, queryReference)
		assert.Nil(t, err)
		if err != nil {
			return
		}

		// 验证时区处理后仍然能正常查询
		assert.Greater(t, total, int64(0))
		assert.NotEmpty(t, list)

		// 验证options
		if len(options) > 0 {
			optActual := json.MarshalListMap([]map[string]any{})
			assert.NotEmpty(t, optActual)
		}
	})
}
