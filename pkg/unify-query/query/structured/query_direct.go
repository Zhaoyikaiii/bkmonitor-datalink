// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package structured

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

type QueryDirect struct {
	References metadata.QueryReference `json:"references,omitempty" bson:"references,omitempty" yaml:"references,omitempty"`
	// MetricMerge 表达式：支持所有PromQL语法
	MetricMerge string `json:"metric_merge,omitempty" example:"a"`
	// OrderBy 排序字段列表，按顺序排序，负数代表倒序, ["_time", "-_time"]
	OrderBy OrderBy `json:"order_by,omitempty"`
	// ResultColumns 指定保留返回字段值
	ResultColumns []string `json:"result_columns,omitempty" swaggerignore:"true"`
	// Start 开始时间：单位为任意长度的时间戳
	Start string `json:"start_time,omitempty" example:"1657848000"`
	// End 结束时间：单位为任意长度的时间戳
	End string `json:"end_time,omitempty" example:"1657851600"`
	// Step 步长：最终返回的点数的时间间隔
	Step string `json:"step,omitempty" example:"1m"`
	// DownSampleRange 降采样：大于Step才能生效，可以为空
	DownSampleRange string `json:"down_sample_range,omitempty" example:"5m"`
	// Timezone 时区
	Timezone string `json:"timezone,omitempty" example:"Asia/Shanghai"`
	// LookBackDelta 偏移量
	LookBackDelta string `json:"look_back_delta,omitempty"`
	// Instant 瞬时数据
	Instant bool `json:"instant"`

	// Reference 查询开始时间是否需要对齐，
	// 例如：
	// true:  range: 10:03 - 10:23 window: 10m -> 10:03 - 10:10, 10:10 - 10:20, 10:20 - 10:23
	// false: range: 10:03 - 10:23 window: 10m -> 10:00 - 10:10, 10:10 - 10:20, 10:20 - 10:23
	Reference bool `json:"reference,omitempty"`

	// NotTimeAlign 查询开始时间和聚合是否需要对齐
	// 例如
	// true:  range: 10:03 - 10:23 window: 10m -> 10:03 - 10:13, 10:13 - 10:23
	// false: range: 10:03 - 10:23 window: 10m -> 10:00 - 10:10, 10:10 - 10:20, 10:20 - 10:23
	NotTimeAlign bool `json:"not_time_align"`

	// 增加公共限制
	// Limit 点数限制数量
	Limit int `json:"limit,omitempty" example:"0"`
	// From 翻页开启数字
	From int `json:"from,omitempty" example:"0"`

	// Scroll 是否启用 Scroll 查询
	Scroll string `json:"scroll,omitempty"`
	// SliceMax 最大切片数量
	SliceMax int `json:"slice_max,omitempty"`
	// IsMultiFrom 是否启用 MultiFrom 查询
	IsMultiFrom bool `json:"is_multi_from,omitempty"`
	// IsSearchAfter 是否启用 SearchAfter 查询
	IsSearchAfter bool `json:"is_search_after,omitempty"`
	// ClearCache 是否强制清理已存在的缓存会话
	ClearCache bool `json:"clear_cache,omitempty"`

	ResultTableOptions metadata.ResultTableOptions `json:"result_table_options,omitempty"`

	// HighLight 是否开启高亮
	//HighLight *metadata.HighLight `json:"highlight,omitempty"`

	// DryRun 是否启用 DryRun
	DryRun bool `json:"dry_run,omitempty"`

	// IsMergeDB 是否启用合并 db 特性
	IsMergeDB bool `json:"is_merge_db,omitempty"`
}

func (qry *QueryDirect) GetReferences() metadata.QueryReference {
	var refs metadata.QueryReference
	qry.References.Each(func(refName string, qm *metadata.Query) {
		// 时间复用
		qm.Timezone = qry.Timezone
		qm.Start = qry.Start

	})
}
