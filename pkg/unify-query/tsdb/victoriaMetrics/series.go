// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed on the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package victoriaMetrics

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var _ storage.Series = &vmSeries{}

// a vmSeries need to implement storage.Series interface
type vmSeries struct {
	labels labels.Labels
	points []pointPair
}

type pointPair struct {
	timestamp int64
	value     float64
}

func newVMSeries(lb labels.Labels, ps []pointPair) *vmSeries {
	return &vmSeries{
		labels: lb,
		points: ps,
	}
}

func (s *vmSeries) Labels() labels.Labels {
	return s.labels
}

func (s *vmSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return newVMSeriesIterator(s.points)
}

// Shrink could filter the points in the series to only keep those within [startTime, endTime] range.
func (s *vmSeries) Shrink(startTime, endTime int64) {
	if startTime >= endTime {
		s.points = nil
		return
	}

	filtered := make([]pointPair, 0, len(s.points))
	for _, point := range s.points {
		if point.timestamp >= startTime && point.timestamp <= endTime {
			filtered = append(filtered, point)
		}
	}
	s.points = filtered
}
