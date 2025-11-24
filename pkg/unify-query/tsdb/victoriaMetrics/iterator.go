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
	"sort"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// vmSeriesIterator needed to adapt victoria metrics query result to prometheus chunkenc.Iterator
type vmSeriesIterator struct {
	points []pointPair
	index  int
}

var _ chunkenc.Iterator = (*vmSeriesIterator)(nil)

func newVMSeriesIterator(points []pointPair) *vmSeriesIterator {
	return &vmSeriesIterator{
		points: points,
		// index initialize point to -1, so the first call to Next() will set it to 0
		index: -1,
	}
}

func (it *vmSeriesIterator) Next() chunkenc.ValueType {
	it.index++
	if it.index < len(it.points) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

func (it *vmSeriesIterator) At() (int64, float64) {
	if it.index < 0 || it.index >= len(it.points) {
		return 0, 0
	}
	return it.points[it.index].timestamp, it.points[it.index].value
}

func (it *vmSeriesIterator) AtT() int64 {
	if it.index < 0 || it.index >= len(it.points) {
		return 0
	}
	return it.points[it.index].timestamp
}

// AtHistogram vm not support histogram type
func (it *vmSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram vm not support float histogram type
func (it *vmSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// Seek retrieves the first sample with timestamp >= t.
func (it *vmSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if it.index == -1 {
		it.index = 0
	}
	if it.index >= len(it.points) {
		return chunkenc.ValNone
	}
	if s := it.points[it.index]; s.timestamp >= t {
		return chunkenc.ValFloat
	}
	// cause it.points is ordered by timestamp, we can use binary search here
	it.index += sort.Search(len(it.points)-it.index, func(i int) bool {
		s := it.points[i+it.index]
		return s.timestamp >= t
	})
	if it.index < len(it.points) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

// Err cause we don't have error return from vm, so always return nil
func (it *vmSeriesIterator) Err() error {
	return nil
}
