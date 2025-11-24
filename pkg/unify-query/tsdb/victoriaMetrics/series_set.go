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
	"github.com/prometheus/prometheus/storage"
)

// make sure vmSeriesSet implements storage.SeriesSet interface
var _ storage.SeriesSet = &vmSeriesSet{}

type vmSeriesSet struct {
	series   []storage.Series
	index    int
	err      error
	warnings storage.Warnings
}

func newVMSeriesSet(series []storage.Series) *vmSeriesSet {
	return &vmSeriesSet{
		series:   series,
		index:    -1,
		err:      nil,
		warnings: nil,
	}
}

func nextVMSeriesSetWithErr(err error) *vmSeriesSet {
	return &vmSeriesSet{
		series:   nil,
		index:    0,
		err:      err,
		warnings: nil,
	}
}

// Next points to the next series
func (s *vmSeriesSet) Next() bool {
	s.index++
	return s.index < len(s.series)
}

// At returns the current series
func (s *vmSeriesSet) At() storage.Series {
	if s.index < 0 || s.index >= len(s.series) {
		return nil
	}
	return s.series[s.index]
}

func (s *vmSeriesSet) Err() error {
	return s.err
}

func (s *vmSeriesSet) Warnings() storage.Warnings {
	return s.warnings
}
