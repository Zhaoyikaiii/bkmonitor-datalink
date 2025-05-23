// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	elastic "github.com/olivere/elastic/v7"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/internal/function"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/internal/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
)

const (
	KeyValue   = "_key"
	FieldValue = "_value"
	FieldTime  = "_time"

	DefaultTimeFieldName = "dtEventTimeStamp"
	DefaultTimeFieldType = TimeFieldTypeTime
	DefaultTimeFieldUnit = function.Millisecond

	Type       = "type"
	Properties = "properties"

	Min         = "min"
	Max         = "max"
	Sum         = "sum"
	Count       = "count"
	Avg         = "avg"
	Cardinality = "cardinality"

	DateHistogram = "date_histogram"
	Percentiles   = "percentiles"

	Nested = "nested"
	Terms  = "terms"

	ESStep = "."
)

const (
	KeyDocID     = "__doc_id"
	KeyHighLight = "__highlight"
	KeySort      = "sort"

	KeyIndex   = "__index"
	KeyTableID = "__result_table"
	KeyAddress = "__address"

	KeyDataLabel = "__data_label"
)

const (
	KeyWord = "keyword"
	Text    = "text"
	Integer = "integer"
	Long    = "long"
	Date    = "date"
)

const (
	TimeFieldTypeTime = "date"
	TimeFieldTypeInt  = "long"
)

const (
	EpochSecond       = "epoch_second"
	EpochMillis       = "epoch_millis"
	EpochMicroseconds = "epoch_microseconds"
	EpochNanoseconds  = "epoch_nanoseconds"
)

const (
	Must      = "must"
	MustNot   = "must_not"
	Should    = "should"
	ShouldNot = "should_not"
)

type TimeSeriesResult struct {
	TimeSeriesMap map[string]*prompb.TimeSeries
	Error         error
}

func mapData(prefix string, data map[string]any, res map[string]any) {
	for k, v := range data {
		if prefix != "" {
			k = prefix + ESStep + k
		}
		switch v.(type) {
		case map[string]any:
			mapData(k, v.(map[string]any), res)
		default:
			res[k] = v
		}
	}
}

func mapProperties(prefix string, data map[string]any, res map[string]string) {
	if prefix != "" {
		if t, ok := data[Type]; ok {
			switch ts := t.(type) {
			case string:
				res[prefix] = ts
			}
		}
	}

	if properties, ok := data[Properties]; ok {
		for k, v := range properties.(map[string]any) {
			if prefix != "" {
				k = prefix + ESStep + k
			}
			switch v.(type) {
			case map[string]any:
				mapProperties(k, v.(map[string]any), res)
			}
		}
	}
}

type ValueAgg struct {
	Name     string
	FuncType string
	Field    string

	Args   []any
	KwArgs map[string]any
}

type TimeAgg struct {
	Name     string
	Window   time.Duration
	TimeZone string
}

type TermAgg struct {
	Name             string
	Orders           metadata.Orders
	AttachedValueAgg *ValueAgg
}

type NestedAgg struct {
	Name string
}

// ReverseNestedAgg represents a reverse_nested aggregation.
// It's a placeholder in aggInfoList to signal f.Agg() to create a reverse_nested aggregation.
// The Name field is not strictly necessary for reverse_nested itself but kept for consistency with other agg types.
type ReverseNestedAgg struct {
	Name string // Can be an empty string or a descriptive name for clarity in logs/debugging
}

type aggInfoList []any

type FormatFactory struct {
	ctx context.Context

	valueField string
	timeField  metadata.TimeField

	decode func(k string) string
	encode func(k string) string

	mapping map[string]string
	data    map[string]any

	aggInfoList aggInfoList
	orders      metadata.Orders

	size     int
	timezone string

	start      time.Time
	end        time.Time
	timeFormat string

	isReference bool
}

func NewFormatFactory(ctx context.Context) *FormatFactory {
	f := &FormatFactory{
		ctx:         ctx,
		mapping:     make(map[string]string),
		aggInfoList: make(aggInfoList, 0),

		// default encode / decode
		encode: func(k string) string {
			return k
		},
		decode: func(k string) string {
			return k
		},
	}

	return f
}

func (f *FormatFactory) WithIsReference(isReference bool) *FormatFactory {
	f.isReference = isReference
	return f
}

func (f *FormatFactory) toFixInterval(window time.Duration) (string, error) {
	switch f.timeField.Unit {
	case function.Second:
		window /= 1e3
	case function.Microsecond:
		window *= 1e3
	case function.Nanosecond:
		window *= 1e6
	}

	if window.Milliseconds() < 1 {
		return "", fmt.Errorf("date histogram aggregation interval must be greater than 0ms")
	}
	return shortDur(window), nil
}

func (f *FormatFactory) toMillisecond(i int64) int64 {
	switch f.timeField.Unit {
	case function.Second:
		return i * 1e3
	case function.Microsecond:
		return i / 1e3
	case function.Nanosecond:
		return i / 1e6
	default:
		// 默认用毫秒
		return i
	}
}

func (f *FormatFactory) timeFormatToEpoch(unit string) string {
	switch unit {
	case function.Millisecond:
		return EpochMillis
	case function.Microsecond:
		return EpochMicroseconds
	case function.Nanosecond:
		return EpochNanoseconds
	default:
		// 默认用秒
		return EpochSecond
	}
}

func (f *FormatFactory) queryToUnix(t time.Time, unit string) int64 {
	switch unit {
	case function.Millisecond:
		return t.UnixMilli()
	case function.Microsecond:
		return t.UnixMicro()
	case function.Nanosecond:
		return t.UnixNano()
	default:
		// 默认用秒
		return t.Unix()
	}
}

func (f *FormatFactory) WithQuery(valueKey string, timeField metadata.TimeField, start, end time.Time, timeFormat string, size int) *FormatFactory {
	if timeField.Name == "" {
		timeField.Name = DefaultTimeFieldName
	}
	if timeField.Type == "" {
		timeField.Type = DefaultTimeFieldType
	}
	if timeField.Unit == "" {
		timeField.Unit = DefaultTimeFieldUnit
	}
	if timeFormat == "" {
		timeFormat = function.Second
	}

	f.start = start
	f.end = end
	f.timeFormat = timeFormat
	f.valueField = valueKey
	f.timeField = timeField
	f.size = size

	return f
}

func (f *FormatFactory) WithTransform(encode func(string) string, decode func(string) string) *FormatFactory {
	if encode != nil {
		f.encode = encode
	}
	if decode != nil {
		f.decode = decode
		// 如果有 decode valueField 需要重新载入
		f.valueField = decode(f.valueField)
	}
	return f
}

func (f *FormatFactory) WithOrders(orders metadata.Orders) *FormatFactory {
	f.orders = make(metadata.Orders, 0, len(orders))
	for _, order := range orders {
		if f.decode != nil {
			order.Name = f.encode(order.Name)
		}
		f.orders = append(f.orders, order)
	}
	return f
}

// WithMappings 合并 mapping，后面的合并前面的
func (f *FormatFactory) WithMappings(mappings ...map[string]any) *FormatFactory {
	for _, mapping := range mappings {
		mapProperties("", mapping, f.mapping)
	}
	return f
}

func (f *FormatFactory) RangeQuery() (elastic.Query, error) {
	var (
		err error
	)

	fieldName := f.timeField.Name
	fieldType := f.timeField.Type

	var query elastic.Query
	switch fieldType {
	case TimeFieldTypeInt:
		// int 类型，直接按照 tableID 配置的单位转换
		query = elastic.NewRangeQuery(fieldName).
			Gte(f.queryToUnix(f.start, f.timeField.Unit)).
			Lte(f.queryToUnix(f.end, f.timeField.Unit))
	case TimeFieldTypeTime:
		// date 类型，使用 查询的单位转换
		query = elastic.NewRangeQuery(fieldName).
			Gte(f.queryToUnix(f.start, f.timeFormat)).
			Lte(f.queryToUnix(f.end, f.timeFormat)).
			Format(f.timeFormatToEpoch(f.timeFormat))
	default:
		err = fmt.Errorf("time field type is error %s", fieldType)
	}
	return query, err
}

func (f *FormatFactory) timeAgg(name string, window time.Duration, timezone string) {
	f.aggInfoList = append(
		f.aggInfoList, TimeAgg{
			Name: name, Window: window, TimeZone: timezone,
		},
	)
}

func (f *FormatFactory) termAgg(name string, isFirst bool) {
	info := TermAgg{
		Name: name,
	}

	for _, order := range f.orders {
		if name == order.Name {
			order.Name = KeyValue
			info.Orders = append(info.Orders, order)
		} else if isFirst {
			if order.Name == FieldValue {
				info.Orders = append(info.Orders, order)
			}
		}
	}

	f.aggInfoList = append(f.aggInfoList, info)
}

func (f *FormatFactory) valueAgg(name, funcType string, fieldForAgg string, args ...any) {
	f.aggInfoList = append(
		f.aggInfoList, ValueAgg{
			Name: name, FuncType: funcType, Field: fieldForAgg, Args: args,
		},
	)
}

func (f *FormatFactory) NestedField(field string) string {
	lbs := strings.Split(field, ESStep)
	for i := len(lbs) - 1; i >= 0; i-- {
		checkKey := strings.Join(lbs[0:i], ESStep)
		if v, ok := f.mapping[checkKey]; ok {
			if v == Nested {
				return checkKey
			}
		}
	}
	return ""
}

func (f *FormatFactory) nestedAgg(key string) {
	nf := f.NestedField(key)
	if nf != "" {
		f.aggInfoList = append(
			f.aggInfoList, NestedAgg{
				Name: nf,
			},
		)
	}

	return
}

// buildElasticValueAgg creates an elastic.Aggregation from a ValueAgg helper struct.
func buildElasticValueAgg(info ValueAgg) (elastic.Aggregation, error) {
	switch info.FuncType {
	case Min:
		return elastic.NewMinAggregation().Field(info.Field), nil
	case Max:
		return elastic.NewMaxAggregation().Field(info.Field), nil
	case Avg:
		return elastic.NewAvgAggregation().Field(info.Field), nil
	case Sum:
		return elastic.NewSumAggregation().Field(info.Field), nil
	case Count:
		return elastic.NewValueCountAggregation().Field(info.Field), nil
	case Cardinality:
		return elastic.NewCardinalityAggregation().Field(info.Field), nil
	case Percentiles:
		percents := make([]float64, 0)
		for _, arg := range info.Args {
			var percent float64
			switch v := arg.(type) {
			case float64:
				percent = v
			case int:
				percent = float64(v)
			case int32:
				percent = float64(v)
			case int64:
				percent = float64(v)
			default:
				return nil, fmt.Errorf("percent type is error: %T, %+v", v, v)
			}
			percents = append(percents, percent)
		}
		return elastic.NewPercentilesAggregation().Field(info.Field).Percentiles(percents...), nil
	default:
		return nil, fmt.Errorf("valueagg aggregation is not support this type %s, info: %+v", info.FuncType, info)
	}
}

// AggDataFormat 解析 es 的聚合计算
func (f *FormatFactory) AggDataFormat(data elastic.Aggregations, metricLabel *prompb.Label) (*prompb.QueryResult, error) {
	if data == nil {
		return &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{},
		}, nil
	}

	defer func() {
		if r := recover(); r != nil {
			log.Errorf(f.ctx, fmt.Sprintf("agg data format %v", r))
		}
	}()

	af := &aggFormat{
		aggInfoList:    f.aggInfoList,
		items:          make(items, 0),
		promDataFormat: f.encode,
		timeFormat:     f.toMillisecond,
	}

	af.get()
	defer af.put()

	err := af.ts(len(f.aggInfoList), data)
	if err != nil {
		return nil, err
	}

	timeSeriesMap := make(map[string]*prompb.TimeSeries)
	keySort := make([]string, 0)

	for _, im := range af.items {
		var (
			tsLabels []prompb.Label
		)
		if len(im.labels) > 0 {
			for _, dim := range af.dims {
				tsLabels = append(tsLabels, prompb.Label{
					Name:  dim,
					Value: im.labels[dim],
				})
			}
		}

		if metricLabel != nil {
			tsLabels = append(tsLabels, *metricLabel)
		}

		var seriesNameBuilder strings.Builder
		for _, l := range tsLabels {
			seriesNameBuilder.WriteString(l.String())
		}

		seriesKey := seriesNameBuilder.String()
		if _, ok := timeSeriesMap[seriesKey]; !ok {
			keySort = append(keySort, seriesKey)
			timeSeriesMap[seriesKey] = &prompb.TimeSeries{
				Samples: make([]prompb.Sample, 0),
			}
		}

		if im.timestamp == 0 {
			im.timestamp = f.start.UnixMilli()
		}

		timeSeriesMap[seriesKey].Labels = tsLabels
		timeSeriesMap[seriesKey].Samples = append(timeSeriesMap[seriesKey].Samples, prompb.Sample{
			Value:     im.value,
			Timestamp: im.timestamp,
		})
	}

	tss := make([]*prompb.TimeSeries, 0, len(timeSeriesMap))
	for _, key := range keySort {
		if ts, ok := timeSeriesMap[key]; ok {
			tss = append(tss, ts)
		}
	}

	return &prompb.QueryResult{Timeseries: tss}, nil
}

func (f *FormatFactory) SetData(data map[string]any) {
	f.data = map[string]any{}
	mapData("", data, f.data)
}

func (f *FormatFactory) Agg() (name string, agg elastic.Aggregation, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf(f.ctx, fmt.Sprintf("get mapping error: %s", r))
		}
	}()

	// aggInfoList is now ordered from outermost to innermost logical aggregation
	// We will iterate it, and 'agg' will hold the result of the *previous* (inner) aggregation.
	// 'name' will hold the *name/key* for that previous (inner) aggregation.

	var currentAgg elastic.Aggregation // This will be built from innermost to outermost
	var innerAggName string            // Name/key for the currentAgg when it becomes an inner agg

	for i := len(f.aggInfoList) - 1; i >= 0; i-- { // Iterate from innermost to outermost from aggInfoList
		aggInfo := f.aggInfoList[i]
		var nextAgg elastic.Aggregation // The aggregation generated in this iteration
		var nextAggName string          // The name/key for nextAgg

		switch info := aggInfo.(type) {
		case ValueAgg: // This case handles standalone ValueAggs (not attached ones)
			nextAggName = info.Name // Typically FieldValue
			var buildErr error
			nextAgg, buildErr = buildElasticValueAgg(info)
			if buildErr != nil {
				err = buildErr
				return
			}
		case TimeAgg:
			nextAggName = info.Name
			var interval string
			if f.timeField.Type == TimeFieldTypeInt {
				interval, err = f.toFixInterval(info.Window)
				if err != nil {
					return
				}
			} else {
				interval = shortDur(info.Window)
			}
			ta := elastic.NewDateHistogramAggregation().
				Field(f.timeField.Name).Interval(interval).MinDocCount(0).
				ExtendedBounds(f.timeFieldUnix(f.start), f.timeFieldUnix(f.end))
			if f.timeField.Type == TimeFieldTypeTime {
				ta = ta.TimeZone(info.TimeZone)
			}
			if currentAgg != nil { // Embed previously built (inner) aggregation
				ta = ta.SubAggregation(innerAggName, currentAgg)
			}
			nextAgg = ta
		case NestedAgg:
			nextAggName = info.Name // Path of the nested field
			na := elastic.NewNestedAggregation().Path(info.Name)
			if currentAgg != nil {
				na = na.SubAggregation(innerAggName, currentAgg)
			}
			nextAgg = na
		case ReverseNestedAgg:
			nextAggName = info.Name // Name for the reverse_nested block itself (e.g. "reverse_nested_aggregation")
			if info.Name == "" {    // Default name if not specified
				nextAggName = "reverse_nested"
			}
			rna := elastic.NewReverseNestedAggregation()
			if currentAgg != nil {
				rna = rna.SubAggregation(innerAggName, currentAgg)
			}
			nextAgg = rna
		case TermAgg:
			nextAggName = info.Name // Field name, becomes the key for this terms agg
			ta := elastic.NewTermsAggregation().Field(info.Name)
			fieldType, ok := f.mapping[info.Name]
			if !ok || fieldType == Text || fieldType == KeyWord {
				ta = ta.Missing(" ")
			}
			if f.size > 0 {
				ta = ta.Size(f.size)
			} else {
				ta = ta.Size(0)
			}
			for _, order := range info.Orders {
				ta = ta.Order(order.Name, order.Ast)
			}

			// Handle attached metric aggregation
			if info.AttachedValueAgg != nil {
				metricAgg, buildErr := buildElasticValueAgg(*info.AttachedValueAgg)
				if buildErr != nil {
					err = buildErr
					return // Propagate error
				}
				if metricAgg != nil {
					ta = ta.SubAggregation(info.AttachedValueAgg.Name /* typically FieldValue */, metricAgg)
				}
			}

			if currentAgg != nil { // This is for the chained (e.g., further nested) aggregation
				ta = ta.SubAggregation(innerAggName, currentAgg)
			}
			nextAgg = ta
		default:
			err = fmt.Errorf("aggInfoList aggregation is not support this type %T, info: %+v", info, info)
			return
		}
		currentAgg = nextAgg
		innerAggName = nextAggName
	}
	// After loop, currentAgg is the outermost aggregation, innerAggName is its name
	agg = currentAgg
	name = innerAggName
	return
}

func (f *FormatFactory) HighLight(queryString string, maxAnalyzedOffset int) *elastic.Highlight {
	requireFieldMatch := false
	if strings.Contains(queryString, ":") {
		requireFieldMatch = true
	}
	hl := elastic.NewHighlight().
		Field("*").NumOfFragments(0).
		RequireFieldMatch(requireFieldMatch).
		PreTags("<mark>").PostTags("</mark>")

	if maxAnalyzedOffset > 0 {
		hl = hl.MaxAnalyzedOffset(maxAnalyzedOffset)
	}

	return hl
}

// FieldPathInfo holds metadata about a field's path and name.
// Used to determine nesting and the correct field string for aggregations.
// Example: For "events.name", Path="events", FieldName="events.name", IsNested=true.
// For "name", Path="", FieldName="name", IsNested=false.
// Note: FieldName is always the OriginalField, Path is the longest prefix found in mappings as "nested".
type FieldPathInfo struct {
	OriginalField string // e.g., "events.name" or "name"
	Path          string // e.g., "events" or "" (empty if not nested or path not in mapping)
	FieldName     string // Always the OriginalField, used in terms/metric aggregations
	IsNested      bool
}

// getFieldPathInfo analyzes a field string to determine its nested path status and an effective field name.
// It checks f.mapping to identify if any prefix of the field corresponds to a known nested path.
func (f *FormatFactory) getFieldPathInfo(field string) FieldPathInfo {
	if field == "" {
		// Return non-nested if field is empty, though this should ideally be handled by callers.
		return FieldPathInfo{OriginalField: field, FieldName: field, Path: "", IsNested: false}
	}

	parts := strings.Split(field, ESStep)

	// Iterate from the longest possible path prefix down to the first part.
	// For a field like "a.b.c", it will test prefixes "a.b", then "a".
	for i := len(parts) - 1; i >= 1; i-- { // loop from parts[0:len-1] down to parts[0:1]
		currentPathPrefix := strings.Join(parts[0:i], ESStep)
		if typ, ok := f.mapping[currentPathPrefix]; ok && typ == Nested {
			// Found the longest prefix that is a known nested path.
			return FieldPathInfo{
				OriginalField: field,
				Path:          currentPathPrefix, // The identified nested path
				FieldName:     field,             // The full original field for aggregations
				IsNested:      true,
			}
		}
	}

	// If no prefix is a known nested path, check if the field *itself* is defined as type "nested".
	// This handles cases like `groupBy: ["events"]` where "events" itself is the nested object path.
	if typ, ok := f.mapping[field]; ok && typ == Nested {
		return FieldPathInfo{
			OriginalField: field,
			Path:          field, // The field itself is the nested path
			FieldName:     field, // Aggregating by the path itself
			IsNested:      true,
		}
	}

	// Default: field is not considered nested based on prefixes or its own type in mapping.
	return FieldPathInfo{
		OriginalField: field,
		Path:          "",    // No specific nested path identified from prefixes
		FieldName:     field, // Use the original field name
		IsNested:      false,
	}
}

// handlePathTransition manages the transition between currentLogicalPath and targetPath,
// adding ReverseNestedAgg and NestedAgg to f.aggInfoList as needed.
// It updates and returns the new currentLogicalPath.
func (f *FormatFactory) handlePathTransition(currentLogicalPath, targetPath string, targetIsNested bool) string {
	if targetIsNested {
		if targetPath != currentLogicalPath {
			// Break down paths into components
			currentParts := strings.Split(currentLogicalPath, ESStep)
			if currentLogicalPath == "" { // handle empty string case for Split
				currentParts = []string{}
			}
			targetParts := strings.Split(targetPath, ESStep)

			// Find common prefix length
			commonPrefixLen := 0
			for commonPrefixLen < len(currentParts) && commonPrefixLen < len(targetParts) && currentParts[commonPrefixLen] == targetParts[commonPrefixLen] {
				commonPrefixLen++
			}

			// Add ReverseNestedAggs to go up from currentLogicalPath to common ancestor
			for i := len(currentParts) - 1; i >= commonPrefixLen; i-- {
				f.aggInfoList = append(f.aggInfoList, ReverseNestedAgg{Name: "reverse_nested"}) // Standardized name
			}

			// Add NestedAggs to go down from common ancestor to targetPath
			for i := commonPrefixLen; i < len(targetParts); i++ {
				pathToNest := strings.Join(targetParts[0:i+1], ESStep)
				f.aggInfoList = append(f.aggInfoList, NestedAgg{Name: pathToNest})
			}
			return targetPath
		}
		return currentLogicalPath // No transition needed if paths are same
	} else { // Target is not nested
		if currentLogicalPath != "" {
			f.aggInfoList = append(f.aggInfoList, ReverseNestedAgg{Name: "reverse_nested"}) // Standardized name
		}
		return "" // New path is root
	}
}

// EsAgg generates the Elasticsearch aggregation structure.
func (f *FormatFactory) EsAgg(aggregates metadata.Aggregates) (string, elastic.Aggregation, error) {
	if len(aggregates) == 0 {
		return "", nil, errors.New("aggregate_method_list is empty")
	}
	aggDef := aggregates[0]

	f.aggInfoList = make(aggInfoList, 0)
	currentLogicalPath := ""

	// Determine metric field and info early for some decisions
	metricFieldForOp := aggDef.Field
	if metricFieldForOp == "" && aggDef.Name == Count {
		metricFieldForOp = f.valueField
	}
	metricFieldInfo := f.getFieldPathInfo(metricFieldForOp)

	// Order of processing: Dimensions first, then Time Aggregation, then Metric Aggregation.
	// This makes dimensions outermost if they exist.
	var metricAggAttached bool // Flag to track if the primary metric has been attached to a TermAgg

	// 1. Dimension Aggregations
	for i, dim := range aggDef.Dimensions {
		if dim == labels.MetricName {
			continue
		}
		decodedDim := dim
		if f.decode != nil {
			decodedDim = f.decode(dim)
		}
		dimInfo := f.getFieldPathInfo(decodedDim)

		currentLogicalPath = f.handlePathTransition(currentLogicalPath, dimInfo.Path, dimInfo.IsNested)

		termAgg := TermAgg{Name: dimInfo.FieldName}
		// Apply orders:
		// If this is the first dimension (i=0), it's the outermost dimension group.
		// Orders by this dimension's key (KeyValue) or by metric (FieldValue) apply here.
		if i == 0 {
			for _, order := range f.orders {
				if dimInfo.OriginalField == order.Name {
					termAgg.Orders = append(termAgg.Orders, metadata.Order{Name: KeyValue, Ast: order.Ast})
				} else if order.Name == FieldValue {
					termAgg.Orders = append(termAgg.Orders, order)
				}
			}
		} else {
			// For inner dimensions, only apply order if it's explicitly for this dimension's key.
			for _, order := range f.orders {
				if dimInfo.OriginalField == order.Name {
					termAgg.Orders = append(termAgg.Orders, metadata.Order{Name: KeyValue, Ast: order.Ast})
				}
			}
		}

		// Check if this dimension is also the metric field, AND the metric field itself is nested.
		// This is for scenarios like scene_7 where the metric on a nested dimension field
		// should be a sibling to further reverse_nested operations from that nested context.
		if !metricAggAttached && aggDef.Name != DateHistogram &&
			dimInfo.OriginalField == metricFieldInfo.OriginalField && // Current dim is the metric field
			metricFieldInfo.IsNested && // AND metric field itself is nested
			aggDef.Window == 0 { // AND there is no time windowing for this aggregate definition

			termAgg.AttachedValueAgg = &ValueAgg{
				Name:     FieldValue, // Standard name for the metric value aggregation
				FuncType: aggDef.Name,
				Field:    metricFieldInfo.OriginalField, // Use the original field of the metric
				Args:     aggDef.Args,
			}
			metricAggAttached = true
			log.Debugf(f.ctx, "EsAgg: Attached ValueAgg for NESTED field '%s' (NO window) to TermAgg for dimension '%s'", metricFieldInfo.OriginalField, dimInfo.OriginalField)
		}

		f.aggInfoList = append(f.aggInfoList, termAgg)
	}

	// 2. Time Aggregation (if window > 0)
	if aggDef.Window > 0 && !aggDef.Without {
		// Time aggregation should generally be at the root or directly within the current logical path
		// established by dimensions. If dimensions created a nested path, time agg might be inside or outside.
		// For typical TSDB queries (group by dim, then time bucket), time agg is *inside* the dimension.
		// If the current path is nested, and we want time agg outside, transition to root.
		// However, if the test expects Term(Time(Value)), TimeAgg should be added after TermAgg
		// within the same logical path or by transitioning as needed.
		// The current `currentLogicalPath` is the path of the *innermost* dimension.
		// If time bucketing is desired within each such dimensional group, we add it here.
		// If time bucketing should be more "global" or at root, a path transition would be needed.
		// For RangeQueryAndAggregates, the expected is Term(Time(Value)), meaning TimeAgg is *inner* to TermAgg.
		// So, we add TimeAgg now, inheriting currentLogicalPath if it implies nesting, or it applies at current level.
		// Critically, date_histogram in ES isn't "path" aware like nested/terms. It operates on a root-level date field.
		// So, if currentLogicalPath is nested, date_histogram still uses the root f.timeField.Name.
		// The `handlePathTransition` to root before time agg logic in previous versions was to ensure
		// time agg wasn't *incorrectly* thought to be part of a nested structure for its *own* operation.
		// Given Term(Time(Value)) order:
		// - Dimensions establish `currentLogicalPath`.
		// - TimeAgg is added. It will become a sub-aggregation of the innermost dimension.
		// No explicit path transition for TimeAgg itself, as it uses a global time field.
		f.aggInfoList = append(f.aggInfoList, TimeAgg{
			Name:     f.timeField.Name, // This is the key for the time agg block
			Window:   aggDef.Window,
			TimeZone: aggDef.TimeZone,
		})
	}

	// 3. Metric Aggregation (only if not already attached)
	// This handles cases where the metric field is not a dimension, or for non-TermAgg scenarios.
	if !metricAggAttached && aggDef.Name != DateHistogram { // DateHistogram is handled by TimeAgg
		fieldForMetricValueAgg := metricFieldForOp
		if aggDef.Field == "" && aggDef.Name == Count {
			// Uses f.valueField (via metricFieldForOp) or explicitly set aggDef.Field.
		} else if aggDef.Field != "" {
			fieldForMetricValueAgg = aggDef.Field // Prioritize explicitly set aggDef.Field
		}

		// The currentLogicalPath is the state after processing all dimensions and time aggs.
		// The metric (ValueAgg) needs to be placed in the context of its own field (metricFieldInfo).
		// Call handlePathTransition to add any necessary NestedAgg or ReverseNestedAgg
		// to transition from currentLogicalPath to metricFieldInfo.Path.
		currentLogicalPath = f.handlePathTransition(currentLogicalPath, metricFieldInfo.Path, metricFieldInfo.IsNested)

		f.aggInfoList = append(f.aggInfoList, ValueAgg{
			Name:     FieldValue, // Standard name for the metric value aggregation
			FuncType: aggDef.Name,
			Field:    fieldForMetricValueAgg,
			Args:     aggDef.Args,
		})
		log.Debugf(f.ctx, "EsAgg: Added standalone ValueAgg for field '%s' with path '%s'", fieldForMetricValueAgg, currentLogicalPath)
	}

	log.Debugf(f.ctx, "EsAgg: Constructed aggInfoList: %+v", f.aggInfoList)

	_, mainAgg, err := f.Agg()
	if err != nil {
		return "", nil, err
	}

	// Determine outermostAggName:
	// Priority: First Dimension > Time Window > Metric Field Path/Name
	outermostAggName := ""
	if len(aggDef.Dimensions) > 0 {
		firstDimDecoded := aggDef.Dimensions[0]
		if f.decode != nil {
			firstDimDecoded = f.decode(firstDimDecoded)
		}
		firstDimInfo := f.getFieldPathInfo(firstDimDecoded)
		if firstDimInfo.IsNested { // If first dim is like "events.name", path "events" is outer.
			outermostAggName = firstDimInfo.Path
		} else { // If first dim is like "name", field "name" is outer.
			outermostAggName = firstDimInfo.FieldName
		}
	} else if aggDef.Window > 0 && !aggDef.Without {
		outermostAggName = f.timeField.Name // Key used for the TimeAgg block
	} else if aggDef.Name != DateHistogram { // Metric is the only component
		if metricFieldInfo.IsNested {
			outermostAggName = metricFieldInfo.Path
		} else {
			if metricFieldForOp != "" {
				outermostAggName = metricFieldForOp
			} else {
				outermostAggName = FieldValue // Fallback for global metric
			}
		}
	} else {
		return "", nil, errors.New("unable to determine outermost aggregation name")
	}

	log.Debugf(f.ctx, "EsAgg: Determined outermost name: %s", outermostAggName)
	return outermostAggName, mainAgg, err
}

func (f *FormatFactory) Orders() metadata.Orders {
	orders := make(metadata.Orders, 0, len(f.orders))
	for _, order := range f.orders {
		if order.Name == FieldValue {
			order.Name = f.valueField
		} else if order.Name == FieldTime {
			order.Name = f.timeField.Name
		}

		if _, ok := f.mapping[order.Name]; ok {
			orders = append(orders, order)
		}
	}
	return orders
}

func (f *FormatFactory) timeFieldUnix(t time.Time) (u int64) {
	switch f.timeField.Unit {
	case function.Millisecond:
		u = t.UnixMilli()
	case function.Microsecond:
		u = t.UnixMicro()
	case function.Nanosecond:
		u = t.UnixNano()
	default:
		u = t.Unix()
	}

	return
}

func (f *FormatFactory) getQuery(key string, qs ...elastic.Query) (q elastic.Query) {
	if len(qs) == 0 {
		return q
	}

	switch key {
	case Must:
		if len(qs) == 1 {
			q = qs[0]
		} else {
			q = elastic.NewBoolQuery().Must(qs...)
		}
	case Should:
		if len(qs) == 1 {
			q = qs[0]
		} else {
			q = elastic.NewBoolQuery().Should(qs...)
		}
	case MustNot:
		q = elastic.NewBoolQuery().MustNot(qs...)
	}
	return q
}

// Query 把 ts 的 conditions 转换成 es 查询
func (f *FormatFactory) Query(allConditions metadata.AllConditions) (elastic.Query, error) {
	orQuery := make([]elastic.Query, 0, len(allConditions))

	for _, conditions := range allConditions {
		nestedPathQueries := make(map[string][]elastic.Query) // Stores queries that should be ANDed under a specific nested path
		rootLevelQueries := make([]elastic.Query, 0)          // Stores non-nested queries and fully formed must_not(nested) queries

		for _, con := range conditions {
			key := con.DimensionName
			if f.decode != nil {
				key = f.decode(key)
			}

			nestedPath := f.NestedField(con.DimensionName)

			var q elastic.Query
			isNegativeOperator := false
			positiveOperator := con.Operator // Used if we handle negativity at a higher level for nested queries

			switch con.Operator {
			case structured.ConditionNotEqual,
				structured.ConditionNotContains,
				structured.ConditionNotRegEqual,
				structured.ConditionNotExisted:
				isNegativeOperator = true
				// Determine the positive equivalent operator for nested must_not cases
				switch con.Operator {
				case structured.ConditionNotEqual: // match_phrase
					positiveOperator = structured.ConditionEqual
				case structured.ConditionNotContains: // wildcard or match_phrase
					positiveOperator = structured.ConditionContains
				case structured.ConditionNotRegEqual: // regexp
					positiveOperator = structured.ConditionRegEqual
				case structured.ConditionNotExisted: // exists
					positiveOperator = structured.ConditionExisted
				}
			}

			if nestedPath != "" && isNegativeOperator && con.Operator != structured.ConditionNotExisted {
				// Handle must_not(nested(positive_condition)) scenario
				// Construct the positive query part first
				positiveQueries := make([]elastic.Query, 0)
				fieldType, _ := f.mapping[key]
				for _, value := range con.Value {
					var positiveQueryPart elastic.Query
					switch positiveOperator {
					case structured.ConditionEqual: // from NotEqual
						if con.IsPrefix {
							positiveQueryPart = elastic.NewMatchPhrasePrefixQuery(key, value)
						} else {
							positiveQueryPart = elastic.NewMatchPhraseQuery(key, value)
						}
					case structured.ConditionContains: // from NotContains
						if fieldType == KeyWord {
							value = fmt.Sprintf("*%s*", value)
						}
						if !con.IsWildcard && fieldType == Text {
							if con.IsPrefix {
								positiveQueryPart = elastic.NewMatchPhrasePrefixQuery(key, value)
							} else {
								positiveQueryPart = elastic.NewMatchPhraseQuery(key, value)
							}
						} else {
							positiveQueryPart = elastic.NewWildcardQuery(key, value)
						}
					case structured.ConditionRegEqual: // from NotRegEqual
						positiveQueryPart = elastic.NewRegexpQuery(key, value)
					// Add other positive operators if needed for other negative ones
					default:
						return nil, fmt.Errorf("unhandled positive operator mapping for %s -> %s", con.Operator, positiveOperator)
					}
					if positiveQueryPart != nil {
						positiveQueries = append(positiveQueries, positiveQueryPart)
					}
				}
				if len(positiveQueries) > 0 {
					positiveBoolQuery := f.getQuery(Should, positiveQueries...) // If multiple values for NotEqual, they are ORed for the positive match inside nested
					nestedQ := elastic.NewNestedQuery(nestedPath, positiveBoolQuery)
					q = f.getQuery(MustNot, nestedQ)
					rootLevelQueries = append(rootLevelQueries, q)
				}
				continue // Handled this condition, move to next
			}

			// Original logic for non-special-nested-must_not or all positive queries
			switch con.Operator {
			case structured.ConditionExisted:
				q = elastic.NewExistsQuery(key)
			case structured.ConditionNotExisted:
				// For ConditionNotExisted, if it's nested, it should be must_not(nested(exists(...)))
				if nestedPath != "" {
					existsQuery := elastic.NewExistsQuery(key)
					nestedExistsQuery := elastic.NewNestedQuery(nestedPath, existsQuery)
					q = f.getQuery(MustNot, nestedExistsQuery)
				} else {
					q = f.getQuery(MustNot, elastic.NewExistsQuery(key))
				}
			default:
				fieldType, ok := f.mapping[key]
				isExistsQuery := true // Should this be used for empty value check?
				if ok && (fieldType == Text || fieldType == KeyWord) {
					isExistsQuery = false
				}

				queries := make([]elastic.Query, 0)
				for _, value := range con.Value {
					var queryPart elastic.Query
					if con.DimensionName != "" {
						if value == "" && isExistsQuery && (con.Operator == structured.ConditionEqual || con.Operator == structured.ConditionContains || con.Operator == structured.ConditionNotEqual || con.Operator == structured.ConditionNotContains) {
							existsQ := elastic.NewExistsQuery(key)
							switch con.Operator {
							case structured.ConditionEqual, structured.ConditionContains:
								queryPart = f.getQuery(MustNot, existsQ)
							case structured.ConditionNotEqual, structured.ConditionNotContains:
								queryPart = f.getQuery(Must, existsQ)
							default:
								// This case should ideally not be reached if handled by prior empty string checks.
							}
						} else {
							switch con.Operator {
							case structured.ConditionEqual, structured.ConditionNotEqual:
								if con.IsPrefix {
									queryPart = elastic.NewMatchPhrasePrefixQuery(key, value)
								} else {
									queryPart = elastic.NewMatchPhraseQuery(key, value)
								}
							case structured.ConditionContains, structured.ConditionNotContains:
								if fieldType == KeyWord {
									value = fmt.Sprintf("*%s*", value)
								}
								if !con.IsWildcard && fieldType == Text {
									if con.IsPrefix {
										queryPart = elastic.NewMatchPhrasePrefixQuery(key, value)
									} else {
										queryPart = elastic.NewMatchPhraseQuery(key, value)
									}
								} else {
									queryPart = elastic.NewWildcardQuery(key, value)
								}
							case structured.ConditionRegEqual, structured.ConditionNotRegEqual:
								queryPart = elastic.NewRegexpQuery(key, value)
							case structured.ConditionGt:
								queryPart = elastic.NewRangeQuery(key).Gt(value)
							case structured.ConditionGte:
								queryPart = elastic.NewRangeQuery(key).Gte(value)
							case structured.ConditionLt:
								queryPart = elastic.NewRangeQuery(key).Lt(value)
							case structured.ConditionLte:
								queryPart = elastic.NewRangeQuery(key).Lte(value)
							default:
								return nil, fmt.Errorf("operator is not supported: %+v", con)
							}
						}
					} else {
						queryPart = elastic.NewQueryStringQuery(value)
					}
					if queryPart != nil {
						queries = append(queries, queryPart)
					}
				}

				switch con.Operator {
				case structured.ConditionEqual, structured.ConditionContains, structured.ConditionRegEqual:
					q = f.getQuery(Should, queries...)
				case structured.ConditionNotEqual, structured.ConditionNotContains, structured.ConditionNotRegEqual:
					q = f.getQuery(MustNot, queries...)
				case structured.ConditionGt, structured.ConditionGte, structured.ConditionLt, structured.ConditionLte:
					q = f.getQuery(Must, queries...)
					// ConditionExisted and ConditionNotExisted are handled above or by special nested logic.
				default:
					// This path might be taken by ConditionExisted/NotExisted if not handled by special nested logic.
					// Ensure q is not nil if those were processed to avoid erroring out.
					if q == nil { // if q was set by ConditionExisted/NotExisted, this won't be true
						return nil, fmt.Errorf("operator is not supported or q remained nil: %+v", con)
					}
				}
			}

			if q == nil {
				continue // Skip if no query was generated for this condition
			}

			if nestedPath != "" && !(isNegativeOperator && con.Operator != structured.ConditionNotExisted) && con.Operator != structured.ConditionNotExisted {
				// This is a positive condition on a nested field, or a negative one that isn't handled as must_not(nested())
				// or a NotExisted on nested (which becomes must_not(nested(exists))) that still needs grouping by path.
				// For ConditionNotExisted on a nested path, 'q' is already must_not(nested(exists(key))). This should not go into nestedPathQueries.
				// It should go to rootLevelQueries.
				// The check `con.Operator != structured.ConditionNotExisted` is because that specific case now forms a root-level query.

				// If q is already a fully formed nested query (like from NotExisted), add to rootLevelQueries.
				// This distinction is getting complicated. Let's simplify:
				// If the condition resulted in a query `q` that is *not* one of the special must_not(nested) cases handled above,
				// and it *is* for a nested field, then group it for later wrapping. Otherwise, it's a root level query.

				// Re-evaluating: The special handling for negative nested queries already puts them in rootLevelQueries.
				// The special handling for ConditionNotExisted on nested path also puts it in rootLevelQueries.
				// So, if nestedPath != "" here, it means it's a *positive* condition on a nested field.
				nestedPathQueries[nestedPath] = append(nestedPathQueries[nestedPath], q)
			} else {
				rootLevelQueries = append(rootLevelQueries, q)
			}
		}

		// Combine queries for each nested path
		// To ensure consistent order for tests, sort the paths
		paths := make([]string, 0, len(nestedPathQueries))
		for path := range nestedPathQueries {
			paths = append(paths, path)
		}
		sort.Strings(paths)

		for _, path := range paths { // Iterate over sorted paths
			nqs := nestedPathQueries[path]
			if len(nqs) > 0 {
				pathQuery := f.getQuery(Must, nqs...)
				nestedQuery := elastic.NewNestedQuery(path, pathQuery)
				rootLevelQueries = append(rootLevelQueries, nestedQuery)
			}
		}

		if len(rootLevelQueries) > 0 {
			andQuery := f.getQuery(Must, rootLevelQueries...)
			if andQuery != nil {
				orQuery = append(orQuery, andQuery)
			}
		}
	}

	finalQuery := f.getQuery(Should, orQuery...)
	return finalQuery, nil
}

func (f *FormatFactory) Sample() (prompb.Sample, error) {
	var (
		err error
		ok  bool

		timestamp interface{}
		value     interface{}

		sample = prompb.Sample{}
	)

	// 如果是非 prom 计算场景，则提前退出
	if f.isReference {
		return sample, nil
	}

	if value, ok = f.data[f.valueField]; ok {
		switch value.(type) {
		case float64:
			sample.Value = value.(float64)
		case int64:
			sample.Value = float64(value.(int64))
		case int:
			sample.Value = float64(value.(int))
		case string:
			sample.Value, err = strconv.ParseFloat(value.(string), 64)
			if err != nil {
				return sample, err
			}
		default:
			return sample, fmt.Errorf("value key %s type is error: %T, %v", f.valueField, value, value)
		}
	} else {
		sample.Value = 0
	}

	if timestamp, ok = f.data[f.timeField.Name]; ok {
		switch timestamp.(type) {
		case int64:
			sample.Timestamp = timestamp.(int64)
		case int:
			sample.Timestamp = int64(timestamp.(int))
		case float64:
			sample.Timestamp = int64(timestamp.(float64))
		case string:
			v, parseErr := strconv.ParseInt(timestamp.(string), 10, 64)
			if parseErr != nil {
				return sample, parseErr
			}
			sample.Timestamp = v
		default:
			return sample, fmt.Errorf("timestamp key type is error: %T, %v", timestamp, timestamp)
		}
		sample.Timestamp = f.toMillisecond(sample.Timestamp)
	} else {
		return sample, fmt.Errorf("timestamp is empty %s", f.timeField.Name)
	}

	return sample, nil
}

func (f *FormatFactory) Labels() (lbs *prompb.Labels, err error) {
	lbl := make([]string, 0)
	for k := range f.data {
		// 只有 promEngine 查询的场景需要跳过该字段
		if !f.isReference {
			if k == f.valueField {
				continue
			}
			if k == f.timeField.Name {
				continue
			}
		}

		if f.encode != nil {
			k = f.encode(k)
		}

		lbl = append(lbl, k)
	}

	sort.Strings(lbl)

	lbs = &prompb.Labels{
		Labels: make([]prompb.Label, 0, len(lbl)),
	}

	for _, k := range lbl {
		var value string
		d := f.data[k]

		if d == nil {
			continue
		}

		switch d.(type) {
		case string:
			value = fmt.Sprintf("%s", d)
		case float64, float32:
			value = fmt.Sprintf("%.f", d)
		case int64, int32, int:
			value = fmt.Sprintf("%d", d)
		case []interface{}:
			o, _ := json.Marshal(d)
			value = fmt.Sprintf("%s", o)
		default:
			err = fmt.Errorf("dimensions key type is error: %T, %v", d, d)
			return
		}

		lbs.Labels = append(lbs.Labels, prompb.Label{
			Name:  k,
			Value: value,
		})
	}

	return
}

func (f *FormatFactory) GetTimeField() metadata.TimeField {
	return f.timeField
}
