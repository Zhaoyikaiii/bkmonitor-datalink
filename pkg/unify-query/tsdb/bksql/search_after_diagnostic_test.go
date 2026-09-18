package bksql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/bksql"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb/bksql/sql_expr"
)

func TestSearchAfterUsesLegacyCompositeCursorWithoutUniqueKey(t *testing.T) {
	query := &metadata.Query{
		DB:            "22684_path_log",
		Measurement:   sql_expr.Doris,
		Field:         "dtEventTimeStamp",
		IsSearchAfter: true,
		Orders: metadata.Orders{{
			Name: sql_expr.FieldTime,
			Ast:  false,
		}},
	}

	sql, err := bksql.NewQueryFactory(metadata.InitHashID(context.Background()), query).
		WithRangeTime(time.UnixMilli(1789142400000), time.UnixMilli(1789228799000)).
		WithFieldsMap(metadata.FieldsMap{
			"dtEventTimeStamp": {FieldType: sql_expr.DorisTypeBigInt},
			"gseIndex":         {FieldType: sql_expr.DorisTypeBigInt},
			"iterationIndex":   {FieldType: sql_expr.DorisTypeBigInt},
			"log":              {FieldType: sql_expr.DorisTypeText},
		}).SQL()

	require.NoError(t, err)
	require.Contains(t, sql, "`gseIndex` AS `__search_after_1`")
	require.Contains(t, sql, "`iterationIndex` AS `__search_after_2`")
	require.NotContains(t, sql, sql_expr.SearchAfterTieBreaker)
}

func TestSearchAfterFallsBackToOffsetWithoutCursorFields(t *testing.T) {
	from := 7
	query := &metadata.Query{
		DB:            "legacy_path_log",
		Measurement:   sql_expr.Doris,
		Field:         "dtEventTimeStamp",
		From:          from,
		Size:          10,
		IsSearchAfter: true,
		Orders: metadata.Orders{{
			Name: sql_expr.FieldTime,
			Ast:  false,
		}},
	}

	sql, err := bksql.NewQueryFactory(metadata.InitHashID(context.Background()), query).
		WithRangeTime(time.UnixMilli(1789142400000), time.UnixMilli(1789228799000)).
		WithFieldsMap(metadata.FieldsMap{
			"dtEventTimeStamp": {FieldType: sql_expr.DorisTypeBigInt},
			"log":              {FieldType: sql_expr.DorisTypeText},
		}).SQL()

	require.NoError(t, err)
	require.Contains(t, sql, "LIMIT 10 OFFSET 7")
	require.NotContains(t, sql, "__search_after_")
}
