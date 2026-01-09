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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/tsdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

// NativeClient 原生 SurrealDB 客户端
type NativeClient struct {
	config     *ClientConfig
	httpClient *http.Client
	timeout    time.Duration
}

// NewNativeClient 创建原生 SurrealDB 客户端
func NewNativeClient(cfg *ClientConfig) (*NativeClient, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("address is required for native client")
	}

	timeout := tsdb.GraphTimeout
	return &NativeClient{
		config: cfg,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}, nil
}

// SetTimeout 设置查询超时时间
func (c *NativeClient) SetTimeout(d time.Duration) {
	c.timeout = d
	c.httpClient.Timeout = d
}

// Connect 建立连接
func (c *NativeClient) Connect(ctx context.Context) error {
	return c.Health(ctx)
}

// Close 关闭客户端
func (c *NativeClient) Close() error {
	return nil
}

// NativeResponse 原生 SurrealDB API 响应
type NativeResponse struct {
	Status string `json:"status"`
	Result any    `json:"result"`
	Detail string `json:"detail,omitempty"`
}

// Execute 执行 SurrealQL 查询
func (c *NativeClient) Execute(ctx context.Context, query string, vars map[string]any) (any, error) {
	var err error
	ctx, span := trace.NewSpan(ctx, "graph-surrealdb-execute")
	defer span.End(&err)

	span.Set("address", c.config.Address)
	span.Set("namespace", c.config.Namespace)
	span.Set("database", c.config.Database)

	// Build the full SQL with namespace and database
	fullSQL := fmt.Sprintf("USE NS %s DB %s; %s", c.config.Namespace, c.config.Database, query)
	span.Set("query", query)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.Address+"/sql", bytes.NewReader([]byte(fullSQL)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Accept", "application/json")

	for k, v := range c.config.Headers {
		req.Header.Set(k, v)
	}

	if c.config.Username != "" && c.config.Password != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	span.Set("status-code", resp.StatusCode)

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	span.Set("response-size", len(respBody))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var results []NativeResponse
	if err := json.Unmarshal(respBody, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Check for errors (skip USE statement result)
	for i, result := range results {
		if result.Status == "ERR" {
			err = fmt.Errorf("SQL error in statement %d: %s", i, result.Detail)
			return nil, err
		}
	}

	// Return the result of the actual query (skip USE statement)
	if len(results) > 1 {
		return results[1].Result, nil
	}
	if len(results) == 1 {
		return results[0].Result, nil
	}

	return nil, nil
}

// Health 检查连接健康状态
func (c *NativeClient) Health(ctx context.Context) error {
	_, err := c.Execute(ctx, "INFO FOR DB", nil)
	return err
}
