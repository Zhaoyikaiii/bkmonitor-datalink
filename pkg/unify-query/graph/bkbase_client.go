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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/tsdb"
)

// BKBaseClient BKBase SurrealDB 客户端
type BKBaseClient struct {
	config     *ClientConfig
	httpClient *http.Client
}

// NewBKBaseClient 创建 BKBase SurrealDB 客户端
func NewBKBaseClient(cfg *ClientConfig) (*BKBaseClient, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("address is required for BKBase client")
	}

	return &BKBaseClient{
		config: cfg,
		httpClient: &http.Client{
			Timeout: tsdb.GraphTimeout,
		},
	}, nil
}

func (c *BKBaseClient) Connect(ctx context.Context) error {
	return c.Health(ctx)
}

func (c *BKBaseClient) Close() error {
	return nil
}

type BKBaseRequest struct {
	SQL  string         `json:"sql"`
	Vars map[string]any `json:"vars,omitempty"`
}

type BKBaseResponse struct {
	Result  bool            `json:"result"`
	Message string          `json:"message"`
	Code    string          `json:"code"`
	Data    json.RawMessage `json:"data"`
}

func (c *BKBaseClient) Execute(ctx context.Context, query string, vars map[string]any) (any, error) {
	reqBody := &BKBaseRequest{
		SQL:  query,
		Vars: vars,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.Address+"/sql", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
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

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var bkResp BKBaseResponse
	if err := json.Unmarshal(respBody, &bkResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !bkResp.Result {
		return nil, fmt.Errorf("query failed: %s (code: %s)", bkResp.Message, bkResp.Code)
	}

	var result any
	if err := json.Unmarshal(bkResp.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return result, nil
}

// Health 检查连接健康状态
func (c *BKBaseClient) Health(ctx context.Context) error {
	_, err := c.Execute(ctx, "INFO FOR DB", nil)
	return err
}
