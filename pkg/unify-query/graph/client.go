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
)

// Client SurrealDB 客户端接口
type Client interface {
	Connect(ctx context.Context) error
	Close() error
	Execute(ctx context.Context, query string, vars map[string]any) (any, error)
	Health(ctx context.Context) error
	SetTimeout(d time.Duration)
}

// ClientType 客户端类型
type ClientType string

const (
	ClientTypeBKBase ClientType = "bkbase" // BKBase SurrealDB
	ClientTypeNative ClientType = "native" // 原生 SurrealDB
)

// ClientConfig 客户端配置
type ClientConfig struct {
	Type      ClientType        `json:"type"`
	Address   string            `json:"address"`
	Namespace string            `json:"namespace"`
	Database  string            `json:"database"`
	Username  string            `json:"username"`
	Password  string            `json:"password"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// NewClient 根据配置创建客户端
func NewClient(cfg *ClientConfig) (Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("client config is nil")
	}

	switch cfg.Type {
	case ClientTypeBKBase:
		return NewBKBaseClient(cfg)
	case ClientTypeNative:
		return NewNativeClient(cfg)
	default:
		return nil, fmt.Errorf("unsupported client type: %s", cfg.Type)
	}
}
