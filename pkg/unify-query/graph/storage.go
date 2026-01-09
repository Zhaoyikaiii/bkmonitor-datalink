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
	"sync"
	"time"
)

var (
	storageMap  = make(map[string]*Storage)
	storageLock = new(sync.RWMutex)
)

// Storage represents a graph storage configuration
type Storage struct {
	Type      ClientType
	Address   string
	Namespace string
	Database  string
	Username  string
	Password  string
	Headers   map[string]string

	Timeout   time.Duration
	MaxLimit  int
	Tolerance int64

	Instance *Instance
}

// GetStorage returns a storage by ID
func GetStorage(storageID string) (*Storage, error) {
	storageLock.RLock()
	defer storageLock.RUnlock()

	storage, ok := storageMap[storageID]
	if !ok {
		return nil, fmt.Errorf("graph storage not found: %s", storageID)
	}
	return storage, nil
}

// SetStorage sets a storage by ID
func SetStorage(storageID string, storage *Storage) {
	storageLock.Lock()
	defer storageLock.Unlock()

	storageMap[storageID] = storage
}

// GetInstance returns a graph instance by storage ID
func GetInstance(ctx context.Context, storageID string) (*Instance, error) {
	storage, err := GetStorage(storageID)
	if err != nil {
		return nil, err
	}

	if storage.Instance != nil {
		return storage.Instance, nil
	}

	// Create a new instance
	cfg := &ClientConfig{
		Type:      storage.Type,
		Address:   storage.Address,
		Namespace: storage.Namespace,
		Database:  storage.Database,
		Username:  storage.Username,
		Password:  storage.Password,
		Headers:   storage.Headers,
	}

	instance, err := NewInstance(ctx, cfg)
	if err != nil {
		return nil, err
	}

	storage.Instance = instance
	return instance, nil
}

// ReloadStorage reloads the graph storage configuration
func ReloadStorage(ctx context.Context, storages map[string]*Storage) error {
	storageLock.Lock()
	defer storageLock.Unlock()

	// Close existing instances
	for _, storage := range storageMap {
		if storage.Instance != nil {
			_ = storage.Instance.Close()
		}
	}

	// Clear and reload
	storageMap = make(map[string]*Storage)
	for id, storage := range storages {
		storageMap[id] = storage
	}

	return nil
}

// CloseAll closes all graph instances
func CloseAll() {
	storageLock.Lock()
	defer storageLock.Unlock()

	for _, storage := range storageMap {
		if storage.Instance != nil {
			_ = storage.Instance.Close()
		}
	}
}
