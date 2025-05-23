// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

const (
	// AsyncTask 异步任务
	AsyncTask = "async"
	// PeriodicTask 周期任务
	PeriodicTask = "periodic"
	// DaemonTask 常驻任务
	DaemonTask = "daemon"
	// 路由前缀
	RouterPrefix     = "/bmw"
	TaskRouterPrefix = "/task"
	// DeleteAllTaskPath 删除所有任务
	DeleteAllTaskPath = "/all"
	// DaemonTaskReloadPath 常驻任务重载(重新启动)
	DaemonTaskReloadPath = "/daemon/reload"
)
