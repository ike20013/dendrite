// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package roomserver

import (
	"github.com/ike20013/dendrite/external/caching"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/setup/jetstream"
	"github.com/ike20013/dendrite/setup/process"
	"github.com/sirupsen/logrus"

	"github.com/ike20013/dendrite/roomserver/api"
	"github.com/ike20013/dendrite/roomserver/internal"
	"github.com/ike20013/dendrite/roomserver/storage"
)

// NewInternalAPI returns a concrete implementation of the external API.
//
// Many of the methods provided by this API depend on access to a federation API, and so
// you may wish to call `SetFederationAPI` on the returned struct to avoid nil-dereference errors.
func NewInternalAPI(
	processContext *process.ProcessContext,
	cfg *config.Dendrite,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	caches caching.RoomServerCaches,
	enableMetrics bool,
) api.RoomserverInternalAPI {
	roomserverDB, err := storage.Open(processContext.Context(), cm, &cfg.RoomServer.Database, caches)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to room server db")
	}

	js, nc := natsInstance.Prepare(processContext, &cfg.Global.JetStream)

	return internal.NewRoomserverAPI(
		processContext, cfg, roomserverDB, js, nc, caches, enableMetrics,
	)
}
