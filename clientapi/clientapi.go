// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"github.com/ike20013/dendrite/external/httputil"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/setup/process"
	userapi "github.com/ike20013/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/fclient"

	appserviceAPI "github.com/ike20013/dendrite/appservice/api"
	"github.com/ike20013/dendrite/clientapi/api"
	"github.com/ike20013/dendrite/clientapi/producers"
	"github.com/ike20013/dendrite/clientapi/routing"
	"github.com/ike20013/dendrite/external/transactions"
	federationAPI "github.com/ike20013/dendrite/federationapi/api"
	roomserverAPI "github.com/ike20013/dendrite/roomserver/api"
	"github.com/ike20013/dendrite/setup/jetstream"
)

// AddPublicRoutes sets up and registers HTTP handlers for the ClientAPI component.
func AddPublicRoutes(
	processContext *process.ProcessContext,
	routers httputil.Routers,
	cfg *config.Dendrite,
	natsInstance *jetstream.NATSInstance,
	federation fclient.FederationClient,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
	transactionsCache *transactions.Cache,
	fsAPI federationAPI.ClientFederationAPI,
	userAPI userapi.ClientUserAPI,
	userDirectoryProvider userapi.QuerySearchProfilesAPI,
	extRoomsProvider api.ExtraPublicRoomsProvider, enableMetrics bool,
) {
	js, natsClient := natsInstance.Prepare(processContext, &cfg.Global.JetStream)

	syncProducer := &producers.SyncAPIProducer{
		JetStream:              js,
		TopicReceiptEvent:      cfg.Global.JetStream.Prefixed(jetstream.OutputReceiptEvent),
		TopicSendToDeviceEvent: cfg.Global.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent),
		TopicTypingEvent:       cfg.Global.JetStream.Prefixed(jetstream.OutputTypingEvent),
		TopicPresenceEvent:     cfg.Global.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		UserAPI:                userAPI,
		ServerName:             cfg.Global.ServerName,
	}

	routing.Setup(
		routers,
		cfg, rsAPI, asAPI,
		userAPI, userDirectoryProvider, federation,
		syncProducer, transactionsCache, fsAPI,
		extRoomsProvider, natsClient, enableMetrics,
	)
}
