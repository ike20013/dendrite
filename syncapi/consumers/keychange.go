// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"encoding/json"

	"github.com/getsentry/sentry-go"
	roomserverAPI "github.com/ike20013/dendrite/roomserver/api"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/setup/jetstream"
	"github.com/ike20013/dendrite/setup/process"
	"github.com/ike20013/dendrite/syncapi/notifier"
	"github.com/ike20013/dendrite/syncapi/storage"
	"github.com/ike20013/dendrite/syncapi/streams"
	"github.com/ike20013/dendrite/syncapi/types"
	"github.com/ike20013/dendrite/userapi/api"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// OutputKeyChangeEventConsumer consumes events that originated in the key server.
type OutputKeyChangeEventConsumer struct {
	ctx       context.Context
	jetstream nats.JetStreamContext
	durable   string
	topic     string
	db        storage.Database
	notifier  *notifier.Notifier
	stream    streams.StreamProvider
	rsAPI     roomserverAPI.SyncRoomserverAPI
}

// NewOutputKeyChangeEventConsumer creates a new OutputKeyChangeEventConsumer.
// Call Start() to begin consuming from the key server.
func NewOutputKeyChangeEventConsumer(
	process *process.ProcessContext,
	cfg *config.SyncAPI,
	topic string,
	js nats.JetStreamContext,
	rsAPI roomserverAPI.SyncRoomserverAPI,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
) *OutputKeyChangeEventConsumer {
	s := &OutputKeyChangeEventConsumer{
		ctx:       process.Context(),
		jetstream: js,
		durable:   cfg.Matrix.JetStream.Durable("SyncAPIKeyChangeConsumer"),
		topic:     topic,
		db:        store,
		rsAPI:     rsAPI,
		notifier:  notifier,
		stream:    stream,
	}

	return s
}

// Start consuming from the key server
func (s *OutputKeyChangeEventConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (s *OutputKeyChangeEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var m api.DeviceMessage
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		logrus.WithError(err).Errorf("failed to read device message from key change topic")
		return true
	}
	if m.DeviceKeys == nil && m.OutputCrossSigningKeyUpdate == nil {
		// This probably shouldn't happen but stops us from panicking if we come
		// across an update that doesn't satisfy either types.
		return true
	}
	switch m.Type {
	case api.TypeCrossSigningUpdate:
		return s.onCrossSigningMessage(m, m.DeviceChangeID)
	case api.TypeDeviceKeyUpdate:
		fallthrough
	default:
		return s.onDeviceKeyMessage(m, m.DeviceChangeID)
	}
}

func (s *OutputKeyChangeEventConsumer) onDeviceKeyMessage(m api.DeviceMessage, deviceChangeID int64) bool {
	if m.DeviceKeys == nil {
		return true
	}
	output := m.DeviceKeys
	// work out who we need to notify about the new key
	var queryRes roomserverAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(s.ctx, &roomserverAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		logrus.WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		sentry.CaptureException(err)
		return true
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return true
}

func (s *OutputKeyChangeEventConsumer) onCrossSigningMessage(m api.DeviceMessage, deviceChangeID int64) bool {
	output := m.CrossSigningKeyUpdate
	// work out who we need to notify about the new key
	var queryRes roomserverAPI.QuerySharedUsersResponse
	err := s.rsAPI.QuerySharedUsers(s.ctx, &roomserverAPI.QuerySharedUsersRequest{
		UserID:    output.UserID,
		LocalOnly: true,
	}, &queryRes)
	if err != nil {
		logrus.WithError(err).Error("syncapi: failed to QuerySharedUsers for key change event from key server")
		sentry.CaptureException(err)
		return true
	}
	// make sure we get our own key updates too!
	queryRes.UserIDsToCount[output.UserID] = 1
	posUpdate := types.StreamPosition(deviceChangeID)

	s.stream.Advance(posUpdate)
	for userID := range queryRes.UserIDsToCount {
		s.notifier.OnNewKeyChange(types.StreamingToken{DeviceListPosition: posUpdate}, userID, output.UserID)
	}

	return true
}
