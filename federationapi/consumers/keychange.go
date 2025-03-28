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
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	"github.com/ike20013/dendrite/federationapi/queue"
	"github.com/ike20013/dendrite/federationapi/storage"
	"github.com/ike20013/dendrite/federationapi/types"
	roomserverAPI "github.com/ike20013/dendrite/roomserver/api"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/setup/jetstream"
	"github.com/ike20013/dendrite/setup/process"
	"github.com/ike20013/dendrite/userapi/api"
)

// KeyChangeConsumer consumes events that originate in key server.
type KeyChangeConsumer struct {
	ctx               context.Context
	jetstream         nats.JetStreamContext
	durable           string
	db                storage.Database
	queues            *queue.OutgoingQueues
	isLocalServerName func(spec.ServerName) bool
	rsAPI             roomserverAPI.FederationRoomserverAPI
	topic             string
}

// NewKeyChangeConsumer creates a new KeyChangeConsumer. Call Start() to begin consuming from key servers.
func NewKeyChangeConsumer(
	process *process.ProcessContext,
	cfg *config.KeyServer,
	js nats.JetStreamContext,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI roomserverAPI.FederationRoomserverAPI,
) *KeyChangeConsumer {
	return &KeyChangeConsumer{
		ctx:               process.Context(),
		jetstream:         js,
		durable:           cfg.Matrix.JetStream.Prefixed("FederationAPIKeyChangeConsumer"),
		topic:             cfg.Matrix.JetStream.Prefixed(jetstream.OutputKeyChangeEvent),
		queues:            queues,
		db:                store,
		isLocalServerName: cfg.Matrix.IsLocalServerName,
		rsAPI:             rsAPI,
	}
}

// Start consuming from key servers
func (t *KeyChangeConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		t.ctx, t.jetstream, t.topic, t.durable, 1,
		t.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called in response to a message received on the
// key change events topic from the key server.
func (t *KeyChangeConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var m api.DeviceMessage
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		sentry.CaptureException(err)
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
		return t.onCrossSigningMessage(m)
	case api.TypeDeviceKeyUpdate:
		fallthrough
	default:
		return t.onDeviceKeyMessage(m)
	}
}

func (t *KeyChangeConsumer) onDeviceKeyMessage(m api.DeviceMessage) bool {
	if m.DeviceKeys == nil {
		return true
	}
	logger := logrus.WithField("user_id", m.UserID)

	// only send key change events which originated from us
	_, originServerName, err := gomatrixserverlib.SplitID('@', m.UserID)
	if err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("Failed to extract domain from key change event")
		return true
	}
	if !t.isLocalServerName(originServerName) {
		return true
	}

	userID, err := spec.NewUserID(m.UserID, true)
	if err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("invalid user ID")
		return true
	}

	roomIDs, err := t.rsAPI.QueryRoomsForUser(t.ctx, *userID, "join")
	if err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("failed to calculate joined rooms for user")
		return true
	}

	roomIDStrs := make([]string, len(roomIDs))
	for i, room := range roomIDs {
		roomIDStrs[i] = room.String()
	}

	// send this key change to all servers who share rooms with this user.
	destinations, err := t.db.GetJoinedHostsForRooms(t.ctx, roomIDStrs, true, true)
	if err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("failed to calculate joined hosts for rooms user is in")
		return true
	}

	if len(destinations) == 0 {
		return true
	}
	// Pack the EDU and marshal it
	edu := &gomatrixserverlib.EDU{
		Type:   spec.MDeviceListUpdate,
		Origin: string(originServerName),
	}
	event := gomatrixserverlib.DeviceListUpdateEvent{
		UserID:            m.UserID,
		DeviceID:          m.DeviceID,
		DeviceDisplayName: m.DisplayName,
		StreamID:          m.StreamID,
		PrevID:            prevID(m.StreamID),
		Deleted:           len(m.KeyJSON) == 0,
		Keys:              m.KeyJSON,
	}
	if edu.Content, err = json.Marshal(event); err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("failed to marshal EDU JSON")
		return true
	}

	logger.Debugf("Sending device list update message to %q", destinations)
	err = t.queues.SendEDU(edu, originServerName, destinations)
	return err == nil
}

func (t *KeyChangeConsumer) onCrossSigningMessage(m api.DeviceMessage) bool {
	output := m.CrossSigningKeyUpdate
	_, host, err := gomatrixserverlib.SplitID('@', output.UserID)
	if err != nil {
		sentry.CaptureException(err)
		logrus.WithError(err).Errorf("fedsender key change consumer: user ID parse failure")
		return true
	}
	if !t.isLocalServerName(host) {
		// Ignore any messages that didn't originate locally, otherwise we'll
		// end up parroting information we received from other servers.
		return true
	}
	logger := logrus.WithField("user_id", output.UserID)

	outputUserID, err := spec.NewUserID(output.UserID, true)
	if err != nil {
		sentry.CaptureException(err)
		logrus.WithError(err).Errorf("invalid user ID")
		return true
	}

	rooms, err := t.rsAPI.QueryRoomsForUser(t.ctx, *outputUserID, "join")
	if err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("fedsender key change consumer: failed to calculate joined rooms for user")
		return true
	}

	roomIDStrs := make([]string, len(rooms))
	for i, room := range rooms {
		roomIDStrs[i] = room.String()
	}

	// send this key change to all servers who share rooms with this user.
	destinations, err := t.db.GetJoinedHostsForRooms(t.ctx, roomIDStrs, true, true)
	if err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("fedsender key change consumer: failed to calculate joined hosts for rooms user is in")
		return true
	}

	if len(destinations) == 0 {
		return true
	}

	// Pack the EDU and marshal it
	edu := &gomatrixserverlib.EDU{
		Type:   types.MSigningKeyUpdate,
		Origin: string(host),
	}
	if edu.Content, err = json.Marshal(output); err != nil {
		sentry.CaptureException(err)
		logger.WithError(err).Error("fedsender key change consumer: failed to marshal output, dropping")
		return true
	}

	logger.Debugf("Sending cross-signing update message to %q", destinations)
	err = t.queues.SendEDU(edu, host, destinations)
	return err == nil
}

func prevID(streamID int64) []int64 {
	if streamID <= 1 {
		return nil
	}
	return []int64{streamID - 1}
}
