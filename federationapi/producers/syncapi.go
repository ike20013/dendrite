// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package producers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/setup/jetstream"
	"github.com/ike20013/dendrite/syncapi/types"
	userapi "github.com/ike20013/dendrite/userapi/api"
)

// SyncAPIProducer produces events for the sync API server to consume
type SyncAPIProducer struct {
	TopicReceiptEvent      string
	TopicSendToDeviceEvent string
	TopicTypingEvent       string
	TopicPresenceEvent     string
	TopicDeviceListUpdate  string
	TopicSigningKeyUpdate  string
	JetStream              nats.JetStreamContext
	Config                 *config.FederationAPI
	UserAPI                userapi.FederationUserAPI
}

func (p *SyncAPIProducer) SendReceipt(
	ctx context.Context,
	userID, roomID, eventID, receiptType string, timestamp spec.Timestamp,
) error {
	m := &nats.Msg{
		Subject: p.TopicReceiptEvent,
		Header:  nats.Header{},
	}
	m.Header.Set(jetstream.UserID, userID)
	m.Header.Set(jetstream.RoomID, roomID)
	m.Header.Set(jetstream.EventID, eventID)
	m.Header.Set("type", receiptType)
	m.Header.Set("timestamp", fmt.Sprintf("%d", timestamp))

	log.WithFields(log.Fields{}).Tracef("Producing to topic '%s'", p.TopicReceiptEvent)
	_, err := p.JetStream.PublishMsg(m, nats.Context(ctx))
	return err
}

func (p *SyncAPIProducer) SendToDevice(
	ctx context.Context, sender, userID, deviceID, eventType string,
	message json.RawMessage,
) error {
	devices := []string{}
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}

	// If the event is targeted locally then we want to expand the wildcard
	// out into individual device IDs so that we can send them to each respective
	// device. If the event isn't targeted locally then we can't expand the
	// wildcard as we don't know about the remote devices, so instead we leave it
	// as-is, so that the federation sender can send it on with the wildcard intact.
	if p.Config.Matrix.IsLocalServerName(domain) && deviceID == "*" {
		var res userapi.QueryDevicesResponse
		err = p.UserAPI.QueryDevices(context.TODO(), &userapi.QueryDevicesRequest{
			UserID: userID,
		}, &res)
		if err != nil {
			return err
		}
		for _, dev := range res.Devices {
			devices = append(devices, dev.ID)
		}
	} else {
		devices = append(devices, deviceID)
	}

	log.WithFields(log.Fields{
		"user_id":     userID,
		"num_devices": len(devices),
		"type":        eventType,
	}).Tracef("Producing to topic '%s'", p.TopicSendToDeviceEvent)
	for i, device := range devices {
		ote := &types.OutputSendToDeviceEvent{
			UserID:   userID,
			DeviceID: device,
			SendToDeviceEvent: gomatrixserverlib.SendToDeviceEvent{
				Sender:  sender,
				Type:    eventType,
				Content: message,
			},
		}

		eventJSON, err := json.Marshal(ote)
		if err != nil {
			log.WithError(err).Error("sendToDevice failed json.Marshal")
			return err
		}
		m := nats.NewMsg(p.TopicSendToDeviceEvent)
		m.Data = eventJSON
		m.Header.Set("sender", sender)
		m.Header.Set(jetstream.UserID, userID)

		if _, err = p.JetStream.PublishMsg(m, nats.Context(ctx)); err != nil {
			if i < len(devices)-1 {
				log.WithError(err).Warn("sendToDevice failed to PublishMsg, trying further devices")
				continue
			}
			log.WithError(err).Error("sendToDevice failed to PublishMsg for all devices")
			return err
		}
	}
	return nil
}

func (p *SyncAPIProducer) SendTyping(
	ctx context.Context, userID, roomID string, typing bool, timeoutMS int64,
) error {
	m := &nats.Msg{
		Subject: p.TopicTypingEvent,
		Header:  nats.Header{},
	}
	m.Header.Set(jetstream.UserID, userID)
	m.Header.Set(jetstream.RoomID, roomID)
	m.Header.Set("typing", strconv.FormatBool(typing))
	m.Header.Set("timeout_ms", strconv.Itoa(int(timeoutMS)))
	_, err := p.JetStream.PublishMsg(m, nats.Context(ctx))
	return err
}

func (p *SyncAPIProducer) SendPresence(
	ctx context.Context, userID string, presence types.Presence, statusMsg *string, lastActiveAgo int64,
) error {
	m := nats.NewMsg(p.TopicPresenceEvent)
	m.Header.Set(jetstream.UserID, userID)
	m.Header.Set("presence", presence.String())
	if statusMsg != nil {
		m.Header.Set("status_msg", *statusMsg)
	}
	lastActiveTS := spec.AsTimestamp(time.Now().Add(-(time.Duration(lastActiveAgo) * time.Millisecond)))

	m.Header.Set("last_active_ts", strconv.Itoa(int(lastActiveTS)))
	log.Tracef("Sending presence to syncAPI: %+v", m.Header)
	_, err := p.JetStream.PublishMsg(m, nats.Context(ctx))
	return err
}

func (p *SyncAPIProducer) SendDeviceListUpdate(
	ctx context.Context, deviceListUpdate spec.RawJSON, origin spec.ServerName,
) (err error) {
	m := nats.NewMsg(p.TopicDeviceListUpdate)
	m.Header.Set("origin", string(origin))
	m.Data = deviceListUpdate
	log.Debugf("Sending device list update: %+v", m.Header)
	_, err = p.JetStream.PublishMsg(m, nats.Context(ctx))
	return err
}

func (p *SyncAPIProducer) SendSigningKeyUpdate(
	ctx context.Context, data spec.RawJSON, origin spec.ServerName,
) (err error) {
	m := nats.NewMsg(p.TopicSigningKeyUpdate)
	m.Header.Set("origin", string(origin))
	m.Data = data

	log.Debugf("Sending signing key update")
	_, err = p.JetStream.PublishMsg(m, nats.Context(ctx))
	return err
}
