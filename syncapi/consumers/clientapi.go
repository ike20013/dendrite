// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/ike20013/dendrite/external/eventutil"
	"github.com/ike20013/dendrite/external/fulltext"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/setup/jetstream"
	"github.com/ike20013/dendrite/setup/process"
	"github.com/ike20013/dendrite/syncapi/notifier"
	"github.com/ike20013/dendrite/syncapi/storage"
	"github.com/ike20013/dendrite/syncapi/streams"
	"github.com/ike20013/dendrite/syncapi/types"
)

// OutputClientDataConsumer consumes events that originated in the client API server.
type OutputClientDataConsumer struct {
	ctx          context.Context
	jetstream    nats.JetStreamContext
	nats         *nats.Conn
	durable      string
	topic        string
	topicReIndex string
	db           storage.Database
	stream       streams.StreamProvider
	notifier     *notifier.Notifier
	serverName   spec.ServerName
	fts          fulltext.Indexer
	cfg          *config.SyncAPI
}

// NewOutputClientDataConsumer creates a new OutputClientData consumer. Call Start() to begin consuming from room servers.
func NewOutputClientDataConsumer(
	process *process.ProcessContext,
	cfg *config.SyncAPI,
	js nats.JetStreamContext,
	nats *nats.Conn,
	store storage.Database,
	notifier *notifier.Notifier,
	stream streams.StreamProvider,
	fts *fulltext.Search,
) *OutputClientDataConsumer {
	return &OutputClientDataConsumer{
		ctx:          process.Context(),
		jetstream:    js,
		topic:        cfg.Matrix.JetStream.Prefixed(jetstream.OutputClientData),
		topicReIndex: cfg.Matrix.JetStream.Prefixed(jetstream.InputFulltextReindex),
		durable:      cfg.Matrix.JetStream.Durable("SyncAPIAccountDataConsumer"),
		nats:         nats,
		db:           store,
		notifier:     notifier,
		stream:       stream,
		serverName:   cfg.Matrix.ServerName,
		fts:          fts,
		cfg:          cfg,
	}
}

// Start consuming from room servers
func (s *OutputClientDataConsumer) Start() error {
	_, err := s.nats.Subscribe(s.topicReIndex, func(msg *nats.Msg) {
		if err := msg.Ack(); err != nil {
			return
		}
		if !s.cfg.Fulltext.Enabled {
			logrus.Warn("Fulltext indexing is disabled")
			return
		}
		ctx := context.Background()
		logrus.Infof("Starting to index events")
		var offset int
		start := time.Now()
		count := 0
		var id int64 = 0
		for {
			evs, err := s.db.ReIndex(ctx, 1000, id)
			if err != nil {
				logrus.WithError(err).Errorf("unable to get events to index")
				return
			}
			if len(evs) == 0 {
				break
			}
			logrus.Debugf("Indexing %d events", len(evs))
			elements := make([]fulltext.IndexElement, 0, len(evs))

			for streamPos, ev := range evs {
				id = streamPos
				e := fulltext.IndexElement{
					EventID:        ev.EventID(),
					RoomID:         ev.RoomID().String(),
					StreamPosition: streamPos,
				}
				e.SetContentType(ev.Type())

				switch ev.Type() {
				case "m.room.message":
					e.Content = gjson.GetBytes(ev.Content(), "body").String()
				case spec.MRoomName:
					e.Content = gjson.GetBytes(ev.Content(), "name").String()
				case spec.MRoomTopic:
					e.Content = gjson.GetBytes(ev.Content(), "topic").String()
				default:
					continue
				}

				if strings.TrimSpace(e.Content) == "" {
					continue
				}
				elements = append(elements, e)
			}
			if err = s.fts.Index(elements...); err != nil {
				logrus.WithError(err).Error("unable to index events")
				continue
			}
			offset += len(evs)
			count += len(elements)
		}
		logrus.Infof("Indexed %d events in %v", count, time.Since(start))
	})
	if err != nil {
		return err
	}
	return jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

// onMessage is called when the sync server receives a new event from the client API server output log.
// It is not safe for this function to be called from multiple goroutines, or else the
// sync stream position may race and be incorrectly calculated.
func (s *OutputClientDataConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// Parse out the event JSON
	userID := msg.Header.Get(jetstream.UserID)
	var output eventutil.AccountData
	if err := json.Unmarshal(msg.Data, &output); err != nil {
		// If the message was invalid, log it and move on to the next message in the stream
		log.WithError(err).Errorf("client API server output log: message parse failure")
		sentry.CaptureException(err)
		return true
	}

	log.WithFields(log.Fields{
		"type":    output.Type,
		"room_id": output.RoomID,
	}).Debug("Received data from client API server")

	streamPos, err := s.db.UpsertAccountData(
		s.ctx, userID, output.RoomID, output.Type,
	)
	if err != nil {
		sentry.CaptureException(err)
		log.WithFields(log.Fields{
			"type":       output.Type,
			"room_id":    output.RoomID,
			log.ErrorKey: err,
		}).Errorf("could not save account data")
		return false
	}

	if output.IgnoredUsers != nil {
		if err := s.db.UpdateIgnoresForUser(ctx, userID, output.IgnoredUsers); err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"user_id": userID,
			}).Errorf("Failed to update ignored users")
			sentry.CaptureException(err)
		}
	}

	s.stream.Advance(streamPos)
	s.notifier.OnNewAccountData(userID, types.StreamingToken{AccountDataPosition: streamPos})

	return true
}
