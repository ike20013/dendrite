// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package util

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"runtime"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/userapi/storage"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type phoneHomeStats struct {
	prevData   timestampToRUUsage
	stats      map[string]interface{}
	serverName spec.ServerName
	startTime  time.Time
	cfg        *config.Dendrite
	db         storage.Statistics
	isMonolith bool
	client     *http.Client
}

type timestampToRUUsage struct {
	timestamp int64
	usage     syscall.Rusage
}

func StartPhoneHomeCollector(startTime time.Time, cfg *config.Dendrite, statsDB storage.Statistics) {

	p := phoneHomeStats{
		startTime:  startTime,
		serverName: cfg.Global.ServerName,
		cfg:        cfg,
		db:         statsDB,
		isMonolith: true,
		client: &http.Client{
			Timeout:   time.Second * 30,
			Transport: http.DefaultTransport,
		},
	}

	// start initial run after 5min
	time.AfterFunc(time.Minute*5, p.collect)

	// run every 3 hours
	ticker := time.NewTicker(time.Hour * 3)
	for range ticker.C {
		p.collect()
	}
}

func (p *phoneHomeStats) collect() {
	p.stats = make(map[string]interface{})
	// general information
	p.stats["homeserver"] = p.serverName
	p.stats["monolith"] = p.isMonolith
	p.stats["version"] = external.VersionString()
	p.stats["timestamp"] = time.Now().Unix()
	p.stats["go_version"] = runtime.Version()
	p.stats["go_arch"] = runtime.GOARCH
	p.stats["go_os"] = runtime.GOOS
	p.stats["num_cpu"] = runtime.NumCPU()
	p.stats["num_go_routine"] = runtime.NumGoroutine()
	p.stats["uptime_seconds"] = math.Floor(time.Since(p.startTime).Seconds())

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*1)
	defer cancel()

	// cpu and memory usage information
	err := getMemoryStats(p)
	if err != nil {
		logrus.WithError(err).Warn("unable to get memory/cpu stats, using defaults")
	}

	// configuration information
	p.stats["federation_disabled"] = p.cfg.Global.DisableFederation
	natsEmbedded := len(p.cfg.Global.JetStream.Addresses) == 0
	p.stats["nats_embedded"] = natsEmbedded
	p.stats["nats_in_memory"] = p.cfg.Global.JetStream.InMemory && natsEmbedded

	if len(p.cfg.Logging) > 0 {
		p.stats["log_level"] = p.cfg.Logging[0].Level
	} else {
		p.stats["log_level"] = "info"
	}

	// message and room stats
	// TODO: Find a solution to actually set this value
	p.stats["total_room_count"] = 0

	messageStats, activeRooms, activeE2EERooms, err := p.db.DailyRoomsMessages(ctx, p.serverName)
	if err != nil {
		logrus.WithError(err).Warn("unable to query message stats, using default values")
	}
	p.stats["daily_messages"] = messageStats.Messages
	p.stats["daily_sent_messages"] = messageStats.SentMessages
	p.stats["daily_e2ee_messages"] = messageStats.MessagesE2EE
	p.stats["daily_sent_e2ee_messages"] = messageStats.SentMessagesE2EE
	p.stats["daily_active_rooms"] = activeRooms
	p.stats["daily_active_e2ee_rooms"] = activeE2EERooms

	// user stats and DB engine
	userStats, db, err := p.db.UserStatistics(ctx)
	if err != nil {
		logrus.WithError(err).Warn("unable to query userstats, using default values")
	}
	p.stats["database_engine"] = db.Engine
	p.stats["database_server_version"] = db.Version
	p.stats["total_users"] = userStats.AllUsers
	p.stats["total_nonbridged_users"] = userStats.NonBridgedUsers
	p.stats["daily_active_users"] = userStats.DailyUsers
	p.stats["monthly_active_users"] = userStats.MonthlyUsers
	for t, c := range userStats.RegisteredUsersByType {
		p.stats["daily_user_type_"+t] = c
	}
	for t, c := range userStats.R30Users {
		p.stats["r30_users_"+t] = c
	}
	for t, c := range userStats.R30UsersV2 {
		p.stats["r30v2_users_"+t] = c
	}

	output := bytes.Buffer{}
	if err = json.NewEncoder(&output).Encode(p.stats); err != nil {
		logrus.WithError(err).Error("Unable to encode phone-home statistics")
		return
	}

	logrus.Infof("Reporting stats to %s: %s", p.cfg.Global.ReportStats.Endpoint, output.String())

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.Global.ReportStats.Endpoint, &output)
	if err != nil {
		logrus.WithError(err).Error("Unable to create phone-home statistics request")
		return
	}
	request.Header.Set("User-Agent", "Dendrite/"+external.VersionString())

	_, err = p.client.Do(request)
	if err != nil {
		logrus.WithError(err).Error("Unable to send phone-home statistics")
		return
	}
}
