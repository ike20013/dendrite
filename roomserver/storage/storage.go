// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !wasm
// +build !wasm

package storage

import (
	"context"
	"fmt"

	"github.com/ike20013/dendrite/external/caching"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/roomserver/storage/postgres"
	"github.com/ike20013/dendrite/roomserver/storage/sqlite3"
	"github.com/ike20013/dendrite/setup/config"
)

// Open opens a database connection.
func Open(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.RoomServerCaches) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.Open(ctx, conMan, dbProperties, cache)
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.Open(ctx, conMan, dbProperties, cache)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
