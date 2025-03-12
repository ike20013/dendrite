// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package storage

import (
	"fmt"

	"github.com/ike20013/dendrite/external/caching"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/relayapi/storage/sqlite3"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/matrix-org/gomatrixserverlib"
)

// NewDatabase opens a new database
func NewDatabase(
	conMan sqlutil.Connections,
	dbProperties *config.DatabaseOptions,
	cache caching.FederationCache,
	isLocalServerName func(spec.ServerName) bool,
) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewDatabase(conMan, dbProperties, cache, isLocalServerName)
	case dbProperties.ConnectionString.IsPostgres():
		return nil, fmt.Errorf("can't use Postgres implementation")
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
