// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/userapi/storage/tables"
	"github.com/matrix-org/gomatrixserverlib"
)

var staleDeviceListsSchema = `
-- Stores whether a user's device lists are stale or not.
CREATE TABLE IF NOT EXISTS keyserver_stale_device_lists (
    user_id TEXT PRIMARY KEY NOT NULL,
	domain TEXT NOT NULL,
	is_stale BOOLEAN NOT NULL,
	ts_added_secs BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS keyserver_stale_device_lists_idx ON keyserver_stale_device_lists (domain, is_stale);
`

const upsertStaleDeviceListSQL = "" +
	"INSERT INTO keyserver_stale_device_lists (user_id, domain, is_stale, ts_added_secs)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (user_id)" +
	" DO UPDATE SET is_stale = $3, ts_added_secs = $4"

const selectStaleDeviceListsWithDomainsSQL = "" +
	"SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 AND domain = $2 ORDER BY ts_added_secs DESC"

const selectStaleDeviceListsSQL = "" +
	"SELECT user_id FROM keyserver_stale_device_lists WHERE is_stale = $1 ORDER BY ts_added_secs DESC"

const deleteStaleDevicesSQL = "" +
	"DELETE FROM keyserver_stale_device_lists WHERE user_id IN ($1)"

type staleDeviceListsStatements struct {
	db                                    *sql.DB
	upsertStaleDeviceListStmt             *sql.Stmt
	selectStaleDeviceListsWithDomainsStmt *sql.Stmt
	selectStaleDeviceListsStmt            *sql.Stmt
	// deleteStaleDeviceListsStmt            *sql.Stmt // Prepared at runtime
}

func NewSqliteStaleDeviceListsTable(db *sql.DB) (tables.StaleDeviceLists, error) {
	s := &staleDeviceListsStatements{
		db: db,
	}
	_, err := db.Exec(staleDeviceListsSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.upsertStaleDeviceListStmt, upsertStaleDeviceListSQL},
		{&s.selectStaleDeviceListsStmt, selectStaleDeviceListsSQL},
		{&s.selectStaleDeviceListsWithDomainsStmt, selectStaleDeviceListsWithDomainsSQL},
		// { &s.deleteStaleDeviceListsStmt, deleteStaleDevicesSQL}, // Prepared at runtime
	}.Prepare(db)
}

func (s *staleDeviceListsStatements) InsertStaleDeviceList(ctx context.Context, userID string, isStale bool) error {
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}
	_, err = s.upsertStaleDeviceListStmt.ExecContext(ctx, userID, string(domain), isStale, spec.AsTimestamp(time.Now()))
	return err
}

func (s *staleDeviceListsStatements) SelectUserIDsWithStaleDeviceLists(ctx context.Context, domains []spec.ServerName) ([]string, error) {
	// we only query for 1 domain or all domains so optimise for those use cases
	if len(domains) == 0 {
		rows, err := s.selectStaleDeviceListsStmt.QueryContext(ctx, true)
		if err != nil {
			return nil, err
		}
		return rowsToUserIDs(ctx, rows)
	}
	var result []string
	for _, domain := range domains {
		rows, err := s.selectStaleDeviceListsWithDomainsStmt.QueryContext(ctx, true, string(domain))
		if err != nil {
			return nil, err
		}
		userIDs, err := rowsToUserIDs(ctx, rows)
		if err != nil {
			return nil, err
		}
		result = append(result, userIDs...)
	}
	return result, nil
}

// DeleteStaleDeviceLists removes users from stale device lists
func (s *staleDeviceListsStatements) DeleteStaleDeviceLists(
	ctx context.Context, txn *sql.Tx, userIDs []string,
) error {
	qry := strings.Replace(deleteStaleDevicesSQL, "($1)", sqlutil.QueryVariadic(len(userIDs)), 1)
	stmt, err := s.db.Prepare(qry)
	if err != nil {
		return err
	}
	defer external.CloseAndLogIfError(ctx, stmt, "DeleteStaleDeviceLists: stmt.Close failed")
	stmt = sqlutil.TxStmt(txn, stmt)

	params := make([]any, len(userIDs))
	for i := range userIDs {
		params[i] = userIDs[i]
	}

	_, err = stmt.ExecContext(ctx, params...)
	return err
}

func rowsToUserIDs(ctx context.Context, rows *sql.Rows) (result []string, err error) {
	defer external.CloseAndLogIfError(ctx, rows, "closing rowsToUserIDs failed")
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		result = append(result, userID)
	}
	return result, rows.Err()
}
