// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2017, 2018 New Vector Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"strings"

	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/roomserver/storage/tables"
	"github.com/ike20013/dendrite/roomserver/types"
)

const eventJSONSchema = `
  CREATE TABLE IF NOT EXISTS roomserver_event_json (
    event_nid INTEGER NOT NULL PRIMARY KEY,
    event_json TEXT NOT NULL
  );
`

const insertEventJSONSQL = `
	INSERT OR REPLACE INTO roomserver_event_json (event_nid, event_json) VALUES ($1, $2)
`

// Bulk event JSON lookup by numeric event ID.
// Sort by the numeric event ID.
// This means that we can use binary search to lookup by numeric event ID.
const bulkSelectEventJSONSQL = `
	SELECT event_nid, event_json FROM roomserver_event_json
	  WHERE event_nid IN ($1)
	  ORDER BY event_nid ASC
`

type eventJSONStatements struct {
	db                      *sql.DB
	insertEventJSONStmt     *sql.Stmt
	bulkSelectEventJSONStmt *sql.Stmt
}

func CreateEventJSONTable(db *sql.DB) error {
	_, err := db.Exec(eventJSONSchema)
	return err
}

func PrepareEventJSONTable(db *sql.DB) (tables.EventJSON, error) {
	s := &eventJSONStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.insertEventJSONStmt, insertEventJSONSQL},
		{&s.bulkSelectEventJSONStmt, bulkSelectEventJSONSQL},
	}.Prepare(db)
}

func (s *eventJSONStatements) InsertEventJSON(
	ctx context.Context, txn *sql.Tx, eventNID types.EventNID, eventJSON []byte,
) error {
	_, err := sqlutil.TxStmt(txn, s.insertEventJSONStmt).ExecContext(ctx, int64(eventNID), eventJSON)
	return err
}

func (s *eventJSONStatements) BulkSelectEventJSON(
	ctx context.Context, txn *sql.Tx, eventNIDs []types.EventNID,
) ([]tables.EventJSONPair, error) {
	iEventNIDs := make([]interface{}, len(eventNIDs))
	for k, v := range eventNIDs {
		iEventNIDs[k] = v
	}
	selectOrig := strings.Replace(bulkSelectEventJSONSQL, "($1)", sqlutil.QueryVariadic(len(iEventNIDs)), 1)
	var rows *sql.Rows
	var err error
	if txn != nil {
		rows, err = txn.QueryContext(ctx, selectOrig, iEventNIDs...)
	} else {
		rows, err = s.db.QueryContext(ctx, selectOrig, iEventNIDs...)
	}
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "bulkSelectEventJSON: rows.close() failed")

	// We know that we will only get as many results as event NIDs
	// because of the unique constraint on event NIDs.
	// So we can allocate an array of the correct size now.
	// We might get fewer results than NIDs so we adjust the length of the slice before returning it.
	results := make([]tables.EventJSONPair, len(eventNIDs))
	i := 0
	var eventNID int64
	for ; rows.Next(); i++ {
		result := &results[i]
		if err := rows.Scan(&eventNID, &result.EventJSON); err != nil {
			return nil, err
		}
		result.EventNID = types.EventNID(eventNID)
	}
	return results[:i], rows.Err()
}
