// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2017, 2018 New Vector Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"

	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/roomserver/storage/tables"
)

const roomAliasesSchema = `
-- Stores room aliases and room IDs they refer to
CREATE TABLE IF NOT EXISTS roomserver_room_aliases (
    -- Alias of the room
    alias TEXT NOT NULL PRIMARY KEY,
    -- Room ID the alias refers to
    room_id TEXT NOT NULL,
    -- User ID of the creator of this alias
    creator_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS roomserver_room_id_idx ON roomserver_room_aliases(room_id);
`

const insertRoomAliasSQL = "" +
	"INSERT INTO roomserver_room_aliases (alias, room_id, creator_id) VALUES ($1, $2, $3)"

const selectRoomIDFromAliasSQL = "" +
	"SELECT room_id FROM roomserver_room_aliases WHERE alias = $1"

const selectAliasesFromRoomIDSQL = "" +
	"SELECT alias FROM roomserver_room_aliases WHERE room_id = $1"

const selectCreatorIDFromAliasSQL = "" +
	"SELECT creator_id FROM roomserver_room_aliases WHERE alias = $1"

const deleteRoomAliasSQL = "" +
	"DELETE FROM roomserver_room_aliases WHERE alias = $1"

type roomAliasesStatements struct {
	insertRoomAliasStmt          *sql.Stmt
	selectRoomIDFromAliasStmt    *sql.Stmt
	selectAliasesFromRoomIDStmt  *sql.Stmt
	selectCreatorIDFromAliasStmt *sql.Stmt
	deleteRoomAliasStmt          *sql.Stmt
}

func CreateRoomAliasesTable(db *sql.DB) error {
	_, err := db.Exec(roomAliasesSchema)
	return err
}

func PrepareRoomAliasesTable(db *sql.DB) (tables.RoomAliases, error) {
	s := &roomAliasesStatements{}

	return s, sqlutil.StatementList{
		{&s.insertRoomAliasStmt, insertRoomAliasSQL},
		{&s.selectRoomIDFromAliasStmt, selectRoomIDFromAliasSQL},
		{&s.selectAliasesFromRoomIDStmt, selectAliasesFromRoomIDSQL},
		{&s.selectCreatorIDFromAliasStmt, selectCreatorIDFromAliasSQL},
		{&s.deleteRoomAliasStmt, deleteRoomAliasSQL},
	}.Prepare(db)
}

func (s *roomAliasesStatements) InsertRoomAlias(
	ctx context.Context, txn *sql.Tx, alias string, roomID string, creatorUserID string,
) (err error) {
	stmt := sqlutil.TxStmt(txn, s.insertRoomAliasStmt)
	_, err = stmt.ExecContext(ctx, alias, roomID, creatorUserID)
	return
}

func (s *roomAliasesStatements) SelectRoomIDFromAlias(
	ctx context.Context, txn *sql.Tx, alias string,
) (roomID string, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectRoomIDFromAliasStmt)
	err = stmt.QueryRowContext(ctx, alias).Scan(&roomID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return
}

func (s *roomAliasesStatements) SelectAliasesFromRoomID(
	ctx context.Context, txn *sql.Tx, roomID string,
) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectAliasesFromRoomIDStmt)
	rows, err := stmt.QueryContext(ctx, roomID)
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "selectAliasesFromRoomID: rows.close() failed")

	var aliases []string
	var alias string
	for rows.Next() {
		if err = rows.Scan(&alias); err != nil {
			return nil, err
		}

		aliases = append(aliases, alias)
	}
	return aliases, rows.Err()
}

func (s *roomAliasesStatements) SelectCreatorIDFromAlias(
	ctx context.Context, txn *sql.Tx, alias string,
) (creatorID string, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectCreatorIDFromAliasStmt)
	err = stmt.QueryRowContext(ctx, alias).Scan(&creatorID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return
}

func (s *roomAliasesStatements) DeleteRoomAlias(
	ctx context.Context, txn *sql.Tx, alias string,
) (err error) {
	stmt := sqlutil.TxStmt(txn, s.deleteRoomAliasStmt)
	_, err = stmt.ExecContext(ctx, alias)
	return
}
