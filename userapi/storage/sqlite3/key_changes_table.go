// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/userapi/storage/sqlite3/deltas"
	"github.com/ike20013/dendrite/userapi/storage/tables"
)

var keyChangesSchema = `
-- Stores key change information about users. Used to determine when to send updated device lists to clients.
CREATE TABLE IF NOT EXISTS keyserver_key_changes (
	change_id INTEGER PRIMARY KEY AUTOINCREMENT,
	-- The key owner
	user_id TEXT NOT NULL,
	UNIQUE (user_id)
);
`

// Replace based on user ID. We don't care how many times the user's keys have changed, only that they
// have changed, hence we can just keep bumping the change ID for this user.
const upsertKeyChangeSQL = "" +
	"INSERT OR REPLACE INTO keyserver_key_changes (user_id)" +
	" VALUES ($1)" +
	" RETURNING change_id"

const selectKeyChangesSQL = "" +
	"SELECT user_id, change_id FROM keyserver_key_changes WHERE change_id > $1 AND change_id <= $2"

type keyChangesStatements struct {
	db                   *sql.DB
	upsertKeyChangeStmt  *sql.Stmt
	selectKeyChangesStmt *sql.Stmt
}

func NewSqliteKeyChangesTable(db *sql.DB) (tables.KeyChanges, error) {
	s := &keyChangesStatements{
		db: db,
	}
	_, err := db.Exec(keyChangesSchema)
	if err != nil {
		return s, err
	}

	if err = executeMigration(context.Background(), db); err != nil {
		return nil, err
	}

	return s, sqlutil.StatementList{
		{&s.upsertKeyChangeStmt, upsertKeyChangeSQL},
		{&s.selectKeyChangesStmt, selectKeyChangesSQL},
	}.Prepare(db)
}

func executeMigration(ctx context.Context, db *sql.DB) error {
	// TODO: Remove when we are sure we are not having goose artefacts in the db
	// This forces an error, which indicates the migration is already applied, since the
	// column partition was removed from the table
	migrationName := "keyserver: refactor key changes"

	var cName string
	err := db.QueryRowContext(ctx, `SELECT p.name FROM sqlite_master AS m JOIN pragma_table_info(m.name) AS p WHERE m.name = 'keyserver_key_changes' AND p.name = 'partition'`).Scan(&cName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) { // migration was already executed, as the column was removed
			if err = sqlutil.InsertMigration(ctx, db, migrationName); err != nil {
				return fmt.Errorf("unable to manually insert migration '%s': %w", migrationName, err)
			}
			return nil
		}
		return err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: migrationName,
		Up:      deltas.UpRefactorKeyChanges,
	})
	return m.Up(ctx)
}

func (s *keyChangesStatements) InsertKeyChange(ctx context.Context, userID string) (changeID int64, err error) {
	err = s.upsertKeyChangeStmt.QueryRowContext(ctx, userID).Scan(&changeID)
	return
}

func (s *keyChangesStatements) SelectKeyChanges(
	ctx context.Context, fromOffset, toOffset int64,
) (userIDs []string, latestOffset int64, err error) {
	latestOffset = fromOffset
	rows, err := s.selectKeyChangesStmt.QueryContext(ctx, fromOffset, toOffset)
	if err != nil {
		return nil, 0, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "selectKeyChangesStmt: rows.close() failed")
	for rows.Next() {
		var userID string
		var offset int64
		if err = rows.Scan(&userID, &offset); err != nil {
			return nil, 0, err
		}
		if offset > latestOffset {
			latestOffset = offset
		}
		userIDs = append(userIDs, userID)
	}
	err = rows.Err()
	return
}
