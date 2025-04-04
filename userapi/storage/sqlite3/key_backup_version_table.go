// Copyright 2024 New Vector Ltd.
// Copyright 2021 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/userapi/storage/tables"
)

const keyBackupVersionTableSchema = `
-- the metadata for each generation of encrypted e2e session backups
CREATE TABLE IF NOT EXISTS userapi_key_backup_versions (
    user_id TEXT NOT NULL,
	-- this means no 2 users will ever have the same version of e2e session backups which strictly
	-- isn't necessary, but this is easy to do rather than SELECT MAX(version)+1.
    version INTEGER PRIMARY KEY AUTOINCREMENT,
    algorithm TEXT NOT NULL,
    auth_data TEXT NOT NULL,
	etag TEXT NOT NULL,
    deleted INTEGER DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_key_backup_versions_idx ON userapi_key_backup_versions(user_id, version);
`

const insertKeyBackupSQL = "" +
	"INSERT INTO userapi_key_backup_versions(user_id, algorithm, auth_data, etag) VALUES ($1, $2, $3, $4) RETURNING version"

const updateKeyBackupAuthDataSQL = "" +
	"UPDATE userapi_key_backup_versions SET auth_data = $1 WHERE user_id = $2 AND version = $3"

const updateKeyBackupETagSQL = "" +
	"UPDATE userapi_key_backup_versions SET etag = $1 WHERE user_id = $2 AND version = $3"

const deleteKeyBackupSQL = "" +
	"UPDATE userapi_key_backup_versions SET deleted=1 WHERE user_id = $1 AND version = $2"

const selectKeyBackupSQL = "" +
	"SELECT algorithm, auth_data, etag, deleted FROM userapi_key_backup_versions WHERE user_id = $1 AND version = $2"

const selectLatestVersionSQL = "" +
	"SELECT MAX(version) FROM userapi_key_backup_versions WHERE user_id = $1"

type keyBackupVersionStatements struct {
	insertKeyBackupStmt         *sql.Stmt
	updateKeyBackupAuthDataStmt *sql.Stmt
	deleteKeyBackupStmt         *sql.Stmt
	selectKeyBackupStmt         *sql.Stmt
	selectLatestVersionStmt     *sql.Stmt
	updateKeyBackupETagStmt     *sql.Stmt
}

func NewSQLiteKeyBackupVersionTable(db *sql.DB) (tables.KeyBackupVersionTable, error) {
	s := &keyBackupVersionStatements{}
	_, err := db.Exec(keyBackupVersionTableSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertKeyBackupStmt, insertKeyBackupSQL},
		{&s.updateKeyBackupAuthDataStmt, updateKeyBackupAuthDataSQL},
		{&s.deleteKeyBackupStmt, deleteKeyBackupSQL},
		{&s.selectKeyBackupStmt, selectKeyBackupSQL},
		{&s.selectLatestVersionStmt, selectLatestVersionSQL},
		{&s.updateKeyBackupETagStmt, updateKeyBackupETagSQL},
	}.Prepare(db)
}

func (s *keyBackupVersionStatements) InsertKeyBackup(
	ctx context.Context, txn *sql.Tx, userID, algorithm string, authData json.RawMessage, etag string,
) (version string, err error) {
	var versionInt int64
	err = txn.Stmt(s.insertKeyBackupStmt).QueryRowContext(ctx, userID, algorithm, string(authData), etag).Scan(&versionInt)
	return strconv.FormatInt(versionInt, 10), err
}

func (s *keyBackupVersionStatements) UpdateKeyBackupAuthData(
	ctx context.Context, txn *sql.Tx, userID, version string, authData json.RawMessage,
) error {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}
	_, err = txn.Stmt(s.updateKeyBackupAuthDataStmt).ExecContext(ctx, string(authData), userID, versionInt)
	return err
}

func (s *keyBackupVersionStatements) UpdateKeyBackupETag(
	ctx context.Context, txn *sql.Tx, userID, version, etag string,
) error {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid version")
	}
	_, err = txn.Stmt(s.updateKeyBackupETagStmt).ExecContext(ctx, etag, userID, versionInt)
	return err
}

func (s *keyBackupVersionStatements) DeleteKeyBackup(
	ctx context.Context, txn *sql.Tx, userID, version string,
) (bool, error) {
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid version")
	}
	result, err := txn.Stmt(s.deleteKeyBackupStmt).ExecContext(ctx, userID, versionInt)
	if err != nil {
		return false, err
	}
	ra, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return ra == 1, nil
}

func (s *keyBackupVersionStatements) SelectKeyBackup(
	ctx context.Context, txn *sql.Tx, userID, version string,
) (versionResult, algorithm string, authData json.RawMessage, etag string, deleted bool, err error) {
	var versionInt int64
	if version == "" {
		var v *int64 // allows nulls
		if err = txn.Stmt(s.selectLatestVersionStmt).QueryRowContext(ctx, userID).Scan(&v); err != nil {
			return
		}
		if v == nil {
			err = sql.ErrNoRows
			return
		}
		versionInt = *v
	} else {
		if versionInt, err = strconv.ParseInt(version, 10, 64); err != nil {
			return
		}
	}
	versionResult = strconv.FormatInt(versionInt, 10)
	var deletedInt int
	var authDataStr string
	err = txn.Stmt(s.selectKeyBackupStmt).QueryRowContext(ctx, userID, versionInt).Scan(&algorithm, &authDataStr, &etag, &deletedInt)
	deleted = deletedInt == 1
	authData = json.RawMessage(authDataStr)
	return
}
