// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ike20013/dendrite/clientapi/auth/authtypes"
	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/userapi/storage/tables"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const profilesSchema = `
-- Stores data about accounts profiles.
CREATE TABLE IF NOT EXISTS userapi_profiles (
    -- The Matrix user ID localpart for this account
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- The display name for this account
    display_name TEXT,
    -- The URL of the avatar for this account
    avatar_url TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_profiles_idx ON userapi_profiles(localpart, server_name);
`

const insertProfileSQL = "" +
	"INSERT INTO userapi_profiles(localpart, server_name, display_name, avatar_url) VALUES ($1, $2, $3, $4)"

const selectProfileByLocalpartSQL = "" +
	"SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart = $1 AND server_name = $2"

const setAvatarURLSQL = "" +
	"UPDATE userapi_profiles SET avatar_url = $1 WHERE localpart = $2 AND server_name = $3" +
	" RETURNING display_name"

const setDisplayNameSQL = "" +
	"UPDATE userapi_profiles SET display_name = $1 WHERE localpart = $2 AND server_name = $3" +
	" RETURNING avatar_url"

const selectProfilesBySearchSQL = "" +
	"SELECT localpart, server_name, display_name, avatar_url FROM userapi_profiles WHERE localpart LIKE $1 OR display_name LIKE $1 LIMIT $2"

type profilesStatements struct {
	db                           *sql.DB
	serverNoticesLocalpart       string
	insertProfileStmt            *sql.Stmt
	selectProfileByLocalpartStmt *sql.Stmt
	setAvatarURLStmt             *sql.Stmt
	setDisplayNameStmt           *sql.Stmt
	selectProfilesBySearchStmt   *sql.Stmt
}

func NewSQLiteProfilesTable(db *sql.DB, serverNoticesLocalpart string) (tables.ProfileTable, error) {
	s := &profilesStatements{
		db:                     db,
		serverNoticesLocalpart: serverNoticesLocalpart,
	}
	_, err := db.Exec(profilesSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertProfileStmt, insertProfileSQL},
		{&s.selectProfileByLocalpartStmt, selectProfileByLocalpartSQL},
		{&s.setAvatarURLStmt, setAvatarURLSQL},
		{&s.setDisplayNameStmt, setDisplayNameSQL},
		{&s.selectProfilesBySearchStmt, selectProfilesBySearchSQL},
	}.Prepare(db)
}

func (s *profilesStatements) InsertProfile(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
) error {
	_, err := sqlutil.TxStmt(txn, s.insertProfileStmt).ExecContext(ctx, localpart, serverName, "", "")
	return err
}

func (s *profilesStatements) SelectProfileByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (*authtypes.Profile, error) {
	var profile authtypes.Profile
	err := s.selectProfileByLocalpartStmt.QueryRowContext(ctx, localpart, serverName).Scan(
		&profile.Localpart, &profile.ServerName, &profile.DisplayName, &profile.AvatarURL,
	)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

func (s *profilesStatements) SetAvatarURL(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	avatarURL string,
) (*authtypes.Profile, bool, error) {
	profile := &authtypes.Profile{
		Localpart:  localpart,
		ServerName: string(serverName),
		AvatarURL:  avatarURL,
	}
	old, err := s.SelectProfileByLocalpart(ctx, localpart, serverName)
	if err != nil {
		return old, false, err
	}
	if old.AvatarURL == avatarURL {
		return old, false, nil
	}
	stmt := sqlutil.TxStmt(txn, s.setAvatarURLStmt)
	err = stmt.QueryRowContext(ctx, avatarURL, localpart, serverName).Scan(&profile.DisplayName)
	return profile, true, err
}

func (s *profilesStatements) SetDisplayName(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	displayName string,
) (*authtypes.Profile, bool, error) {
	profile := &authtypes.Profile{
		Localpart:   localpart,
		ServerName:  string(serverName),
		DisplayName: displayName,
	}
	old, err := s.SelectProfileByLocalpart(ctx, localpart, serverName)
	if err != nil {
		return old, false, err
	}
	if old.DisplayName == displayName {
		return old, false, nil
	}
	stmt := sqlutil.TxStmt(txn, s.setDisplayNameStmt)
	err = stmt.QueryRowContext(ctx, displayName, localpart, serverName).Scan(&profile.AvatarURL)
	return profile, true, err
}

func (s *profilesStatements) SelectProfilesBySearch(
	ctx context.Context, searchString string, limit int,
) ([]authtypes.Profile, error) {
	var profiles []authtypes.Profile
	// The fmt.Sprintf directive below is building a parameter for the
	// "LIKE" condition in the SQL query. %% escapes the % char, so the
	// statement in the end will look like "LIKE %searchString%".
	rows, err := s.selectProfilesBySearchStmt.QueryContext(ctx, fmt.Sprintf("%%%s%%", searchString), limit)
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "selectProfilesBySearch: rows.close() failed")
	for rows.Next() {
		var profile authtypes.Profile
		if err := rows.Scan(&profile.Localpart, &profile.ServerName, &profile.DisplayName, &profile.AvatarURL); err != nil {
			return nil, err
		}
		if profile.Localpart != s.serverNoticesLocalpart {
			profiles = append(profiles, profile)
		}
	}
	return profiles, rows.Err()
}
