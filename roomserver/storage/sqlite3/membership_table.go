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
	"fmt"
	"strings"

	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/roomserver/storage/sqlite3/deltas"
	"github.com/ike20013/dendrite/roomserver/storage/tables"
	"github.com/ike20013/dendrite/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const membershipSchema = `
	CREATE TABLE IF NOT EXISTS roomserver_membership (
		room_nid INTEGER NOT NULL,
		target_nid INTEGER NOT NULL,
		sender_nid INTEGER NOT NULL DEFAULT 0,
		membership_nid INTEGER NOT NULL DEFAULT 1,
		event_nid INTEGER NOT NULL DEFAULT 0,
		target_local BOOLEAN NOT NULL DEFAULT false,
		forgotten BOOLEAN NOT NULL DEFAULT false,
		UNIQUE (room_nid, target_nid)
	);
`

var selectJoinedUsersSetForRoomsAndUserSQL = "" +
	"SELECT target_nid, COUNT(room_nid) FROM roomserver_membership" +
	" WHERE (target_local OR $1 = false)" +
	" AND room_nid IN ($2) AND target_nid IN ($3)" +
	" AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	" AND forgotten = false" +
	" GROUP BY target_nid"

var selectJoinedUsersSetForRoomsSQL = "" +
	"SELECT target_nid, COUNT(room_nid) FROM roomserver_membership" +
	" WHERE (target_local OR $1 = false)" +
	" AND room_nid IN ($2)" +
	" AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	" AND forgotten = false" +
	" GROUP BY target_nid"

// Insert a row in to membership table so that it can be locked by the
// SELECT FOR UPDATE
const insertMembershipSQL = "" +
	"INSERT INTO roomserver_membership (room_nid, target_nid, target_local)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT DO NOTHING"

const selectMembershipFromRoomAndTargetSQL = "" +
	"SELECT membership_nid, event_nid, forgotten FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND target_nid = $2"

const selectMembershipsFromRoomAndMembershipSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND membership_nid = $2 and forgotten = false"

const selectLocalMembershipsFromRoomAndMembershipSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 AND membership_nid = $2" +
	" AND target_local = true and forgotten = false"

const selectMembershipsFromRoomSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0 and forgotten = false"

const selectLocalMembershipsFromRoomSQL = "" +
	"SELECT event_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND event_nid != 0" +
	" AND target_local = true and forgotten = false"

const selectMembershipForUpdateSQL = "" +
	"SELECT membership_nid FROM roomserver_membership" +
	" WHERE room_nid = $1 AND target_nid = $2"

const updateMembershipSQL = "" +
	"UPDATE roomserver_membership SET sender_nid = $1, membership_nid = $2, event_nid = $3, forgotten = $4" +
	" WHERE room_nid = $5 AND target_nid = $6"

const updateMembershipForgetRoom = "" +
	"UPDATE roomserver_membership SET forgotten = $1" +
	" WHERE room_nid = $2 AND target_nid = $3"

const selectRoomsWithMembershipSQL = "" +
	"SELECT room_nid FROM roomserver_membership WHERE membership_nid = $1 AND target_nid = $2 and forgotten = false"

// selectKnownUsersSQL uses a sub-select statement here to find rooms that the user is
// joined to. Since this information is used to populate the user directory, we will
// only return users that the user would ordinarily be able to see anyway.
var selectKnownUsersSQL = "" +
	"SELECT DISTINCT event_state_key FROM roomserver_membership INNER JOIN roomserver_event_state_keys ON " +
	"roomserver_membership.target_nid = roomserver_event_state_keys.event_state_key_nid" +
	" WHERE room_nid IN (" +
	"  SELECT DISTINCT room_nid FROM roomserver_membership WHERE target_nid=$1 AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) +
	") AND membership_nid = " + fmt.Sprintf("%d", tables.MembershipStateJoin) + " AND event_state_key LIKE $2 LIMIT $3"

// selectLocalServerInRoomSQL is an optimised case for checking if we, the local server,
// are in the room by using the target_local column of the membership table. Normally when
// we want to know if a server is in a room, we have to unmarshal the entire room state which
// is expensive. The presence of a single row from this query suggests we're still in the
// room, no rows returned suggests we aren't.
const selectLocalServerInRoomSQL = "" +
	"SELECT room_nid FROM roomserver_membership WHERE target_local = 1 AND membership_nid = $1 AND room_nid = $2 LIMIT 1"

// selectServerMembersInRoomSQL is an optimised case for checking for server members in a room.
// The JOIN is significantly leaner than the previous case of looking up event NIDs and reading the
// membership events from the database, as the JOIN query amounts to little more than two index
// scans which are very fast. The presence of a single row from this query suggests the server is
// in the room, no rows returned suggests they aren't.
const selectServerInRoomSQL = "" +
	"SELECT room_nid FROM roomserver_membership" +
	" JOIN roomserver_event_state_keys ON roomserver_membership.target_nid = roomserver_event_state_keys.event_state_key_nid" +
	" WHERE membership_nid = $1 AND room_nid = $2 AND event_state_key LIKE '%:' || $3 LIMIT 1"

const deleteMembershipSQL = "" +
	"DELETE FROM roomserver_membership WHERE room_nid = $1 AND target_nid = $2"

const selectJoinedUsersSQL = `
SELECT DISTINCT target_nid
FROM roomserver_membership m
WHERE membership_nid > $1 AND target_nid IN ($2)
`

type membershipStatements struct {
	db                                              *sql.DB
	insertMembershipStmt                            *sql.Stmt
	selectMembershipForUpdateStmt                   *sql.Stmt
	selectMembershipFromRoomAndTargetStmt           *sql.Stmt
	selectMembershipsFromRoomAndMembershipStmt      *sql.Stmt
	selectLocalMembershipsFromRoomAndMembershipStmt *sql.Stmt
	selectMembershipsFromRoomStmt                   *sql.Stmt
	selectLocalMembershipsFromRoomStmt              *sql.Stmt
	selectRoomsWithMembershipStmt                   *sql.Stmt
	updateMembershipStmt                            *sql.Stmt
	selectKnownUsersStmt                            *sql.Stmt
	updateMembershipForgetRoomStmt                  *sql.Stmt
	selectLocalServerInRoomStmt                     *sql.Stmt
	selectServerInRoomStmt                          *sql.Stmt
	deleteMembershipStmt                            *sql.Stmt
	// selectJoinedUsersStmt                           *sql.Stmt // Prepared at runtime
}

func CreateMembershipTable(db *sql.DB) error {
	_, err := db.Exec(membershipSchema)
	if err != nil {
		return err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "roomserver: add forgotten column",
		Up:      deltas.UpAddForgottenColumn,
	})
	return m.Up(context.Background())
}

func PrepareMembershipTable(db *sql.DB) (tables.Membership, error) {
	s := &membershipStatements{
		db: db,
	}

	return s, sqlutil.StatementList{
		{&s.insertMembershipStmt, insertMembershipSQL},
		{&s.selectMembershipForUpdateStmt, selectMembershipForUpdateSQL},
		{&s.selectMembershipFromRoomAndTargetStmt, selectMembershipFromRoomAndTargetSQL},
		{&s.selectMembershipsFromRoomAndMembershipStmt, selectMembershipsFromRoomAndMembershipSQL},
		{&s.selectLocalMembershipsFromRoomAndMembershipStmt, selectLocalMembershipsFromRoomAndMembershipSQL},
		{&s.selectMembershipsFromRoomStmt, selectMembershipsFromRoomSQL},
		{&s.selectLocalMembershipsFromRoomStmt, selectLocalMembershipsFromRoomSQL},
		{&s.updateMembershipStmt, updateMembershipSQL},
		{&s.selectRoomsWithMembershipStmt, selectRoomsWithMembershipSQL},
		{&s.selectKnownUsersStmt, selectKnownUsersSQL},
		{&s.updateMembershipForgetRoomStmt, updateMembershipForgetRoom},
		{&s.selectLocalServerInRoomStmt, selectLocalServerInRoomSQL},
		{&s.selectServerInRoomStmt, selectServerInRoomSQL},
		{&s.deleteMembershipStmt, deleteMembershipSQL},
	}.Prepare(db)
}

func (s *membershipStatements) InsertMembership(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
	localTarget bool,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertMembershipStmt)
	_, err := stmt.ExecContext(ctx, roomNID, targetUserNID, localTarget)
	return err
}

func (s *membershipStatements) SelectMembershipForUpdate(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) (membership tables.MembershipState, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembershipForUpdateStmt)
	err = stmt.QueryRowContext(
		ctx, roomNID, targetUserNID,
	).Scan(&membership)
	return
}

func (s *membershipStatements) SelectMembershipFromRoomAndTarget(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) (eventNID types.EventNID, membership tables.MembershipState, forgotten bool, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembershipFromRoomAndTargetStmt)
	err = stmt.QueryRowContext(
		ctx, roomNID, targetUserNID,
	).Scan(&membership, &eventNID, &forgotten)
	return
}

func (s *membershipStatements) SelectMembershipsFromRoom(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, localOnly bool,
) (eventNIDs []types.EventNID, err error) {
	var selectStmt *sql.Stmt
	if localOnly {
		selectStmt = s.selectLocalMembershipsFromRoomStmt
	} else {
		selectStmt = s.selectMembershipsFromRoomStmt
	}
	selectStmt = sqlutil.TxStmt(txn, selectStmt)
	rows, err := selectStmt.QueryContext(ctx, roomNID)
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "selectMembershipsFromRoom: rows.close() failed")

	var eNID types.EventNID
	for rows.Next() {
		if err = rows.Scan(&eNID); err != nil {
			return
		}
		eventNIDs = append(eventNIDs, eNID)
	}
	err = rows.Err()
	return
}

func (s *membershipStatements) SelectMembershipsFromRoomAndMembership(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, membership tables.MembershipState, localOnly bool,
) (eventNIDs []types.EventNID, err error) {
	var stmt *sql.Stmt
	if localOnly {
		stmt = s.selectLocalMembershipsFromRoomAndMembershipStmt
	} else {
		stmt = s.selectMembershipsFromRoomAndMembershipStmt
	}
	stmt = sqlutil.TxStmt(txn, stmt)
	rows, err := stmt.QueryContext(ctx, roomNID, membership)
	if err != nil {
		return
	}
	defer external.CloseAndLogIfError(ctx, rows, "selectMembershipsFromRoomAndMembership: rows.close() failed")

	var eNID types.EventNID
	for rows.Next() {
		if err = rows.Scan(&eNID); err != nil {
			return
		}
		eventNIDs = append(eventNIDs, eNID)
	}
	err = rows.Err()
	return
}

func (s *membershipStatements) UpdateMembership(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID, senderUserNID types.EventStateKeyNID, membership tables.MembershipState,
	eventNID types.EventNID, forgotten bool,
) (bool, error) {
	stmt := sqlutil.TxStmt(txn, s.updateMembershipStmt)
	res, err := stmt.ExecContext(
		ctx, senderUserNID, membership, eventNID, forgotten, roomNID, targetUserNID,
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (s *membershipStatements) SelectRoomsWithMembership(
	ctx context.Context, txn *sql.Tx, userID types.EventStateKeyNID, membershipState tables.MembershipState,
) ([]types.RoomNID, error) {
	stmt := sqlutil.TxStmt(txn, s.selectRoomsWithMembershipStmt)
	rows, err := stmt.QueryContext(ctx, membershipState, userID)
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "SelectRoomsWithMembership: rows.close() failed")
	var roomNIDs []types.RoomNID
	var roomNID types.RoomNID
	for rows.Next() {
		if err := rows.Scan(&roomNID); err != nil {
			return nil, err
		}
		roomNIDs = append(roomNIDs, roomNID)
	}
	return roomNIDs, rows.Err()
}

func (s *membershipStatements) SelectJoinedUsersSetForRooms(ctx context.Context, txn *sql.Tx, roomNIDs []types.RoomNID, userNIDs []types.EventStateKeyNID, localOnly bool) (map[types.EventStateKeyNID]int, error) {
	params := make([]interface{}, 0, 1+len(roomNIDs)+len(userNIDs))
	params = append(params, localOnly)
	for _, v := range roomNIDs {
		params = append(params, v)
	}
	for _, v := range userNIDs {
		params = append(params, v)
	}

	query := strings.Replace(selectJoinedUsersSetForRoomsSQL, "($2)", sqlutil.QueryVariadicOffset(len(roomNIDs), 1), 1)
	if len(userNIDs) > 0 {
		query = strings.Replace(selectJoinedUsersSetForRoomsAndUserSQL, "($2)", sqlutil.QueryVariadicOffset(len(roomNIDs), 1), 1)
		query = strings.Replace(query, "($3)", sqlutil.QueryVariadicOffset(len(userNIDs), len(roomNIDs)+1), 1)
	}
	var rows *sql.Rows
	var err error
	if txn != nil {
		rows, err = txn.QueryContext(ctx, query, params...)
	} else {
		rows, err = s.db.QueryContext(ctx, query, params...)
	}
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "selectJoinedUsersSetForRooms: rows.close() failed")
	result := make(map[types.EventStateKeyNID]int)
	var userID types.EventStateKeyNID
	var count int
	for rows.Next() {
		if err := rows.Scan(&userID, &count); err != nil {
			return nil, err
		}
		result[userID] = count
	}
	return result, rows.Err()
}

func (s *membershipStatements) SelectKnownUsers(ctx context.Context, txn *sql.Tx, userID types.EventStateKeyNID, searchString string, limit int) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectKnownUsersStmt)
	rows, err := stmt.QueryContext(ctx, userID, fmt.Sprintf("%%%s%%", searchString), limit)
	if err != nil {
		return nil, err
	}
	result := []string{}
	defer external.CloseAndLogIfError(ctx, rows, "SelectKnownUsers: rows.close() failed")
	var resUserID string
	for rows.Next() {
		if err := rows.Scan(&resUserID); err != nil {
			return nil, err
		}
		result = append(result, resUserID)
	}
	return result, rows.Err()
}

func (s *membershipStatements) UpdateForgetMembership(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
	forget bool,
) error {
	_, err := sqlutil.TxStmt(txn, s.updateMembershipForgetRoomStmt).ExecContext(
		ctx, forget, roomNID, targetUserNID,
	)
	return err
}

func (s *membershipStatements) SelectLocalServerInRoom(ctx context.Context, txn *sql.Tx, roomNID types.RoomNID) (bool, error) {
	var nid types.RoomNID
	stmt := sqlutil.TxStmt(txn, s.selectLocalServerInRoomStmt)
	err := stmt.QueryRowContext(ctx, tables.MembershipStateJoin, roomNID).Scan(&nid)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	found := nid > 0
	return found, nil
}

func (s *membershipStatements) SelectServerInRoom(ctx context.Context, txn *sql.Tx, roomNID types.RoomNID, serverName spec.ServerName) (bool, error) {
	var nid types.RoomNID
	stmt := sqlutil.TxStmt(txn, s.selectServerInRoomStmt)
	err := stmt.QueryRowContext(ctx, tables.MembershipStateJoin, roomNID, serverName).Scan(&nid)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return roomNID == nid, nil
}

func (s *membershipStatements) DeleteMembership(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, targetUserNID types.EventStateKeyNID,
) error {
	_, err := sqlutil.TxStmt(txn, s.deleteMembershipStmt).ExecContext(
		ctx, roomNID, targetUserNID,
	)
	return err
}

func (s *membershipStatements) SelectJoinedUsers(
	ctx context.Context, txn *sql.Tx,
	targetUserNIDs []types.EventStateKeyNID,
) ([]types.EventStateKeyNID, error) {
	result := make([]types.EventStateKeyNID, 0, len(targetUserNIDs))

	qry := strings.Replace(selectJoinedUsersSQL, "($2)", sqlutil.QueryVariadicOffset(len(targetUserNIDs), 1), 1)

	stmt, err := s.db.Prepare(qry)
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, stmt, "SelectJoinedUsers: stmt.Close failed")

	params := make([]any, len(targetUserNIDs)+1)
	params[0] = tables.MembershipStateLeaveOrBan
	for i := range targetUserNIDs {
		params[i+1] = targetUserNIDs[i]
	}

	stmt = sqlutil.TxStmt(txn, stmt)
	rows, err := stmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "SelectJoinedUsers: rows.close() failed")
	var targetNID types.EventStateKeyNID
	for rows.Next() {
		if err = rows.Scan(&targetNID); err != nil {
			return nil, err
		}
		result = append(result, targetNID)
	}

	return result, rows.Err()
}
