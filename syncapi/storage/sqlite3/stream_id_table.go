package sqlite3

import (
	"context"
	"database/sql"

	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/syncapi/types"
)

const streamIDTableSchema = `
-- Global stream ID counter, used by other tables.
CREATE TABLE IF NOT EXISTS syncapi_stream_id (
  stream_name TEXT NOT NULL PRIMARY KEY,
  stream_id INT DEFAULT 0,

  UNIQUE(stream_name)
);
INSERT INTO syncapi_stream_id (stream_name, stream_id) VALUES ("global", 0)
  ON CONFLICT DO NOTHING;
INSERT INTO syncapi_stream_id (stream_name, stream_id) VALUES ("receipt", 0)
  ON CONFLICT DO NOTHING;
INSERT INTO syncapi_stream_id (stream_name, stream_id) VALUES ("accountdata", 0)
  ON CONFLICT DO NOTHING;
INSERT INTO syncapi_stream_id (stream_name, stream_id) VALUES ("invite", 0)
  ON CONFLICT DO NOTHING;
INSERT INTO syncapi_stream_id (stream_name, stream_id) VALUES ("presence", 0)
  ON CONFLICT DO NOTHING;
INSERT INTO syncapi_stream_id (stream_name, stream_id) VALUES ("notification", 0)
  ON CONFLICT DO NOTHING;
INSERT INTO syncapi_stream_id (stream_name, stream_id) VALUES ("relation", 0)
  ON CONFLICT DO NOTHING;
`

const increaseStreamIDStmt = "" +
	"UPDATE syncapi_stream_id SET stream_id = stream_id + 1 WHERE stream_name = $1" +
	" RETURNING stream_id"

type StreamIDStatements struct {
	db                   *sql.DB
	increaseStreamIDStmt *sql.Stmt
}

func (s *StreamIDStatements) Prepare(db *sql.DB) (err error) {
	s.db = db
	_, err = db.Exec(streamIDTableSchema)
	if err != nil {
		return
	}
	return sqlutil.StatementList{
		{&s.increaseStreamIDStmt, increaseStreamIDStmt},
	}.Prepare(db)
}

func (s *StreamIDStatements) nextPDUID(ctx context.Context, txn *sql.Tx) (pos types.StreamPosition, err error) {
	increaseStmt := sqlutil.TxStmt(txn, s.increaseStreamIDStmt)
	err = increaseStmt.QueryRowContext(ctx, "global").Scan(&pos)
	return
}

func (s *StreamIDStatements) nextReceiptID(ctx context.Context, txn *sql.Tx) (pos types.StreamPosition, err error) {
	increaseStmt := sqlutil.TxStmt(txn, s.increaseStreamIDStmt)
	err = increaseStmt.QueryRowContext(ctx, "receipt").Scan(&pos)
	return
}

func (s *StreamIDStatements) nextInviteID(ctx context.Context, txn *sql.Tx) (pos types.StreamPosition, err error) {
	increaseStmt := sqlutil.TxStmt(txn, s.increaseStreamIDStmt)
	err = increaseStmt.QueryRowContext(ctx, "invite").Scan(&pos)
	return
}

func (s *StreamIDStatements) nextAccountDataID(ctx context.Context, txn *sql.Tx) (pos types.StreamPosition, err error) {
	increaseStmt := sqlutil.TxStmt(txn, s.increaseStreamIDStmt)
	err = increaseStmt.QueryRowContext(ctx, "accountdata").Scan(&pos)
	return
}

func (s *StreamIDStatements) nextPresenceID(ctx context.Context, txn *sql.Tx) (pos types.StreamPosition, err error) {
	increaseStmt := sqlutil.TxStmt(txn, s.increaseStreamIDStmt)
	err = increaseStmt.QueryRowContext(ctx, "presence").Scan(&pos)
	return
}

func (s *StreamIDStatements) nextNotificationID(ctx context.Context, txn *sql.Tx) (pos types.StreamPosition, err error) {
	increaseStmt := sqlutil.TxStmt(txn, s.increaseStreamIDStmt)
	err = increaseStmt.QueryRowContext(ctx, "notification").Scan(&pos)
	return
}

func (s *StreamIDStatements) nextRelationID(ctx context.Context, txn *sql.Tx) (pos types.StreamPosition, err error) {
	increaseStmt := sqlutil.TxStmt(txn, s.increaseStreamIDStmt)
	err = increaseStmt.QueryRowContext(ctx, "relation").Scan(&pos)
	return
}
