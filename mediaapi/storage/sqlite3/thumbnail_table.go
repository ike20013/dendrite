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
	"time"

	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/mediaapi/storage/tables"
	"github.com/ike20013/dendrite/mediaapi/types"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const thumbnailSchema = `
-- The mediaapi_thumbnail table holds metadata for each thumbnail file stored and accessible to the local server,
-- the actual file is stored separately.
CREATE TABLE IF NOT EXISTS mediaapi_thumbnail (
    media_id TEXT NOT NULL,
    media_origin TEXT NOT NULL,
    content_type TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    creation_ts INTEGER NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    resize_method TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS mediaapi_thumbnail_index ON mediaapi_thumbnail (media_id, media_origin, width, height, resize_method);
`

const insertThumbnailSQL = `
INSERT INTO mediaapi_thumbnail (media_id, media_origin, content_type, file_size_bytes, creation_ts, width, height, resize_method)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`

// Note: this selects one specific thumbnail
const selectThumbnailSQL = `
SELECT content_type, file_size_bytes, creation_ts FROM mediaapi_thumbnail WHERE media_id = $1 AND media_origin = $2 AND width = $3 AND height = $4 AND resize_method = $5
`

// Note: this selects all thumbnails for a media_origin and media_id
const selectThumbnailsSQL = `
SELECT content_type, file_size_bytes, creation_ts, width, height, resize_method FROM mediaapi_thumbnail WHERE media_id = $1 AND media_origin = $2 ORDER BY creation_ts ASC
`

type thumbnailStatements struct {
	insertThumbnailStmt  *sql.Stmt
	selectThumbnailStmt  *sql.Stmt
	selectThumbnailsStmt *sql.Stmt
}

func NewSQLiteThumbnailsTable(db *sql.DB) (tables.Thumbnails, error) {
	s := &thumbnailStatements{}
	_, err := db.Exec(thumbnailSchema)
	if err != nil {
		return nil, err
	}

	return s, sqlutil.StatementList{
		{&s.insertThumbnailStmt, insertThumbnailSQL},
		{&s.selectThumbnailStmt, selectThumbnailSQL},
		{&s.selectThumbnailsStmt, selectThumbnailsSQL},
	}.Prepare(db)
}

func (s *thumbnailStatements) InsertThumbnail(ctx context.Context, txn *sql.Tx, thumbnailMetadata *types.ThumbnailMetadata) error {
	thumbnailMetadata.MediaMetadata.CreationTimestamp = spec.AsTimestamp(time.Now())
	_, err := sqlutil.TxStmtContext(ctx, txn, s.insertThumbnailStmt).ExecContext(
		ctx,
		thumbnailMetadata.MediaMetadata.MediaID,
		thumbnailMetadata.MediaMetadata.Origin,
		thumbnailMetadata.MediaMetadata.ContentType,
		thumbnailMetadata.MediaMetadata.FileSizeBytes,
		thumbnailMetadata.MediaMetadata.CreationTimestamp,
		thumbnailMetadata.ThumbnailSize.Width,
		thumbnailMetadata.ThumbnailSize.Height,
		thumbnailMetadata.ThumbnailSize.ResizeMethod,
	)
	return err
}

func (s *thumbnailStatements) SelectThumbnail(
	ctx context.Context,
	txn *sql.Tx,
	mediaID types.MediaID,
	mediaOrigin spec.ServerName,
	width, height int,
	resizeMethod string,
) (*types.ThumbnailMetadata, error) {
	thumbnailMetadata := types.ThumbnailMetadata{
		MediaMetadata: &types.MediaMetadata{
			MediaID: mediaID,
			Origin:  mediaOrigin,
		},
		ThumbnailSize: types.ThumbnailSize{
			Width:        width,
			Height:       height,
			ResizeMethod: resizeMethod,
		},
	}
	err := sqlutil.TxStmtContext(ctx, txn, s.selectThumbnailStmt).QueryRowContext(
		ctx,
		thumbnailMetadata.MediaMetadata.MediaID,
		thumbnailMetadata.MediaMetadata.Origin,
		thumbnailMetadata.ThumbnailSize.Width,
		thumbnailMetadata.ThumbnailSize.Height,
		thumbnailMetadata.ThumbnailSize.ResizeMethod,
	).Scan(
		&thumbnailMetadata.MediaMetadata.ContentType,
		&thumbnailMetadata.MediaMetadata.FileSizeBytes,
		&thumbnailMetadata.MediaMetadata.CreationTimestamp,
	)
	return &thumbnailMetadata, err
}

func (s *thumbnailStatements) SelectThumbnails(
	ctx context.Context, txn *sql.Tx, mediaID types.MediaID,
	mediaOrigin spec.ServerName,
) ([]*types.ThumbnailMetadata, error) {
	rows, err := sqlutil.TxStmtContext(ctx, txn, s.selectThumbnailsStmt).QueryContext(
		ctx, mediaID, mediaOrigin,
	)
	if err != nil {
		return nil, err
	}
	defer external.CloseAndLogIfError(ctx, rows, "selectThumbnails: rows.close() failed")

	var thumbnails []*types.ThumbnailMetadata
	for rows.Next() {
		thumbnailMetadata := types.ThumbnailMetadata{
			MediaMetadata: &types.MediaMetadata{
				MediaID: mediaID,
				Origin:  mediaOrigin,
			},
		}
		err = rows.Scan(
			&thumbnailMetadata.MediaMetadata.ContentType,
			&thumbnailMetadata.MediaMetadata.FileSizeBytes,
			&thumbnailMetadata.MediaMetadata.CreationTimestamp,
			&thumbnailMetadata.ThumbnailSize.Width,
			&thumbnailMetadata.ThumbnailSize.Height,
			&thumbnailMetadata.ThumbnailSize.ResizeMethod,
		)
		if err != nil {
			return nil, err
		}
		thumbnails = append(thumbnails, &thumbnailMetadata)
	}

	return thumbnails, rows.Err()
}
