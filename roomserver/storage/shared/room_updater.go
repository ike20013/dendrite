package shared

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/matrix-org/gomatrixserverlib"

	"github.com/ike20013/dendrite/roomserver/types"
)

type RoomUpdater struct {
	transaction
	d                       *Database
	roomInfo                *types.RoomInfo
	latestEvents            []types.StateAtEventAndReference
	lastEventIDSent         string
	currentStateSnapshotNID types.StateSnapshotNID
	roomExists              bool
}

func rollback(txn *sql.Tx) {
	if txn == nil {
		return
	}
	txn.Rollback() // nolint: errcheck
}

func NewRoomUpdater(ctx context.Context, d *Database, txn *sql.Tx, roomInfo *types.RoomInfo) (*RoomUpdater, error) {
	// If the roomInfo is nil then that means that the room doesn't exist
	// yet, so we can't do `SelectLatestEventsNIDsForUpdate` because that
	// would involve locking a row on the table that doesn't exist. Instead
	// we will just run with a normal database transaction. It'll either
	// succeed, processing a create event which creates the room, or it won't.
	if roomInfo == nil {
		return &RoomUpdater{
			transaction{ctx, txn}, d, nil, nil, "", 0, false,
		}, nil
	}

	eventNIDs, lastEventNIDSent, currentStateSnapshotNID, err :=
		d.RoomsTable.SelectLatestEventsNIDsForUpdate(ctx, txn, roomInfo.RoomNID)
	if err != nil {
		rollback(txn)
		return nil, err
	}
	stateAndRefs, err := d.EventsTable.BulkSelectStateAtEventAndReference(ctx, txn, eventNIDs)
	if err != nil {
		rollback(txn)
		return nil, err
	}
	var lastEventIDSent string
	if lastEventNIDSent != 0 {
		lastEventIDSent, err = d.EventsTable.SelectEventID(ctx, txn, lastEventNIDSent)
		if err != nil {
			rollback(txn)
			return nil, err
		}
	}
	return &RoomUpdater{
		transaction{ctx, txn}, d, roomInfo, stateAndRefs, lastEventIDSent, currentStateSnapshotNID, true,
	}, nil
}

// RoomExists returns true if the room exists and false otherwise.
func (u *RoomUpdater) RoomExists() bool {
	return u.roomExists
}

// Implements sqlutil.Transaction
func (u *RoomUpdater) Commit() error {
	if u.txn == nil { // SQLite mode probably
		return nil
	}
	return u.txn.Commit()
}

// Implements sqlutil.Transaction
func (u *RoomUpdater) Rollback() error {
	if u.txn == nil { // SQLite mode probably
		return nil
	}
	return u.txn.Rollback()
}

// RoomVersion implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) RoomVersion() (version gomatrixserverlib.RoomVersion) {
	return u.roomInfo.RoomVersion
}

// LatestEvents implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) LatestEvents() []types.StateAtEventAndReference {
	return u.latestEvents
}

// LastEventIDSent implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) LastEventIDSent() string {
	return u.lastEventIDSent
}

// CurrentStateSnapshotNID implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) CurrentStateSnapshotNID() types.StateSnapshotNID {
	return u.currentStateSnapshotNID
}

func (u *RoomUpdater) Events(ctx context.Context, _ gomatrixserverlib.RoomVersion, eventNIDs []types.EventNID) ([]types.Event, error) {
	if u.roomInfo == nil {
		return nil, types.ErrorInvalidRoomInfo
	}
	return u.d.events(ctx, u.txn, u.roomInfo.RoomVersion, eventNIDs)
}

func (u *RoomUpdater) SnapshotNIDFromEventID(
	ctx context.Context, eventID string,
) (types.StateSnapshotNID, error) {
	return u.d.snapshotNIDFromEventID(ctx, u.txn, eventID)
}

func (u *RoomUpdater) StateBlockNIDs(
	ctx context.Context, stateNIDs []types.StateSnapshotNID,
) ([]types.StateBlockNIDList, error) {
	return u.d.stateBlockNIDs(ctx, u.txn, stateNIDs)
}

func (u *RoomUpdater) StateEntries(
	ctx context.Context, stateBlockNIDs []types.StateBlockNID,
) ([]types.StateEntryList, error) {
	return u.d.stateEntries(ctx, u.txn, stateBlockNIDs)
}

func (u *RoomUpdater) StateEntriesForTuples(
	ctx context.Context,
	stateBlockNIDs []types.StateBlockNID,
	stateKeyTuples []types.StateKeyTuple,
) ([]types.StateEntryList, error) {
	return u.d.stateEntriesForTuples(ctx, u.txn, stateBlockNIDs, stateKeyTuples)
}

func (u *RoomUpdater) AddState(
	ctx context.Context,
	roomNID types.RoomNID,
	stateBlockNIDs []types.StateBlockNID,
	state []types.StateEntry,
) (stateNID types.StateSnapshotNID, err error) {
	return u.d.addState(ctx, u.txn, roomNID, stateBlockNIDs, state)
}

func (u *RoomUpdater) SetState(
	ctx context.Context, eventNID types.EventNID, stateNID types.StateSnapshotNID,
) error {
	return u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		return u.d.EventsTable.UpdateEventState(ctx, txn, eventNID, stateNID)
	})
}

func (u *RoomUpdater) EventTypeNIDs(
	ctx context.Context, eventTypes []string,
) (map[string]types.EventTypeNID, error) {
	return u.d.eventTypeNIDs(ctx, u.txn, eventTypes)
}

func (u *RoomUpdater) EventStateKeyNIDs(
	ctx context.Context, eventStateKeys []string,
) (map[string]types.EventStateKeyNID, error) {
	return u.d.eventStateKeyNIDs(ctx, u.txn, eventStateKeys)
}

func (u *RoomUpdater) RoomInfo(ctx context.Context, roomID string) (*types.RoomInfo, error) {
	return u.d.roomInfo(ctx, u.txn, roomID)
}

func (u *RoomUpdater) EventIDs(
	ctx context.Context, eventNIDs []types.EventNID,
) (map[types.EventNID]string, error) {
	return u.d.EventsTable.BulkSelectEventID(ctx, u.txn, eventNIDs)
}

func (u *RoomUpdater) BulkSelectSnapshotsFromEventIDs(ctx context.Context, eventIDs []string) (map[types.StateSnapshotNID][]string, error) {
	return u.d.EventsTable.BulkSelectSnapshotsFromEventIDs(ctx, u.txn, eventIDs)
}

func (u *RoomUpdater) StateAtEventIDs(
	ctx context.Context, eventIDs []string,
) ([]types.StateAtEvent, error) {
	return u.d.EventsTable.BulkSelectStateAtEventByID(ctx, u.txn, eventIDs)
}

func (u *RoomUpdater) EventsFromIDs(ctx context.Context, roomInfo *types.RoomInfo, eventIDs []string) ([]types.Event, error) {
	return u.d.eventsFromIDs(ctx, u.txn, u.roomInfo, eventIDs, NoFilter)
}

// IsReferenced implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) IsReferenced(eventID string) (bool, error) {
	err := u.d.PrevEventsTable.SelectPreviousEventExists(u.ctx, u.txn, eventID)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, fmt.Errorf("u.d.PrevEventsTable.SelectPreviousEventExists: %w", err)
}

// SetLatestEvents implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) SetLatestEvents(
	roomNID types.RoomNID, latest []types.StateAtEventAndReference, lastEventNIDSent types.EventNID,
	currentStateSnapshotNID types.StateSnapshotNID,
) error {
	switch {
	case len(latest) == 0:
		return fmt.Errorf("cannot set latest events with no latest event references")
	case currentStateSnapshotNID == 0:
		return fmt.Errorf("cannot set latest events with invalid state snapshot NID")
	case lastEventNIDSent == 0:
		return fmt.Errorf("cannot set latest events with invalid latest event NID")
	}
	eventNIDs := make([]types.EventNID, len(latest))
	for i := range latest {
		eventNIDs[i] = latest[i].EventNID
	}
	return u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		if err := u.d.RoomsTable.UpdateLatestEventNIDs(u.ctx, txn, roomNID, eventNIDs, lastEventNIDSent, currentStateSnapshotNID); err != nil {
			return fmt.Errorf("u.d.RoomsTable.updateLatestEventNIDs: %w", err)
		}

		// Since it's entirely possible that this types.RoomInfo came from the
		// cache, we should make sure to update that entry so that the next run
		// works from live data.
		if u.roomInfo != nil {
			u.roomInfo.SetStateSnapshotNID(currentStateSnapshotNID)
			u.roomInfo.SetIsStub(false)
		}
		return nil
	})
}

// HasEventBeenSent implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) HasEventBeenSent(eventNID types.EventNID) (bool, error) {
	return u.d.EventsTable.SelectEventSentToOutput(u.ctx, u.txn, eventNID)
}

// MarkEventAsSent implements types.RoomRecentEventsUpdater
func (u *RoomUpdater) MarkEventAsSent(eventNID types.EventNID) error {
	return u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		return u.d.EventsTable.UpdateEventSentToOutput(u.ctx, txn, eventNID)
	})
}

func (u *RoomUpdater) MembershipUpdater(targetUserNID types.EventStateKeyNID, targetLocal bool) (*MembershipUpdater, error) {
	return u.d.membershipUpdaterTxn(u.ctx, u.txn, u.roomInfo.RoomNID, targetUserNID, targetLocal)
}

func (u *RoomUpdater) IsEventRejected(ctx context.Context, roomNID types.RoomNID, eventID string) (bool, error) {
	return u.d.IsEventRejected(ctx, roomNID, eventID)
}
