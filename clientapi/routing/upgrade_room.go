// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"errors"
	"net/http"

	appserviceAPI "github.com/ike20013/dendrite/appservice/api"
	"github.com/ike20013/dendrite/clientapi/httputil"
	"github.com/ike20013/dendrite/external/eventutil"
	roomserverAPI "github.com/ike20013/dendrite/roomserver/api"
	"github.com/ike20013/dendrite/roomserver/version"
	"github.com/ike20013/dendrite/setup/config"
	userapi "github.com/ike20013/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

type upgradeRoomRequest struct {
	NewVersion string `json:"new_version"`
}

type upgradeRoomResponse struct {
	ReplacementRoom string `json:"replacement_room"`
}

// UpgradeRoom implements /upgrade
func UpgradeRoom(
	req *http.Request, device *userapi.Device,
	cfg *config.ClientAPI,
	roomID string, profileAPI userapi.ClientUserAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
) util.JSONResponse {
	var r upgradeRoomRequest
	if rErr := httputil.UnmarshalJSONRequest(req, &r); rErr != nil {
		return *rErr
	}

	// Validate that the room version is supported
	if _, err := version.SupportedRoomVersion(gomatrixserverlib.RoomVersion(r.NewVersion)); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion("This server does not support that room version"),
		}
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("device UserID is invalid")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	newRoomID, err := rsAPI.PerformRoomUpgrade(req.Context(), roomID, *userID, gomatrixserverlib.RoomVersion(r.NewVersion))
	switch e := err.(type) {
	case nil:
	case roomserverAPI.ErrNotAllowed:
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(e.Error()),
		}
	default:
		if errors.Is(err, eventutil.ErrRoomNoExists{}) {
			return util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("Room does not exist"),
			}
		}
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: upgradeRoomResponse{
			ReplacementRoom: newRoomID,
		},
	}
}
