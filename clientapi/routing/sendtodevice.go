// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"net/http"

	"github.com/matrix-org/util"

	"github.com/ike20013/dendrite/clientapi/httputil"
	"github.com/ike20013/dendrite/clientapi/producers"
	"github.com/ike20013/dendrite/external/transactions"
	userapi "github.com/ike20013/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// SendToDevice handles PUT /_matrix/client/r0/sendToDevice/{eventType}/{txnId}
// sends the device events to the syncapi & federationsender
func SendToDevice(
	req *http.Request, device *userapi.Device,
	syncProducer *producers.SyncAPIProducer,
	txnCache *transactions.Cache,
	eventType string, txnID *string,
) util.JSONResponse {
	if txnID != nil {
		if res, ok := txnCache.FetchTransaction(device.AccessToken, *txnID, req.URL); ok {
			return *res
		}
	}

	var httpReq struct {
		Messages map[string]map[string]json.RawMessage `json:"messages"`
	}
	resErr := httputil.UnmarshalJSONRequest(req, &httpReq)
	if resErr != nil {
		return *resErr
	}

	for userID, byUser := range httpReq.Messages {
		for deviceID, message := range byUser {
			if err := syncProducer.SendToDevice(
				req.Context(), device.UserID, userID, deviceID, eventType, message,
			); err != nil {
				util.GetLogger(req.Context()).WithError(err).Error("eduProducer.SendToDevice failed")
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
		}
	}

	res := util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}

	if txnID != nil {
		txnCache.AddTransaction(device.AccessToken, *txnID, req.URL, &res)
	}

	return res
}
