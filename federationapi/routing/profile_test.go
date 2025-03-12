// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing_test

import (
	"context"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gorilla/mux"
	"github.com/ike20013/dendrite/clientapi/auth/authtypes"
	"github.com/ike20013/dendrite/cmd/dendrite-demo-yggdrasil/signing"
	"github.com/ike20013/dendrite/external/caching"
	"github.com/ike20013/dendrite/external/httputil"
	"github.com/ike20013/dendrite/external/sqlutil"
	fedAPI "github.com/ike20013/dendrite/federationapi"
	"github.com/ike20013/dendrite/federationapi/routing"
	"github.com/ike20013/dendrite/setup/jetstream"
	"github.com/ike20013/dendrite/test"
	"github.com/ike20013/dendrite/test/testrig"
	userAPI "github.com/ike20013/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/ed25519"
)

type fakeUserAPI struct {
	userAPI.FederationUserAPI
}

func (u *fakeUserAPI) QueryProfile(ctx context.Context, userID string) (*authtypes.Profile, error) {
	return &authtypes.Profile{}, nil
}

func TestHandleQueryProfile(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		routers := httputil.NewRouters()
		defer close()

		fedMux := mux.NewRouter().SkipClean(true).PathPrefix(httputil.PublicFederationPathPrefix).Subrouter().UseEncodedPath()
		natsInstance := jetstream.NATSInstance{}
		routers.Federation = fedMux
		cfg.FederationAPI.Matrix.SigningIdentity.ServerName = testOrigin
		cfg.FederationAPI.Matrix.Metrics.Enabled = false
		fedClient := fakeFedClient{}
		serverKeyAPI := &signing.YggdrasilKeys{}
		keyRing := serverKeyAPI.KeyRing()
		fedapi := fedAPI.NewInternalAPI(processCtx, cfg, cm, &natsInstance, &fedClient, nil, nil, keyRing, true)
		userapi := fakeUserAPI{}

		routing.Setup(routers, cfg, nil, fedapi, keyRing, &fedClient, &userapi, &cfg.MSCs, nil, caching.DisableMetrics)

		handler := fedMux.Get(routing.QueryProfileRouteName).GetHandler().ServeHTTP
		_, sk, _ := ed25519.GenerateKey(nil)
		keyID := signing.KeyID
		pk := sk.Public().(ed25519.PublicKey)
		serverName := spec.ServerName(hex.EncodeToString(pk))
		req := fclient.NewFederationRequest("GET", serverName, testOrigin, "/query/profile?user_id="+url.QueryEscape("@user:"+string(testOrigin)))
		type queryContent struct{}
		content := queryContent{}
		err := req.SetContent(content)
		if err != nil {
			t.Fatalf("Error: %s", err.Error())
		}
		req.Sign(serverName, gomatrixserverlib.KeyID(keyID), sk)
		httpReq, err := req.HTTPRequest()
		if err != nil {
			t.Fatalf("Error: %s", err.Error())
		}
		// vars := map[string]string{"room_alias": "#room:server"}
		w := httptest.NewRecorder()
		// httpReq = mux.SetURLVars(httpReq, vars)
		handler(w, httpReq)

		res := w.Result()
		data, _ := io.ReadAll(res.Body)
		println(string(data))
		assert.Equal(t, 200, res.StatusCode)
	})
}
