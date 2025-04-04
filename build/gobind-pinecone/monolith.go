// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package gobind

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/ike20013/dendrite/federationapi/api"
	"net"
	"path/filepath"
	"strings"

	"github.com/ike20013/dendrite/clientapi/userutil"
	"github.com/ike20013/dendrite/cmd/dendrite-demo-pinecone/conduit"
	"github.com/ike20013/dendrite/cmd/dendrite-demo-pinecone/monolith"
	"github.com/ike20013/dendrite/cmd/dendrite-demo-pinecone/relay"
	"github.com/ike20013/dendrite/cmd/dendrite-demo-yggdrasil/signing"
	"github.com/ike20013/dendrite/external/httputil"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/setup/process"
	userapiAPI "github.com/ike20013/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/pinecone/types"
	"github.com/sirupsen/logrus"

	pineconeMulticast "github.com/matrix-org/pinecone/multicast"
	pineconeRouter "github.com/matrix-org/pinecone/router"

	_ "golang.org/x/mobile/bind"
)

const (
	PeerTypeRemote    = pineconeRouter.PeerTypeRemote
	PeerTypeMulticast = pineconeRouter.PeerTypeMulticast
	PeerTypeBluetooth = pineconeRouter.PeerTypeBluetooth
	PeerTypeBonjour   = pineconeRouter.PeerTypeBonjour

	MaxFrameSize = types.MaxFrameSize
)

// Re-export Conduit in this package for bindings.
type Conduit struct {
	conduit.Conduit
}

type DendriteMonolith struct {
	logger           logrus.Logger
	p2pMonolith      monolith.P2PMonolith
	StorageDirectory string
	CacheDirectory   string
	listener         net.Listener
}

func (m *DendriteMonolith) PublicKey() string {
	return m.p2pMonolith.Router.PublicKey().String()
}

func (m *DendriteMonolith) BaseURL() string {
	return fmt.Sprintf("http://%s", m.p2pMonolith.Addr())
}

func (m *DendriteMonolith) PeerCount(peertype int) int {
	return m.p2pMonolith.Router.PeerCount(peertype)
}

func (m *DendriteMonolith) SessionCount() int {
	return len(m.p2pMonolith.Sessions.Protocol(monolith.SessionProtocol).Sessions())
}

type InterfaceInfo struct {
	Name         string
	Index        int
	Mtu          int
	Up           bool
	Broadcast    bool
	Loopback     bool
	PointToPoint bool
	Multicast    bool
	Addrs        string
}

type InterfaceRetriever interface {
	CacheCurrentInterfaces() int
	GetCachedInterface(index int) *InterfaceInfo
}

func (m *DendriteMonolith) RegisterNetworkCallback(intfCallback InterfaceRetriever) {
	callback := func() []pineconeMulticast.InterfaceInfo {
		count := intfCallback.CacheCurrentInterfaces()
		intfs := []pineconeMulticast.InterfaceInfo{}
		for i := 0; i < count; i++ {
			iface := intfCallback.GetCachedInterface(i)
			if iface != nil {
				intfs = append(intfs, pineconeMulticast.InterfaceInfo{
					Name:         iface.Name,
					Index:        iface.Index,
					Mtu:          iface.Mtu,
					Up:           iface.Up,
					Broadcast:    iface.Broadcast,
					Loopback:     iface.Loopback,
					PointToPoint: iface.PointToPoint,
					Multicast:    iface.Multicast,
					Addrs:        iface.Addrs,
				})
			}
		}
		return intfs
	}
	m.p2pMonolith.Multicast.RegisterNetworkCallback(callback)
}

func (m *DendriteMonolith) SetMulticastEnabled(enabled bool) {
	if enabled {
		m.p2pMonolith.Multicast.Start()
	} else {
		m.p2pMonolith.Multicast.Stop()
		m.DisconnectType(int(pineconeRouter.PeerTypeMulticast))
	}
}

func (m *DendriteMonolith) SetStaticPeer(uri string) {
	m.p2pMonolith.ConnManager.RemovePeers()
	for _, uri := range strings.Split(uri, ",") {
		m.p2pMonolith.ConnManager.AddPeer(strings.TrimSpace(uri))
	}
}

func getServerKeyFromString(nodeID string) (spec.ServerName, error) {
	var nodeKey spec.ServerName
	if userID, err := spec.NewUserID(nodeID, false); err == nil {
		hexKey, decodeErr := hex.DecodeString(string(userID.Domain()))
		if decodeErr != nil || len(hexKey) != ed25519.PublicKeySize {
			return "", fmt.Errorf("UserID domain is not a valid ed25519 public key: %v", userID.Domain())
		} else {
			nodeKey = userID.Domain()
		}
	} else {
		hexKey, decodeErr := hex.DecodeString(nodeID)
		if decodeErr != nil || len(hexKey) != ed25519.PublicKeySize {
			return "", fmt.Errorf("Relay server uri is not a valid ed25519 public key: %v", nodeID)
		} else {
			nodeKey = spec.ServerName(nodeID)
		}
	}

	return nodeKey, nil
}

func (m *DendriteMonolith) SetRelayServers(nodeID string, uris string) {
	relays := []spec.ServerName{}
	for _, uri := range strings.Split(uris, ",") {
		uri = strings.TrimSpace(uri)
		if len(uri) == 0 {
			continue
		}

		nodeKey, err := getServerKeyFromString(uri)
		if err != nil {
			logrus.Errorf(err.Error())
			continue
		}
		relays = append(relays, nodeKey)
	}

	nodeKey, err := getServerKeyFromString(nodeID)
	if err != nil {
		logrus.Errorf(err.Error())
		return
	}

	if string(nodeKey) == m.PublicKey() {
		logrus.Infof("Setting own relay servers to: %v", relays)
		m.p2pMonolith.RelayRetriever.SetRelayServers(relays)
	} else {
		relay.UpdateNodeRelayServers(
			spec.ServerName(nodeKey),
			relays,
			m.p2pMonolith.ProcessCtx.Context(),
			m.p2pMonolith.GetFederationAPI(),
		)
	}
}

func (m *DendriteMonolith) GetRelayServers(nodeID string) string {
	nodeKey, err := getServerKeyFromString(nodeID)
	if err != nil {
		logrus.Errorf(err.Error())
		return ""
	}

	relaysString := ""
	if string(nodeKey) == m.PublicKey() {
		relays := m.p2pMonolith.RelayRetriever.GetRelayServers()

		for i, relay := range relays {
			if i != 0 {
				// Append a comma to the previous entry if there is one.
				relaysString += ","
			}
			relaysString += string(relay)
		}
	} else {
		request := api.P2PQueryRelayServersRequest{Server: spec.ServerName(nodeKey)}
		response := api.P2PQueryRelayServersResponse{}
		err := m.p2pMonolith.GetFederationAPI().P2PQueryRelayServers(m.p2pMonolith.ProcessCtx.Context(), &request, &response)
		if err != nil {
			logrus.Warnf("Failed obtaining list of this node's relay servers: %s", err.Error())
			return ""
		}

		for i, relay := range response.RelayServers {
			if i != 0 {
				// Append a comma to the previous entry if there is one.
				relaysString += ","
			}
			relaysString += string(relay)
		}
	}

	return relaysString
}

func (m *DendriteMonolith) RelayingEnabled() bool {
	return m.p2pMonolith.GetRelayAPI().RelayingEnabled()
}

func (m *DendriteMonolith) SetRelayingEnabled(enabled bool) {
	m.p2pMonolith.GetRelayAPI().SetRelayingEnabled(enabled)
}

func (m *DendriteMonolith) DisconnectType(peertype int) {
	for _, p := range m.p2pMonolith.Router.Peers() {
		if int(peertype) == p.PeerType {
			m.p2pMonolith.Router.Disconnect(types.SwitchPortID(p.Port), nil)
		}
	}
}

func (m *DendriteMonolith) DisconnectZone(zone string) {
	for _, p := range m.p2pMonolith.Router.Peers() {
		if zone == p.Zone {
			m.p2pMonolith.Router.Disconnect(types.SwitchPortID(p.Port), nil)
		}
	}
}

func (m *DendriteMonolith) DisconnectPort(port int) {
	m.p2pMonolith.Router.Disconnect(types.SwitchPortID(port), nil)
}

func (m *DendriteMonolith) Conduit(zone string, peertype int) (*Conduit, error) {
	l, r := net.Pipe()
	newConduit := Conduit{conduit.NewConduit(r, 0)}
	go func() {
		logrus.Errorf("Attempting authenticated connect")
		var port types.SwitchPortID
		var err error
		if port, err = m.p2pMonolith.Router.Connect(
			l,
			pineconeRouter.ConnectionZone(zone),
			pineconeRouter.ConnectionPeerType(peertype),
		); err != nil {
			logrus.Errorf("Authenticated connect failed: %s", err)
			_ = l.Close()
			_ = r.Close()
			_ = newConduit.Close()
			return
		}
		newConduit.SetPort(port)
		logrus.Infof("Authenticated connect succeeded (port %d)", newConduit.Port())
	}()
	return &newConduit, nil
}

func (m *DendriteMonolith) RegisterUser(localpart, password string) (string, error) {
	pubkey := m.p2pMonolith.Router.PublicKey()
	userID := userutil.MakeUserID(
		localpart,
		spec.ServerName(hex.EncodeToString(pubkey[:])),
	)
	userReq := &userapiAPI.PerformAccountCreationRequest{
		AccountType: userapiAPI.AccountTypeUser,
		Localpart:   localpart,
		Password:    password,
	}
	userRes := &userapiAPI.PerformAccountCreationResponse{}
	if err := m.p2pMonolith.GetUserAPI().PerformAccountCreation(context.Background(), userReq, userRes); err != nil {
		return userID, fmt.Errorf("userAPI.PerformAccountCreation: %w", err)
	}
	return userID, nil
}

func (m *DendriteMonolith) RegisterDevice(localpart, deviceID string) (string, error) {
	accessTokenBytes := make([]byte, 16)
	n, err := rand.Read(accessTokenBytes)
	if err != nil {
		return "", fmt.Errorf("rand.Read: %w", err)
	}
	loginReq := &userapiAPI.PerformDeviceCreationRequest{
		Localpart:   localpart,
		DeviceID:    &deviceID,
		AccessToken: hex.EncodeToString(accessTokenBytes[:n]),
	}
	loginRes := &userapiAPI.PerformDeviceCreationResponse{}
	if err := m.p2pMonolith.GetUserAPI().PerformDeviceCreation(context.Background(), loginReq, loginRes); err != nil {
		return "", fmt.Errorf("userAPI.PerformDeviceCreation: %w", err)
	}
	if !loginRes.DeviceCreated {
		return "", fmt.Errorf("device was not created")
	}
	return loginRes.Device.AccessToken, nil
}

func (m *DendriteMonolith) Start() {
	keyfile := filepath.Join(m.StorageDirectory, "p2p.pem")
	oldKeyfile := filepath.Join(m.StorageDirectory, "p2p.key")
	sk, pk := monolith.GetOrCreateKey(keyfile, oldKeyfile)

	m.logger = logrus.Logger{
		Out: BindLogger{},
	}
	m.logger.SetOutput(BindLogger{})
	logrus.SetOutput(BindLogger{})

	m.p2pMonolith = monolith.P2PMonolith{}
	m.p2pMonolith.SetupPinecone(sk)

	prefix := hex.EncodeToString(pk)
	cfg := monolith.GenerateDefaultConfig(sk, m.StorageDirectory, m.CacheDirectory, prefix)
	cfg.Global.ServerName = spec.ServerName(hex.EncodeToString(pk))
	cfg.Global.KeyID = gomatrixserverlib.KeyID(signing.KeyID)
	cfg.Global.JetStream.InMemory = false
	// NOTE : disabled for now since there is a 64 bit alignment panic on 32 bit systems
	// This isn't actually fixed: https://github.com/blevesearch/zapx/pull/147
	cfg.SyncAPI.Fulltext.Enabled = false

	processCtx := process.NewProcessContext()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	routers := httputil.NewRouters()

	enableRelaying := false
	enableMetrics := false
	enableWebsockets := false
	m.p2pMonolith.SetupDendrite(processCtx, cfg, cm, routers, 65432, enableRelaying, enableMetrics, enableWebsockets)
	m.p2pMonolith.StartMonolith()
}

func (m *DendriteMonolith) Stop() {
	m.p2pMonolith.Stop()
}
