// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package main

import (
	"github.com/ike20013/dendrite/setup/process"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/ike20013/dendrite/external"
	"github.com/ike20013/dendrite/external/caching"
	"github.com/ike20013/dendrite/external/httputil"
	"github.com/ike20013/dendrite/external/sqlutil"
	"github.com/ike20013/dendrite/setup/jetstream"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/ike20013/dendrite/appservice"
	"github.com/ike20013/dendrite/federationapi"
	"github.com/ike20013/dendrite/roomserver"
	"github.com/ike20013/dendrite/setup"
	basepkg "github.com/ike20013/dendrite/setup/base"
	"github.com/ike20013/dendrite/setup/config"
	"github.com/ike20013/dendrite/setup/mscs"
	"github.com/ike20013/dendrite/userapi"
)

var _, skip = os.LookupEnv("CI")

func main() {
	cfg := setup.ParseFlags(true)
	if skip {
		return
	}
	configErrors := &config.ConfigErrors{}
	cfg.Verify(configErrors)
	if len(*configErrors) > 0 {
		for _, err := range *configErrors {
			logrus.Errorf("Configuration error: %s", err)
		}
		logrus.Fatalf("Failed to start due to configuration errors")
	}
	processCtx := process.NewProcessContext()

	external.SetupStdLogging()
	external.SetupHookLogging(cfg.Logging)
	external.SetupPprof()

	basepkg.PlatformSanityChecks()

	logrus.Infof("Dendrite version %s", external.VersionString())
	if !cfg.ClientAPI.RegistrationDisabled && cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled {
		logrus.Warn("Open registration is enabled")
	}

	// create DNS cache
	var dnsCache *fclient.DNSCache
	if cfg.Global.DNSCache.Enabled {
		dnsCache = fclient.NewDNSCache(
			cfg.Global.DNSCache.CacheSize,
			cfg.Global.DNSCache.CacheLifetime,
			cfg.FederationAPI.AllowNetworkCIDRs,
			cfg.FederationAPI.DenyNetworkCIDRs,
		)
		logrus.Infof(
			"DNS cache enabled (size %d, lifetime %s)",
			cfg.Global.DNSCache.CacheSize,
			cfg.Global.DNSCache.CacheLifetime,
		)
	}

	// setup tracing
	closer, err := cfg.SetupTracing()
	if err != nil {
		logrus.WithError(err).Panicf("failed to start opentracing")
	}
	defer closer.Close() // nolint: errcheck

	// setup sentry
	if cfg.Global.Sentry.Enabled {
		logrus.Info("Setting up Sentry for debugging...")
		err = sentry.Init(sentry.ClientOptions{
			Dsn:              cfg.Global.Sentry.DSN,
			Environment:      cfg.Global.Sentry.Environment,
			Debug:            true,
			ServerName:       string(cfg.Global.ServerName),
			Release:          "dendrite@" + external.VersionString(),
			AttachStacktrace: true,
		})
		if err != nil {
			logrus.WithError(err).Panic("failed to start Sentry")
		}
		go func() {
			processCtx.ComponentStarted()
			<-processCtx.WaitForShutdown()
			if !sentry.Flush(time.Second * 5) {
				logrus.Warnf("failed to flush all Sentry events!")
			}
			processCtx.ComponentFinished()
		}()
	}

	federationClient := basepkg.CreateFederationClient(cfg, dnsCache)
	httpClient := basepkg.CreateClient(cfg, dnsCache)

	// prepare required dependencies
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	routers := httputil.NewRouters()

	caches := caching.NewRistrettoCache(cfg.Global.Cache.EstimatedMaxSize, cfg.Global.Cache.MaxAge, caching.EnableMetrics)
	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.EnableMetrics)
	fsAPI := federationapi.NewInternalAPI(
		processCtx, cfg, cm, &natsInstance, federationClient, rsAPI, caches, nil, false,
	)

	keyRing := fsAPI.KeyRing()

	// The underlying roomserver implementation needs to be able to call the fedsender.
	// This is different to rsAPI which can be the http client which doesn't need this
	// dependency. Other components also need updating after their dependencies are up.
	rsAPI.SetFederationAPI(fsAPI, keyRing)

	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, federationClient, caching.EnableMetrics, fsAPI.IsBlacklistedOrBackingOff)
	asAPI := appservice.NewInternalAPI(processCtx, cfg, &natsInstance, userAPI, rsAPI)

	rsAPI.SetAppserviceAPI(asAPI)
	rsAPI.SetUserAPI(userAPI)

	monolith := setup.Monolith{
		Config:    cfg,
		Client:    httpClient,
		FedClient: federationClient,
		KeyRing:   keyRing,

		AppserviceAPI: asAPI,
		// always use the concrete impl here even in -http mode because adding public routes
		// must be done on the concrete impl not an HTTP client else fedapi will call itself
		FederationAPI: fsAPI,
		RoomserverAPI: rsAPI,
		UserAPI:       userAPI,
	}
	monolith.AddAllPublicRoutes(processCtx, cfg, routers, cm, &natsInstance, caches, caching.EnableMetrics)

	if len(cfg.MSCs.MSCs) > 0 {
		if err := mscs.Enable(cfg, cm, routers, &monolith, caches); err != nil {
			logrus.WithError(err).Fatalf("Failed to enable MSCs")
		}
	}

	upCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "dendrite",
		Name:      "up",
		ConstLabels: map[string]string{
			"version": external.VersionString(),
		},
	})
	upCounter.Add(1)
	prometheus.MustRegister(upCounter)

	// Expose the matrix APIs directly rather than putting them under a /api path.
	go func() {
		SetupAndServeHTTPS(processCtx, cfg, routers) //, httpsAddr, nil, nil)
	}()

	// We want to block forever to let the HTTP and HTTPS handler serve the APIs
	basepkg.WaitForShutdown(processCtx)
}
