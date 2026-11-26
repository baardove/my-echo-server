package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/baardove/my-echo/server/internal/admin"
	"github.com/baardove/my-echo/server/internal/analysis"
	"github.com/baardove/my-echo/server/internal/api"
	"github.com/baardove/my-echo/server/internal/artwork"
	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
	"github.com/baardove/my-echo/server/internal/dj"
	"github.com/baardove/my-echo/server/internal/editorial"
	"github.com/baardove/my-echo/server/internal/indexer"
	"github.com/baardove/my-echo/server/internal/lidarr"
	"github.com/baardove/my-echo/server/internal/party"
	"github.com/baardove/my-echo/server/internal/snapshot"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[my-echo-server] ")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store, err := db.Open(cfg.DBPath())
	if err != nil {
		log.Fatalf("open sqlite db: %v", err)
	}
	defer store.Close()

	artCache, err := artwork.NewCache(cfg.ArtworkDir())
	if err != nil {
		log.Fatalf("init artwork cache: %v", err)
	}

	snapBuilder, err := snapshot.NewBuilder(cfg.ShardsDir(), store)
	if err != nil {
		log.Fatalf("init snapshot builder: %v", err)
	}

	for _, lib := range cfg.Libraries {
		_ = store.UpsertLibrary(lib.ID, lib.Name, lib.Path, lib.ShardCount)
	}

	partyEngine := party.NewEngine(cfg, store, artCache)
	idx := indexer.New(cfg, store, artCache, snapBuilder)

	// Editorial fetcher (Deezer + Wikipedia) with automatic compressed bundle rebuild + .myecho share mirroring
	rebuildAndMirrorEditorial := func() {
		if _, err := snapBuilder.BuildEditorialBundle(); err != nil {
			log.Printf("editorial bundle error: %v", err)
			return
		}
		if !cfg.MirrorToShare {
			return
		}
		host := cfg.EffectiveAdvertiseHost()
		for _, lib := range cfg.Libraries {
			if libMeta, err := store.GetLibrary(lib.ID); err == nil {
				snapBuilder.MirrorToShare(lib.Path, snapshot.ShareBeacon{
					ServerName:  cfg.ServerName,
					ServerKind:  "echo-server",
					LibraryID:   lib.ID,
					LibraryName: lib.Name,
					LANURL:      fmt.Sprintf("http://%s:%d", host, cfg.APIPort),
					PartyURL:    fmt.Sprintf("http://%s:%d", host, cfg.PartyHTTPPort),
					APIPort:     cfg.APIPort,
					PartyPort:   cfg.PartyHTTPPort,
					UpdatedAt:   time.Now().UTC(),
				}, libMeta, nil)
			}
		}
	}

	edFetcher := editorial.NewFetcher(store, rebuildAndMirrorEditorial)
	lidarrMgr := lidarr.NewManager(cfg, idx, edFetcher)
	analyzer := analysis.NewAnalyzer(cfg, store)
	djEngine := dj.NewEngine(cfg, store, edFetcher)
	djEngine.SetLidarrChecker(lidarrMgr)
	djEngine.SetAnalyzer(analyzer)

	idx.SetOnChanged(func() {
		partyEngine.ReloadCatalog()
		if cfg.AutoFetchEditorial {
			edFetcher.TriggerEnrichment()
		}
		if cfg.AdvancedAnalysisEnabled {
			analyzer.TriggerRun()
		}
	})

	adminUI := admin.NewHandler(cfg, store, idx, partyEngine)
	apiServer := api.New(cfg, store, artCache, snapBuilder, idx, partyEngine, adminUI)
	apiServer.SetEditorialFetcher(edFetcher)
	apiServer.SetDJEngine(djEngine)
	apiServer.SetLidarrManager(lidarrMgr)
	apiServer.SetAnalyzer(analyzer)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.AutoFetchEditorial {
		edFetcher.StartBackgroundWorker(ctx)
	}
	djEngine.StartWorker(ctx)
	lidarrMgr.StartPoller(ctx)
	analyzer.StartWorker(ctx)

	// Initial background scan + periodic schedule
	go func() {
		if _, err := idx.ScanAll(ctx); err != nil && ctx.Err() == nil {
			log.Printf("initial scan warning: %v", err)
		}
		if cfg.ScanInterval <= 0 {
			return
		}
		ticker := time.NewTicker(cfg.ScanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = idx.ScanAll(ctx)
			}
		}
	}()

	// 1. Main API + Admin Web UI HTTP server (:48721)
	mainHTTP := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.APIPort),
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 15 * time.Second,
	}

	// 2. Party Mode Guest Web UI HTTP server (:53333)
	partyHTTP := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.PartyHTTPPort),
		Handler:           partyEngine.Handler(),
		ReadHeaderTimeout: 15 * time.Second,
	}

	// 3. Party Mode Companion HTTPS server (:53334) for mobile voice intro mic recording
	var partyHTTPS *http.Server
	if cfg.PartyHTTPSPort > 0 {
		cert, err := party.LoadOrCreateSelfSignedCert(cfg.TLSDir(), cfg.EffectiveAdvertiseHost())
		if err != nil {
			log.Printf("warning: companion Party HTTPS disabled (%v)", err)
			partyEngine.SetHTTPSPort(0)
		} else {
			partyEngine.SetHTTPSPort(cfg.PartyHTTPSPort)
			partyHTTPS = &http.Server{
				Addr:              fmt.Sprintf(":%d", cfg.PartyHTTPSPort),
				Handler:           partyEngine.Handler(),
				ReadHeaderTimeout: 15 * time.Second,
				TLSConfig: &tls.Config{
					MinVersion:   tls.VersionTLS12,
					Certificates: []tls.Certificate{cert},
				},
			}
		}
	}

	host := cfg.EffectiveAdvertiseHost()
	log.Printf("=======================================================")
	log.Printf("My Echo Server '%s' ready!", cfg.ServerName)
	log.Printf("  Admin & API : http://%s:%d/admin", host, cfg.APIPort)
	log.Printf("  Pairing PIN : %s", cfg.PairingPIN)
	log.Printf("  Party Mode  : http://%s:%d (HTTPS :%d)", host, cfg.PartyHTTPPort, cfg.PartyHTTPSPort)
	log.Printf("  Libraries   : %d configured", len(cfg.Libraries))
	log.Printf("=======================================================")

	go func() {
		if err := mainHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("main api listen failed: %v", err)
		}
	}()
	go func() {
		if err := partyHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("party http listen failed: %v", err)
		}
	}()
	if partyHTTPS != nil {
		go func() {
			if err := partyHTTPS.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				log.Printf("party https listen failed: %v", err)
			}
		}()
	}

	<-ctx.Done()
	log.Printf("Shutting down gracefully...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = mainHTTP.Shutdown(shutCtx)
	_ = partyHTTP.Shutdown(shutCtx)
	if partyHTTPS != nil {
		_ = partyHTTPS.Shutdown(shutCtx)
	}
}
