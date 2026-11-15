package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/yoohooshantanu/dialpulse/internal/dnc"
	"github.com/yoohooshantanu/dialpulse/internal/logger"
	"github.com/yoohooshantanu/dialpulse/internal/metrics"
	"github.com/yoohooshantanu/dialpulse/internal/model"
	"github.com/yoohooshantanu/dialpulse/internal/queue"
	"github.com/yoohooshantanu/dialpulse/internal/storage"
	"github.com/yoohooshantanu/dialpulse/internal/timezone"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

type ServerState struct {
	mu        sync.RWMutex
	campaigns map[string]*queue.CampaignEngine
	dncFilter *dnc.BloomFilter
	tzGov     *timezone.TimezoneGovernor
	repo      storage.CampaignRepository
}

func main() {
	port := getEnv("PORT", "8086")
	logger.L.Info("starting_dialpulse_engine", "port", port)

	// Connect to PostgreSQL persistence layer if DATABASE_URL is configured
	databaseURL := os.Getenv("DATABASE_URL")
	var repo storage.CampaignRepository
	if databaseURL != "" {
		pgRepo, err := storage.NewPostgresRepositoryFromURL(context.Background(), databaseURL)
		if err != nil {
			logger.L.Error("postgres_connection_failed", "database_url", databaseURL, "error", err)
			os.Exit(1)
		}
		repo = pgRepo
		logger.L.Info("connected_to_postgres_persistence")
	} else {
		logger.L.Info("no_database_url_provided_using_memory_repository")
		repo = storage.NewMemoryPostgresRepository()
	}
	defer repo.Close()

	state := &ServerState{
		campaigns: make(map[string]*queue.CampaignEngine),
		dncFilter: dnc.NewBloomFilter(1000000, 0.001),
		tzGov:     timezone.NewTimezoneGovernor(),
		repo:      repo,
	}

	mux := http.NewServeMux()

	// 1. Create Campaign
	mux.HandleFunc("/v1/campaigns", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req model.Campaign
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}

		if req.ID == "" {
			req.ID = "camp_" + uuid.NewString()[:8]
		}
		if req.AllocatedAgents <= 0 {
			req.AllocatedAgents = 10
		}
		req.Status = model.CampaignDraft
		req.CreatedAt = time.Now().UTC()

		if err := state.repo.SaveCampaign(r.Context(), &req); err != nil {
			logger.L.Error("failed_saving_campaign", "error", err)
			http.Error(w, `{"error":"failed to persist campaign"}`, http.StatusInternalServerError)
			return
		}

		engine := queue.NewCampaignEngine(&req, state.dncFilter, state.tzGov, nil)
		engine.SetRepository(state.repo)

		state.mu.Lock()
		state.campaigns[req.ID] = engine
		state.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(req)
	})

	// 2. Ingest Leads & Campaign Control
	mux.HandleFunc("/v1/campaigns/", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 5<<20) // 5MB batch ceiling
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 {
			http.Error(w, `{"error":"invalid path"}`, http.StatusBadRequest)
			return
		}

		campaignID := parts[2]
		state.mu.RLock()
		engine, ok := state.campaigns[campaignID]
		state.mu.RUnlock()

		if !ok {
			http.Error(w, `{"error":"campaign not found"}`, http.StatusNotFound)
			return
		}

		action := ""
		if len(parts) >= 4 {
			action = parts[3]
		}

		switch action {
		case "leads":
			if r.Method == http.MethodGet && len(parts) >= 5 {
				leadID := parts[4]
				lead, ok := engine.GetLead(leadID)
				if !ok {
					http.Error(w, `{"error":"lead not found"}`, http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(lead)
				return
			}

			if r.Method != http.MethodPost {
				http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
				return
			}
			var leads []*model.Lead
			if err := json.NewDecoder(r.Body).Decode(&leads); err != nil {
				http.Error(w, `{"error":"invalid JSON leads array"}`, http.StatusBadRequest)
				return
			}
			count, duplicates, err := engine.IngestLeads(r.Context(), leads)
			if err != nil {
				logger.L.Error("failed_ingesting_leads", "campaign_id", campaignID, "error", err)
				http.Error(w, `{"error":"failed to ingest leads"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"campaign_id": campaignID,
				"ingested":    count,
				"duplicates":  duplicates,
			})

		case "dlq":
			dlqLeads := engine.GetDLQ()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"campaign_id": campaignID,
				"dlq_count":   len(dlqLeads),
				"leads":       dlqLeads,
			})

		case "start":
			if r.Method != http.MethodPost {
				http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
				return
			}
			engine.Start()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "running"})

		case "stats":
			stats := engine.GetStats()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(stats)

		default:
			http.Error(w, `{"error":"unknown action"}`, http.StatusBadRequest)
		}
	})

	// 3. Do-Not-Call (DNC) Registry Ingestion
	mux.HandleFunc("/v1/dnc", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			PhoneNumbers []string `json:"phone_numbers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}

		for _, p := range req.PhoneNumbers {
			state.dncFilter.Add(p)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"added":     len(req.PhoneNumbers),
			"total_dnc": state.dncFilter.Count(),
		})
	})

	// 4. Telemetry & Health Checks
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/metrics", metrics.DefaultMetrics.Handler())

	// pprof
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.L.Info("dialpulse_listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Error("server_listen_failed", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.L.Info("shutting_down_dialpulse")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = server.Shutdown(ctx)
	state.mu.Lock()
	for _, engine := range state.campaigns {
		engine.Stop()
	}
	state.mu.Unlock()
	logger.L.Info("dialpulse_terminated_cleanly")
}
