package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

type server struct {
	eng *engine.Engine
	mgr *backup.Manager
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	dataDir := flag.String("data", "data/engine", "engine data directory")
	backupDir := flag.String("backup", "data/backup", "backup storage directory")
	syncWrites := flag.Bool("sync-writes", true, "fsync every commit")
	snapshotThreshold := flag.Int("snapshot-threshold", 10000, "commits between engine snapshots")
	flag.Parse()

	eng, err := engine.Open(engine.Options{
		Dir:               *dataDir,
		SyncWrites:        *syncWrites,
		SnapshotThreshold: *snapshotThreshold,
	})
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	defer eng.Close()
	mgr, err := backup.NewManager(eng, *backupDir)
	if err != nil {
		log.Fatalf("open backup manager: %v", err)
	}
	s := &server{eng: eng, mgr: mgr}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.getKV)
	mux.HandleFunc("PUT /kv/{key}", s.putKV)
	mux.HandleFunc("DELETE /kv/{key}", s.deleteKV)
	mux.HandleFunc("POST /backup/full", s.fullBackup)
	mux.HandleFunc("POST /backup/incr", s.incrBackup)
	mux.HandleFunc("GET /chains", s.listChains)
	mux.HandleFunc("POST /restore", s.restore)
	mux.HandleFunc("POST /validate", s.validate)
	mux.HandleFunc("POST /retention", s.retention)
	log.Printf("kvserver listening on %s (data=%s backup=%s)", *addr, *dataDir, *backupDir)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	var ce *backup.CorruptionError
	switch {
	case errors.As(err, &ce):
		code = http.StatusUnprocessableEntity
	case errors.Is(err, backup.ErrChainNotFound):
		code = http.StatusNotFound
	case errors.Is(err, backup.ErrNoNewData):
		code = http.StatusConflict
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *server) getKV(w http.ResponseWriter, r *http.Request) {
	v, ok, err := s.eng.Get(r.PathValue("key"))
	if err != nil {
		writeErr(w, err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(v)
}

func (s *server) putKV(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		writeErr(w, err)
		return
	}
	seq, err := s.eng.Put(r.PathValue("key"), body)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]uint64{"seq": seq})
}

func (s *server) deleteKV(w http.ResponseWriter, r *http.Request) {
	seq, err := s.eng.Delete(r.PathValue("key"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]uint64{"seq": seq})
}

func (s *server) fullBackup(w http.ResponseWriter, r *http.Request) {
	chainID, err := s.mgr.FullBackup()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"chain": chainID})
}

func (s *server) incrBackup(w http.ResponseWriter, r *http.Request) {
	meta, err := s.mgr.IncrementalBackup(r.URL.Query().Get("chain"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, meta)
}

func (s *server) listChains(w http.ResponseWriter, r *http.Request) {
	chains, err := s.mgr.Chains()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, chains)
}

type restoreRequest struct {
	TargetDir   string `json:"target_dir"`
	ChainID     string `json:"chain_id"`
	PointInTime string `json:"point_in_time"` // RFC3339，可选
	ToSeq       uint64 `json:"to_seq"`        // 可选
}

func (s *server) restore(w http.ResponseWriter, r *http.Request) {
	var req restoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	opts := backup.RestoreOptions{
		TargetDir: req.TargetDir,
		ChainID:   req.ChainID,
		ToSeq:     req.ToSeq,
	}
	if req.PointInTime != "" {
		t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(req.PointInTime))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad point_in_time: " + err.Error()})
			return
		}
		opts.PointInTime = t
	}
	if err := s.mgr.Restore(opts); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "restored", "target_dir": req.TargetDir})
}

func (s *server) validate(w http.ResponseWriter, r *http.Request) {
	if err := s.mgr.ValidateChain(r.URL.Query().Get("chain")); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type retentionRequest struct {
	RetainCount int   `json:"retain_count"`
	RetainAge   int64 `json:"retain_age_sec"`
}

func (s *server) retention(w http.ResponseWriter, r *http.Request) {
	var req retentionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	deleted, err := s.mgr.ApplyRetention(backup.RetentionPolicy{
		RetainCount: req.RetainCount,
		RetainAge:   time.Duration(req.RetainAge) * time.Second,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}
