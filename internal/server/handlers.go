package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
)

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /v1/kv/", s.handleGet)
	s.mux.HandleFunc("PUT /v1/kv/", s.handlePut)
	s.mux.HandleFunc("DELETE /v1/kv/", s.handleDelete)

	s.mux.HandleFunc("POST /v1/backups/full", s.handleFullBackup)
	s.mux.HandleFunc("POST /v1/backups/incremental", s.handleIncremental)
	s.mux.HandleFunc("GET /v1/backups", s.handleList)
	s.mux.HandleFunc("POST /v1/backups/restore", s.handleRestore)
	s.mux.HandleFunc("POST /v1/backups/retention/apply", s.handleApplyRetention)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type kvBody struct {
	Value string `json:"value"`
}

func (s *Server) keyFromPath(r *http.Request) string {
	return r.URL.Path[len("/v1/kv/"):]
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := s.keyFromPath(r)
	if key == "" {
		writeErr(w, http.StatusBadRequest, "missing key")
		return
	}
	v, ok, err := s.eng.Get(r.Context(), key)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "key not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": v})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := s.keyFromPath(r)
	if key == "" {
		writeErr(w, http.StatusBadRequest, "missing key")
		return
	}
	var body kvBody
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json body")
			return
		}
	}
	info, err := s.eng.Put(r.Context(), key, body.Value)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, commitJSON(info.CSN, info.CommittedAt))
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := s.keyFromPath(r)
	if key == "" {
		writeErr(w, http.StatusBadRequest, "missing key")
		return
	}
	info, err := s.eng.Delete(r.Context(), key)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, commitJSON(info.CSN, info.CommittedAt))
}

func commitJSON(csn uint64, at time.Time) map[string]any {
	return map[string]any{"csn": csn, "committed_at": at.UTC().Format(time.RFC3339Nano)}
}

type fullRequest struct {
	ChainID string `json:"chain_id"`
}

func (s *Server) handleFullBackup(w http.ResponseWriter, r *http.Request) {
	var req fullRequest
	if r.ContentLength > 0 {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	m, err := s.backups.FullBackup(r.Context(), backup.FullOptions{ChainID: req.ChainID})
	if err != nil {
		s.writeBackupError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

type incrementalRequest struct {
	ChainID string `json:"chain_id"`
}

func (s *Server) handleIncremental(w http.ResponseWriter, r *http.Request) {
	var req incrementalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	m, err := s.backups.IncrementalBackup(r.Context(), req.ChainID)
	if err != nil {
		s.writeBackupError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	chains, err := s.backups.ListChains(r.Context())
	if err != nil {
		s.writeBackupError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, chains)
}

type restoreRequest struct {
	ChainID    string `json:"chain_id"`
	TargetCSN  uint64 `json:"target_csn"`
	TargetTime string `json:"target_time"` // RFC3339Nano
	DestDir    string `json:"dest_dir"`
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	var req restoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	opts := backup.RestoreOptions{
		ChainID:   req.ChainID,
		TargetCSN: req.TargetCSN,
		DestDir:   req.DestDir,
	}
	if req.TargetTime != "" {
		t, err := time.Parse(time.RFC3339Nano, req.TargetTime)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "target_time must be RFC3339Nano")
			return
		}
		opts.TargetTime = t
	}
	m, err := s.backups.RestoreTo(r.Context(), opts)
	if err != nil {
		s.writeBackupError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

func (s *Server) handleApplyRetention(w http.ResponseWriter, r *http.Request) {
	if err := s.backups.ApplyRetention(r.Context()); err != nil {
		s.writeBackupError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "applied"})
}

func (s *Server) writeBackupError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, backup.ErrCorruptBackup) {
		status = http.StatusUnprocessableEntity
	}
	writeErr(w, status, err.Error())
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
