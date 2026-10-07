package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

func setup(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), "eng")
	repoDir := filepath.Join(t.TempDir(), "repo")
	eng, err := engine.Open(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := backup.NewService(ctx, eng, backup.Config{Dir: repoDir})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{Addr: ":0"}, eng, svc)
	ts := httptest.NewServer(srv.Handler())
	return ts, func() {
		ts.Close()
		eng.Close()
	}
}

func doJSON(t *testing.T, method, url string, body any, into any) int {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if into != nil {
		_ = json.NewDecoder(resp.Body).Decode(into)
	}
	return resp.StatusCode
}

func TestHTTPKVSAndBackupRestore(t *testing.T) {
	ts, cleanup := setup(t)
	defer cleanup()

	if code := doJSON(t, http.MethodPut, ts.URL+"/v1/kv/a", map[string]string{"value": "1"}, nil); code != 200 {
		t.Fatalf("put status=%d", code)
	}
	if code := doJSON(t, http.MethodPut, ts.URL+"/v1/kv/b", map[string]string{"value": "2"}, nil); code != 200 {
		t.Fatalf("put status=%d", code)
	}

	var full backup.Meta
	if code := doJSON(t, http.MethodPost, ts.URL+"/v1/backups/full", map[string]string{}, &full); code != 201 {
		t.Fatalf("full status=%d", code)
	}
	if full.PointCSN != 2 {
		t.Fatalf("full point=%d want 2", full.PointCSN)
	}

	if code := doJSON(t, http.MethodPut, ts.URL+"/v1/kv/c", map[string]string{"value": "3"}, nil); code != 200 {
		t.Fatalf("put c status=%d", code)
	}
	var inc backup.Meta
	if code := doJSON(t, http.MethodPost, ts.URL+"/v1/backups/incremental",
		map[string]string{"chain_id": full.ChainID}, &inc); code != 201 {
		t.Fatalf("incremental status=%d", code)
	}

	chainsResp, err := http.Get(ts.URL + "/v1/backups")
	if err != nil {
		t.Fatal(err)
	}
	var chains []backup.Chain
	_ = json.NewDecoder(chainsResp.Body).Decode(&chains)
	chainsResp.Body.Close()
	if len(chains) != 1 || len(chains[0].Increments) != 1 {
		t.Fatalf("chains=%+v", chains)
	}

	dest := filepath.Join(t.TempDir(), "restored")
	var restored backup.Meta
	code := doJSON(t, http.MethodPost, ts.URL+"/v1/backups/restore", map[string]any{
		"target_csn": full.PointCSN,
		"dest_dir":   dest,
	}, &restored)
	if code != 201 {
		t.Fatalf("restore status=%d", code)
	}

	// Verify restored data independently.
	r, err := engine.Open(context.Background(), dest)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := r.Get(context.Background(), "c"); ok {
		t.Fatal("c must not exist when restoring at CSN 2")
	}
	if v, ok, _ := r.Get(context.Background(), "a"); !ok || v != "1" {
		t.Fatalf("a=%q ok=%v", v, ok)
	}
	r.Close()
}

// Restore-target conflicts and missing chains surface as distinguishable
// HTTP statuses: 400 for a protected destination, 404 for a reclaimed chain.
func TestHTTPRestoreTargetRejection(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), "eng")
	repoDir := filepath.Join(t.TempDir(), "repo")
	eng, err := engine.Open(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	svc, err := backup.NewService(ctx, eng, backup.Config{Dir: repoDir})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Config{Addr: ":0"}, eng, svc)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if code := doJSON(t, http.MethodPut, ts.URL+"/v1/kv/a", map[string]string{"value": "1"}, nil); code != 200 {
		t.Fatalf("put status=%d", code)
	}
	var full backup.Meta
	if code := doJSON(t, http.MethodPost, ts.URL+"/v1/backups/full", map[string]string{}, &full); code != 201 {
		t.Fatalf("full status=%d", code)
	}

	var errBody map[string]string
	// Engine data directory itself and inside it: 400 naming the conflict.
	for _, dest := range []string{dataDir, filepath.Join(dataDir, "wal")} {
		code := doJSON(t, http.MethodPost, ts.URL+"/v1/backups/restore", map[string]any{
			"chain_id": full.ChainID, "target_csn": full.PointCSN, "dest_dir": dest,
		}, &errBody)
		if code != http.StatusBadRequest {
			t.Fatalf("restore to %q status=%d want 400 (%v)", dest, code, errBody)
		}
		if !strings.Contains(errBody["error"], "engine-data-dir") {
			t.Fatalf("error %q does not name engine-data-dir", errBody["error"])
		}
	}
	// Inside the backup repository: 400 naming the repository.
	code := doJSON(t, http.MethodPost, ts.URL+"/v1/backups/restore", map[string]any{
		"chain_id": full.ChainID, "target_csn": full.PointCSN,
		"dest_dir": filepath.Join(repoDir, "sub"),
	}, &errBody)
	if code != http.StatusBadRequest || !strings.Contains(errBody["error"], "backup-repository") {
		t.Fatalf("repo restore status=%d err=%v", code, errBody)
	}
	// A chain that does not exist: 404.
	code = doJSON(t, http.MethodPost, ts.URL+"/v1/backups/restore", map[string]any{
		"chain_id": "chain-missing", "target_csn": 1,
		"dest_dir": filepath.Join(t.TempDir(), "out"),
	}, &errBody)
	if code != http.StatusNotFound {
		t.Fatalf("missing chain status=%d want 404", code)
	}
	// A legitimate independent destination still works: 201.
	code = doJSON(t, http.MethodPost, ts.URL+"/v1/backups/restore", map[string]any{
		"chain_id": full.ChainID, "target_csn": full.PointCSN,
		"dest_dir": filepath.Join(t.TempDir(), "ok"),
	}, &full)
	if code != http.StatusCreated {
		t.Fatalf("legit restore status=%d want 201", code)
	}
	// Engine is untouched and still serves writes.
	if code := doJSON(t, http.MethodPut, ts.URL+"/v1/kv/b", map[string]string{"value": "2"}, nil); code != 200 {
		t.Fatalf("put after rejections status=%d", code)
	}
}
