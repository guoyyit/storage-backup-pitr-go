package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// snapshotTree returns the sorted relative paths of every entry under dir.
func snapshotTree(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(dir, path)
		if rerr != nil {
			return rerr
		}
		if rel != "." {
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func sameTree(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func restoreConflictErr(t *testing.T, err error) *TargetConflictError {
	t.Helper()
	if err == nil {
		t.Fatal("restore must be rejected")
	}
	var ce *TargetConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("want *TargetConflictError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrRestoreTargetConflict) {
		t.Fatalf("errors.Is must match ErrRestoreTargetConflict: %v", err)
	}
	return ce
}

// Restoring into the engine dir or the backup repository (including relative,
// symlink and duplicate-separator equivalents) must be rejected before any
// write; engine, chains and the target's parent must remain untouched.
func TestRestoreTargetIsolation(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, repoDir := openPair(t)
	defer eng.Close()

	for i := 1; i <= 5; i++ {
		if _, err := eng.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	full, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}

	work := t.TempDir()
	// An independent directory with pre-existing content and a symlink into
	// the engine directory; it must be left byte-for-byte entry-identical.
	holder := filepath.Join(work, "holder")
	if err := os.MkdirAll(holder, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(holder, "marker.txt")
	if err := os.WriteFile(marker, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	engineLink := filepath.Join(holder, "engine-link")
	if err := os.Symlink(engineDir, engineLink); err != nil {
		t.Fatal(err)
	}
	repoLink := filepath.Join(holder, "repo-link")
	if err := os.Symlink(repoDir, repoLink); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		dest string
		kind TargetConflictKind
	}{
		{"engine dir itself", engineDir, ConflictEngineDir},
		{"inside engine dir", filepath.Join(engineDir, "restore"), ConflictEngineDir},
		{"engine via dup separators", engineDir + string(filepath.Separator) + string(filepath.Separator) + "restore", ConflictEngineDir},
		{"engine via symlink", engineLink, ConflictEngineDir},
		{"inside engine via symlink", filepath.Join(engineLink, "restore"), ConflictEngineDir},
		{"repo itself", repoDir, ConflictBackupRepo},
		{"inside repo", filepath.Join(repoDir, full.ChainID), ConflictBackupRepo},
		{"repo via symlink", repoLink, ConflictBackupRepo},
		{"inside repo via symlink", filepath.Join(repoLink, "chain-x"), ConflictBackupRepo},
	}

	holderBefore := snapshotTree(t, holder)
	engineBefore := snapshotTree(t, engineDir)
	repoBefore := snapshotTree(t, repoDir)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: tc.dest,
			})
			ce := restoreConflictErr(t, err)
			if ce.Kind != tc.kind {
				t.Fatalf("kind=%s want %s", ce.Kind, tc.kind)
			}
			// Error must be locatable: mention the resolved destination.
			if !strings.Contains(err.Error(), tc.dest) &&
				!strings.Contains(ce.Error(), ce.Resolved) {
				t.Fatalf("error must locate the target: %v", err)
			}
			// No staging directory may appear next to any rejected target.
			staging := tc.dest + restoreStagingSuffix
			if _, err := os.Lstat(staging); !os.IsNotExist(err) {
				t.Fatalf("staging %s must not exist after rejection, err=%v", staging, err)
			}
		})
	}

	// The holder directory (including marker and symlinks) is unchanged.
	if got := snapshotTree(t, holder); !sameTree(got, holderBefore) {
		t.Fatalf("holder changed:\nbefore=%v\nafter =%v", holderBefore, got)
	}
	// Engine directory contents are unchanged.
	if got := snapshotTree(t, engineDir); !sameTree(got, engineBefore) {
		t.Fatalf("engine dir changed:\nbefore=%v\nafter =%v", engineBefore, got)
	}
	// Repository contents are unchanged.
	if got := snapshotTree(t, repoDir); !sameTree(got, repoBefore) {
		t.Fatalf("repo dir changed:\nbefore=%v\nafter =%v", repoBefore, got)
	}

	// The running engine still serves reads and writes with its original data.
	if v, ok, _ := eng.Get(ctx, "k3"); !ok || v != "v" {
		t.Fatalf("engine read after rejection: k3=%q ok=%v", v, ok)
	}
	if info, err := eng.Put(ctx, "after", "reject"); err != nil || info.CSN != 6 {
		t.Fatalf("engine write after rejection: info=%+v err=%v", info, err)
	}

	// The existing backup chain is intact and still restorable to an
	// independent destination.
	dest := filepath.Join(work, "ok", "r")
	m, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
	})
	if err != nil {
		t.Fatalf("legitimate restore after rejections: %v", err)
	}
	r, err := engine.Open(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.LastCSN() != m.PointCSN {
		t.Fatalf("restored csn=%d want %d", r.LastCSN(), m.PointCSN)
	}
}

// Relative-path equivalents are rejected too, without changing directories.
func TestRestoreRelativePathEquivalent(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, _ := openPair(t)
	defer eng.Close()
	if _, err := eng.Put(ctx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	full, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	t.Chdir(dir)
	// Relative symlink that resolves to the engine data directory.
	rel, err := filepath.Rel(dir, engineDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(rel, "eng"); err != nil {
		t.Fatal(err)
	}
	_, err = svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: "eng/./r",
	})
	ce := restoreConflictErr(t, err)
	if ce.Kind != ConflictEngineDir {
		t.Fatalf("kind=%s want engine", ce.Kind)
	}

	// A relative independent destination still works (compatibility).
	okDest := filepath.Join("out", "r")
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: okDest,
	}); err != nil {
		t.Fatalf("relative independent restore must work: %v", err)
	}
	if _, err := engine.Open(ctx, okDest); err != nil {
		t.Fatal(err)
	}
}
