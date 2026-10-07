package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// seed writes n keys (k1..kn) and takes one full backup, returning its meta.
func seedChain(t *testing.T, ctx context.Context, eng *engine.Engine, svc *Service, n int) *Meta {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := eng.Put(ctx, key(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	m, err := svc.FullBackup(ctx, FullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// assertTargetConflict requires err to be a distinguishable restore-target
// conflict of the expected kind and relation.
func assertTargetConflict(t *testing.T, err error, kind ProtectedKind, rel PathRelation) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected restore target conflict (%s %s), got nil", rel, kind)
	}
	if !errors.Is(err, ErrRestoreTargetConflict) {
		t.Fatalf("error %v is not ErrRestoreTargetConflict", err)
	}
	var rte *RestoreTargetError
	if !errors.As(err, &rte) {
		t.Fatalf("error %T is not *RestoreTargetError", err)
	}
	if rte.Kind != kind || rte.Relation != rel {
		t.Fatalf("conflict = (%s, %s), want (%s, %s)", rte.Kind, rte.Relation, kind, rel)
	}
}

// treeMap snapshots a directory tree: relpath -> "d" or "f:<size>".
func treeMap(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if info.IsDir() {
			out[rel] = "d"
		} else {
			out[rel] = fmt.Sprintf("f:%d", info.Size())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func assertTreeEqual(t *testing.T, before map[string]string, root string) {
	t.Helper()
	after := treeMap(t, root)
	if len(before) != len(after) {
		t.Fatalf("tree size changed: before=%d after=%d\nbefore=%v\nafter=%v",
			len(before), len(after), before, after)
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("tree entry %q changed: %q -> %q", k, v, after[k])
		}
	}
}

// assertPathUntouched verifies a rejected destination produced no new,
// deleted or renamed content at or around the destination path.
func assertPathUntouched(t *testing.T, dest string) {
	t.Helper()
	for _, p := range []string{dest, dest + restoreStagingSuffix, dest + ".old"} {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Fatalf("rejected restore left trace at %q (err=%v)", p, err)
		}
	}
}

func TestRestoreTargetRejectsEngineDir(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, _ := openPair(t)
	defer eng.Close()
	full := seedChain(t, ctx, eng, svc, 5)

	cases := []struct {
		name string
		dest string
		rel  PathRelation
	}{
		{"engine-dir-itself", engineDir, RelationIdentical},
		{"engine-wal-subdir", filepath.Join(engineDir, "wal"), RelationInside},
		{"engine-new-subdir", filepath.Join(engineDir, "restore-out"), RelationInside},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: tc.dest,
			})
			assertTargetConflict(t, err, ProtectedEngineDir, tc.rel)
		})
	}
}

func TestRestoreTargetRejectsBackupRepo(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()
	full := seedChain(t, ctx, eng, svc, 5)

	cases := []struct {
		name string
		dest string
		rel  PathRelation
	}{
		{"repo-itself", repo, RelationIdentical},
		{"repo-chain-dir", filepath.Join(repo, full.ChainID), RelationInside},
		{"repo-new-subdir", filepath.Join(repo, "restore-out"), RelationInside},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: tc.dest,
			})
			assertTargetConflict(t, err, ProtectedBackupRepo, tc.rel)
		})
	}
}

// A destination that contains a protected directory would move or delete the
// protected data when atomically replaced, so ancestors are rejected too.
// Each case uses a layout where the destination is an ancestor of exactly one
// protected directory.
func TestRestoreTargetRejectsAncestors(t *testing.T) {
	ctx := context.Background()

	t.Run("engine-ancestor", func(t *testing.T) {
		base := t.TempDir()
		engineDir := filepath.Join(base, "data", "engine")
		eng, err := openEngine(ctx, engineDir)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		svc, err := NewService(ctx, eng, Config{Dir: filepath.Join(t.TempDir(), "repo")})
		if err != nil {
			t.Fatal(err)
		}
		full := seedChain(t, ctx, eng, svc, 3)
		for _, dest := range []string{filepath.Join(base, "data"), base} {
			_, err := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
			})
			assertTargetConflict(t, err, ProtectedEngineDir, RelationAncestor)
		}
	})

	t.Run("repo-ancestor", func(t *testing.T) {
		base := t.TempDir()
		eng, err := openEngine(ctx, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		svc, err := NewService(ctx, eng, Config{Dir: filepath.Join(base, "backups", "repo")})
		if err != nil {
			t.Fatal(err)
		}
		full := seedChain(t, ctx, eng, svc, 3)
		for _, dest := range []string{filepath.Join(base, "backups"), base} {
			_, err := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
			})
			assertTargetConflict(t, err, ProtectedBackupRepo, RelationAncestor)
		}
	})
}

func TestRestoreTargetRejectsPathEquivalents(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, repo := openPair(t)
	defer eng.Close()
	full := seedChain(t, ctx, eng, svc, 5)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relEngine, err := filepath.Rel(cwd, engineDir)
	if err != nil {
		t.Skipf("cannot build relative path: %v", err)
	}
	relRepo, err := filepath.Rel(cwd, repo)
	if err != nil {
		t.Skipf("cannot build relative path: %v", err)
	}

	// Symlinks aliasing the protected directories.
	linkRoot := t.TempDir()
	engineLink := filepath.Join(linkRoot, "engine-link")
	if err := os.Symlink(engineDir, engineLink); err != nil {
		t.Fatal(err)
	}
	repoLink := filepath.Join(linkRoot, "repo-link")
	if err := os.Symlink(repo, repoLink); err != nil {
		t.Fatal(err)
	}
	// A symlink one level up: link/sub resolves inside the engine dir.
	parentLink := filepath.Join(linkRoot, "parent-link")
	if err := os.Symlink(filepath.Dir(engineDir), parentLink); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		dest string
		kind ProtectedKind
		rel  PathRelation
	}{
		{"relative-engine-dir", relEngine, ProtectedEngineDir, RelationIdentical},
		{"relative-engine-subdir", relEngine + string(os.PathSeparator) + "out", ProtectedEngineDir, RelationInside},
		{"relative-repo", relRepo, ProtectedBackupRepo, RelationIdentical},
		{"dotdot-into-engine", filepath.Join(engineDir, "wal", ".."), ProtectedEngineDir, RelationIdentical},
		{"dotdot-into-engine-wal", filepath.Join(engineDir, "sub", "..", "wal"), ProtectedEngineDir, RelationInside},
		{"dup-separators-engine", engineDir + string(os.PathSeparator) + string(os.PathSeparator) + "wal", ProtectedEngineDir, RelationInside},
		{"dup-separators-repo", repo + string(os.PathSeparator) + string(os.PathSeparator), ProtectedBackupRepo, RelationIdentical},
		{"symlink-to-engine", engineLink, ProtectedEngineDir, RelationIdentical},
		{"symlink-into-engine-subdir", filepath.Join(engineLink, "wal"), ProtectedEngineDir, RelationInside},
		{"symlink-to-repo", repoLink, ProtectedBackupRepo, RelationIdentical},
		{"symlink-parent-into-engine", filepath.Join(parentLink, filepath.Base(engineDir)), ProtectedEngineDir, RelationIdentical},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: tc.dest,
			})
			assertTargetConflict(t, err, tc.kind, tc.rel)
		})
	}
}

// After any rejection the engine must keep serving its original data, every
// existing chain must stay restorable, and the destination path must be
// untouched.
func TestRestoreTargetRejectionLeavesEngineAndChainsIntact(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, repo := openPair(t)
	defer eng.Close()
	full := seedChain(t, ctx, eng, svc, 6)

	engineBefore := treeMap(t, engineDir)
	repoBefore := treeMap(t, repo)

	// A symlink aliasing the engine directory.
	linkRoot := t.TempDir()
	link := filepath.Join(linkRoot, "alias")
	if err := os.Symlink(engineDir, link); err != nil {
		t.Fatal(err)
	}

	// Existing protected destinations: content trees must stay identical.
	existing := []string{engineDir, filepath.Join(engineDir, "wal"), repo, link}
	// Non-existent destinations inside protected dirs: must not be created.
	fresh := []string{
		filepath.Join(engineDir, "no-such-subdir"),
		filepath.Join(repo, "no-such-subdir"),
		filepath.Join(link, "via-symlink"),
	}
	for _, dest := range append(existing, fresh...) {
		if _, err := svc.RestoreTo(ctx, RestoreOptions{
			ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
		}); !errors.Is(err, ErrRestoreTargetConflict) {
			t.Fatalf("dest %q: want ErrRestoreTargetConflict, got %v", dest, err)
		}
	}

	// No new, deleted or renamed content anywhere protected.
	assertTreeEqual(t, engineBefore, engineDir)
	assertTreeEqual(t, repoBefore, repo)
	if target, err := os.Readlink(link); err != nil || target != engineDir {
		t.Fatalf("symlink modified: target=%q err=%v", target, err)
	}
	for _, dest := range fresh {
		assertPathUntouched(t, dest)
	}

	// Engine still reads/writes with its original data.
	if v, ok, err := eng.Get(ctx, key(3)); err != nil || !ok || v != "v3" {
		t.Fatalf("engine get after rejection: v=%q ok=%v err=%v", v, ok, err)
	}
	info, err := eng.Put(ctx, "post-reject", "ok")
	if err != nil {
		t.Fatalf("engine write after rejection: %v", err)
	}
	if info.CSN != 7 {
		t.Fatalf("CSN after rejection = %d, want 7", info.CSN)
	}

	// The engine restarts cleanly from the untouched original data.
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openEngine(ctx, engineDir)
	if err != nil {
		t.Fatalf("engine restart after rejection: %v", err)
	}
	defer reopened.Close()
	if v, ok, _ := reopened.Get(ctx, "post-reject"); !ok || v != "ok" {
		t.Fatalf("restarted engine lost data: v=%q ok=%v", v, ok)
	}
	if v, ok, _ := reopened.Get(ctx, key(6)); !ok || v != "v6" {
		t.Fatalf("restarted engine lost base data: v=%q ok=%v", v, ok)
	}

	// Every existing chain still restores to an independent destination.
	chains, err := svc.ListChains(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(chains) != 1 {
		t.Fatalf("chains=%d want 1", len(chains))
	}
	dest := filepath.Join(t.TempDir(), "restore")
	m, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: chains[0].ID, TargetCSN: full.PointCSN, DestDir: dest,
	})
	if err != nil {
		t.Fatalf("chain restore after rejections: %v", err)
	}
	if m.PointCSN != full.PointCSN {
		t.Fatalf("restored CSN=%d want %d", m.PointCSN, full.PointCSN)
	}
}

// Legitimate independent destinations keep working: both a fresh directory
// and an existing independent directory.
func TestRestoreTargetIndependentDirsStillWork(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, repo := openPair(t)
	defer eng.Close()
	full := seedChain(t, ctx, eng, svc, 4)

	// Fresh directory that does not exist yet.
	fresh := filepath.Join(t.TempDir(), "new-dir")
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: fresh,
	}); err != nil {
		t.Fatalf("restore to fresh dir: %v", err)
	}

	// Sibling directories sharing a name prefix with protected dirs are NOT
	// inside them and must not be rejected.
	for _, sibling := range []string{repo + "-sibling", engineDir + "-sibling"} {
		dest := filepath.Join(sibling, "restore")
		if _, err := svc.RestoreTo(ctx, RestoreOptions{
			ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
		}); err != nil {
			t.Fatalf("restore to prefix-sibling %q: %v", dest, err)
		}
	}

	// Existing independent directory with prior content gets replaced.
	existing := t.TempDir()
	if err := os.WriteFile(filepath.Join(existing, "stale.txt"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: existing,
	}); err != nil {
		t.Fatalf("restore to existing dir: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(existing, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale content survived restore: err=%v", err)
	}
	r, err := openEngine(ctx, existing)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.LastCSN() != full.PointCSN {
		t.Fatalf("restored CSN=%d want %d", r.LastCSN(), full.PointCSN)
	}
	if v, ok, _ := r.Get(ctx, key(2)); !ok || v != "v2" {
		t.Fatalf("restored k2=%q ok=%v", v, ok)
	}
}

// Error messages must locate the conflict precisely.
func TestRestoreTargetErrorMessage(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, _ := openPair(t)
	defer eng.Close()
	full := seedChain(t, ctx, eng, svc, 2)

	_, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN,
		DestDir: filepath.Join(engineDir, "wal"),
	})
	if err == nil {
		t.Fatal("expected conflict")
	}
	msg := err.Error()
	for _, want := range []string{string(ProtectedEngineDir), string(RelationInside), "wal"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q missing %q", msg, want)
		}
	}
}
