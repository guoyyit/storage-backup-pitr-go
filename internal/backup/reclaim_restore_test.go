package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// verifyRestoredState opens a restored directory and checks it contains
// exactly the deterministic state at target CSN: keys k1..k<csn> with values
// v1..v<csn>, and nothing beyond.
func verifyRestoredState(t *testing.T, ctx context.Context, dir string, csn uint64, total uint64) {
	t.Helper()
	r, err := openEngine(ctx, dir)
	if err != nil {
		t.Fatalf("open restored dir %s: %v", dir, err)
	}
	defer r.Close()
	if r.LastCSN() != csn {
		t.Fatalf("restored LastCSN=%d want %d", r.LastCSN(), csn)
	}
	for i := uint64(1); i <= csn; i++ {
		v, ok, err := r.Get(ctx, key(int(i)))
		if err != nil || !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("restored key %d = %q ok=%v err=%v, want v%d", i, v, ok, err, i)
		}
	}
	for i := csn + 1; i <= total; i++ {
		if v, ok, _ := r.Get(ctx, key(int(i))); ok {
			t.Fatalf("restored key %d = %q must not exist beyond target CSN %d", i, v, csn)
		}
	}
}

// A chain reclaimed before the restore starts fails with a distinguishable
// error before anything is written to the destination.
func TestRestoreAfterReclaimFailsBeforeWrites(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()

	for i := 1; i <= 4; i++ {
		if _, err := eng.Put(ctx, key(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	victim, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-victim"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 5; i <= 8; i++ {
		if _, err := eng.Put(ctx, key(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-keeper"}); err != nil {
		t.Fatal(err)
	}

	svc.cfg.Retention = Retention{Count: 1}
	if err := svc.ApplyRetention(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(repo, victim.ChainID)); !os.IsNotExist(err) {
		t.Fatalf("victim chain should be reclaimed, stat err=%v", err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	_, err = svc.RestoreTo(ctx, RestoreOptions{
		ChainID: victim.ChainID, TargetCSN: victim.PointCSN, DestDir: dest,
	})
	if !errors.Is(err, ErrChainNotFound) {
		t.Fatalf("restore of reclaimed chain: want ErrChainNotFound, got %v", err)
	}
	assertPathUntouched(t, dest)

	// The retained chain still restores exactly.
	keeperDest := filepath.Join(t.TempDir(), "keeper")
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: "chain-keeper", TargetCSN: 8, DestDir: keeperDest,
	}); err != nil {
		t.Fatalf("retained chain restore: %v", err)
	}
	verifyRestoredState(t, ctx, keeperDest, 8, 8)
}

// Retention must not delete a chain while a restore holds it: deletion waits
// for the chain lock, then proceeds.
func TestReclaimWaitsForInFlightChain(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()

	for i := 1; i <= 3; i++ {
		if _, err := eng.Put(ctx, key(i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-victim"}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Put(ctx, "k9", "v9"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-keeper"}); err != nil {
		t.Fatal(err)
	}
	svc.cfg.Retention = Retention{Count: 1}

	// Hold the victim chain lock as an in-flight restore would.
	mu := chainLock("chain-victim")
	mu.Lock()

	done := make(chan error, 1)
	go func() { done <- svc.ApplyRetention(ctx) }()

	select {
	case err := <-done:
		mu.Unlock()
		t.Fatalf("retention finished while chain lock held: %v", err)
	case <-time.After(150 * time.Millisecond):
		// Still blocked: the chain cannot be reclaimed mid-restore.
		if _, err := os.Stat(filepath.Join(repo, "chain-victim")); err != nil {
			t.Fatalf("victim chain deleted while locked: %v", err)
		}
	}

	mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("retention after unlock: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retention did not finish after chain lock release")
	}
	if _, err := os.Stat(filepath.Join(repo, "chain-victim")); !os.IsNotExist(err) {
		t.Fatalf("victim chain should be reclaimed after unlock, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(repo, "chain-keeper")); err != nil {
		t.Fatalf("keeper chain missing: %v", err)
	}
}

// Restore and reclaim racing repeatedly must produce a deterministic outcome
// per run: either a complete restore identical to a concurrency-free one, or
// a clean ErrChainNotFound with zero writes. The retained chain stays
// restorable throughout.
func TestConcurrentReclaimRestoreDeterministic(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()

	const rounds = 25
	var total uint64
	for round := 0; round < rounds; round++ {
		put := func(n int) {
			t.Helper()
			for i := 0; i < n; i++ {
				total++
				if _, err := eng.Put(ctx, key(int(total)), fmt.Sprintf("v%d", total)); err != nil {
					t.Fatal(err)
				}
			}
		}
		put(3)
		victim, err := svc.FullBackup(ctx, FullOptions{ChainID: fmt.Sprintf("chain-victim-%d", round)})
		if err != nil {
			t.Fatal(err)
		}
		put(3)
		keeper, err := svc.FullBackup(ctx, FullOptions{ChainID: fmt.Sprintf("chain-keeper-%d", round)})
		if err != nil {
			t.Fatal(err)
		}

		svc.cfg.Retention = Retention{Count: 1}
		dest := filepath.Join(t.TempDir(), "out")
		var wg sync.WaitGroup
		var restoreErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, restoreErr = svc.RestoreTo(ctx, RestoreOptions{
				ChainID: victim.ChainID, TargetCSN: victim.PointCSN, DestDir: dest,
			})
		}()
		go func() {
			defer wg.Done()
			if err := svc.ApplyRetention(ctx); err != nil {
				t.Errorf("retention: %v", err)
			}
		}()
		wg.Wait()
		svc.cfg.Retention = Retention{}

		if restoreErr == nil {
			// Success must be byte-identical to a concurrency-free restore.
			verifyRestoredState(t, ctx, dest, victim.PointCSN, total)
		} else {
			if !errors.Is(restoreErr, ErrChainNotFound) {
				t.Fatalf("round %d: unexpected restore error: %v", round, restoreErr)
			}
			assertPathUntouched(t, dest)
		}

		// The victim chain is always gone once retention has run; the keeper
		// chain always survives.
		if _, err := os.Stat(filepath.Join(repo, victim.ChainID)); !os.IsNotExist(err) {
			t.Fatalf("round %d: victim chain still present, err=%v", round, err)
		}
		if _, err := os.Stat(filepath.Join(repo, keeper.ChainID)); err != nil {
			t.Fatalf("round %d: keeper chain missing: %v", round, err)
		}
	}

	// After all interleavings the retained chain still restores exactly.
	chains, err := svc.ListChains(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(chains) != 1 {
		t.Fatalf("retained chains=%d want 1", len(chains))
	}
	finalDest := filepath.Join(t.TempDir(), "final")
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: chains[0].ID, TargetCSN: chains[0].TipCSN, DestDir: finalDest,
	}); err != nil {
		t.Fatalf("final retained chain restore: %v", err)
	}
	verifyRestoredState(t, ctx, finalDest, chains[0].TipCSN, total)
}

// A restore interrupted mid-flight can be retried while retention runs
// concurrently: the retry must equal a single successful run, leave no
// partial output, and never modify the source chain.
func TestInterruptedRestoreRetryWithConcurrentReclaim(t *testing.T) {
	ctx := context.Background()
	eng, svc, engineDir, repo := openPair(t)
	defer eng.Close()

	const n = 400
	for i := 1; i <= n; i++ {
		if _, err := eng.Put(ctx, key(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	full, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-main"})
	if err != nil {
		t.Fatal(err)
	}
	// A second, newer chain so retention has something to reclaim without
	// touching chain-main.
	for i := n + 1; i <= n+3; i++ {
		if _, err := eng.Put(ctx, key(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-extra"}); err != nil {
		t.Fatal(err)
	}
	svc.cfg.Retention = Retention{Count: 2}

	repoBefore := treeMap(t, repo)

	// First attempt is cancelled immediately: it fails mid-flight and may
	// leave only staging debris, never a destination.
	dest := filepath.Join(t.TempDir(), "out")
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := svc.RestoreTo(cancelCtx, RestoreOptions{
		ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
	}); err == nil {
		t.Fatal("cancelled restore unexpectedly succeeded")
	}
	if _, err := os.Lstat(dest); !os.IsNotExist(err) {
		t.Fatalf("interrupted restore created destination: err=%v", err)
	}

	// Retry repeatedly while retention runs concurrently.
	const attempts = 5
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := svc.ApplyRetention(ctx); err != nil {
					t.Errorf("retention: %v", err)
					return
				}
			}
		}
	}()
	for a := 0; a < attempts; a++ {
		if _, err := svc.RestoreTo(ctx, RestoreOptions{
			ChainID: full.ChainID, TargetCSN: full.PointCSN, DestDir: dest,
		}); err != nil {
			t.Fatalf("retry %d: %v", a, err)
		}
		// Every successful retry is identical to a single successful run.
		verifyRestoredState(t, ctx, dest, full.PointCSN, n+3)
	}
	close(stop)
	wg.Wait()

	// No staging debris remains after a successful retry.
	if _, err := os.Lstat(dest + restoreStagingSuffix); !os.IsNotExist(err) {
		t.Fatalf("staging dir left behind: err=%v", err)
	}
	// The source chain was never modified by restore or retention.
	assertTreeEqual(t, repoBefore, repo)
	// Engine local state is also consistent (retention may checkpoint it).
	engCheck, err := openEngine(ctx, engineDir)
	if err != nil {
		t.Fatalf("engine dir no longer opens: %v", err)
	}
	if err := engCheck.Close(); err != nil {
		t.Fatal(err)
	}
}

// Sustained pressure: concurrent writes, full+incremental backups, restores
// and retention all at once. No deadlocks, no unexpected errors, and every
// successful restore matches a concurrency-free restore of the same point.
func TestConcurrentPressureDeterministic(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()
	svc.cfg.Retention = Retention{Count: 3}

	// Seed one chain so restores have something to work on immediately.
	for i := 1; i <= 5; i++ {
		if _, err := eng.Put(ctx, key(i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-seed"}); err != nil {
		t.Fatal(err)
	}

	var total atomic.Uint64
	total.Store(5)
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Single writer: key k<i> gets value v<i> at CSN i, strictly ordered.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			i := total.Add(1)
			if _, err := eng.Put(ctx, key(int(i)), fmt.Sprintf("v%d", i)); err != nil {
				errs <- fmt.Errorf("put: %w", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Two backup workers, each growing its own chain.
	for g := 0; g < 2; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			chainID := fmt.Sprintf("chain-worker-%d", g)
			first := true
			for {
				select {
				case <-stop:
					return
				default:
				}
				if first {
					if _, err := svc.FullBackup(ctx, FullOptions{ChainID: chainID}); err != nil {
						errs <- fmt.Errorf("full backup: %w", err)
						return
					}
					first = false
					continue
				}
				_, err := svc.IncrementalBackup(ctx, chainID)
				switch {
				case err == nil:
				case errors.Is(err, ErrChainNotFound):
					first = true // chain was reclaimed; start a fresh one
				case strings.Contains(err.Error(), "no new committed segments"),
					strings.Contains(err.Error(), "no new commits since chain tip"):
					time.Sleep(2 * time.Millisecond)
				default:
					errs <- fmt.Errorf("incremental: %w", err)
					return
				}
			}
		}()
	}

	// Retention worker.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				if err := svc.ApplyRetention(ctx); err != nil {
					errs <- fmt.Errorf("retention: %w", err)
					return
				}
			}
		}
	}()

	// Restore workers: pick the newest chain and restore its tip.
	for g := 0; g < 3; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			destRoot := t.TempDir()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				chains, err := svc.ListChains(ctx)
				if err != nil {
					errs <- fmt.Errorf("list chains: %w", err)
					return
				}
				if len(chains) == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				c := chains[(g+i)%len(chains)]
				dest := filepath.Join(destRoot, fmt.Sprintf("r%d", i))
				m, err := svc.RestoreTo(ctx, RestoreOptions{
					ChainID: c.ID, TargetCSN: c.TipCSN, DestDir: dest,
				})
				if err != nil {
					if errors.Is(err, ErrChainNotFound) {
						continue // reclaimed before we started: allowed
					}
					errs <- fmt.Errorf("restore chain %s: %w", c.ID, err)
					return
				}
				if m.PointCSN != c.TipCSN {
					errs <- fmt.Errorf("restore point %d != chain tip %d", m.PointCSN, c.TipCSN)
					return
				}
				verifyRestoredStateSample(t, dest, c.TipCSN, total.Load())
			}
		}()
	}

	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Every chain inside the retention window still restores exactly.
	chains, err := svc.ListChains(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(chains) == 0 {
		t.Fatal("no chains retained")
	}
	if len(chains) > 3 {
		t.Fatalf("retained %d chains, want <= 3", len(chains))
	}
	for _, c := range chains {
		dest := filepath.Join(t.TempDir(), "final")
		if _, err := svc.RestoreTo(ctx, RestoreOptions{
			ChainID: c.ID, TargetCSN: c.TipCSN, DestDir: dest,
		}); err != nil {
			t.Fatalf("retained chain %s no longer restorable: %v", c.ID, err)
		}
		verifyRestoredStateSample(t, dest, c.TipCSN, total.Load())
	}
	_ = repo
}

// verifyRestoredStateSample spot-checks a restored directory: exact CSN, the
// first/middle/last keys below the target, and absence of keys beyond it.
func verifyRestoredStateSample(t *testing.T, dir string, csn, total uint64) {
	t.Helper()
	r, err := openEngine(context.Background(), dir)
	if err != nil {
		t.Fatalf("open restored dir %s: %v", dir, err)
	}
	defer r.Close()
	if r.LastCSN() != csn {
		t.Fatalf("restored LastCSN=%d want %d", r.LastCSN(), csn)
	}
	ctx := context.Background()
	for _, i := range []uint64{1, csn / 2, csn} {
		if i == 0 {
			continue
		}
		v, ok, err := r.Get(ctx, key(int(i)))
		if err != nil || !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("restored key %d = %q ok=%v err=%v, want v%d", i, v, ok, err, i)
		}
	}
	for _, i := range []uint64{csn + 1, total} {
		if i <= csn || i > total {
			continue
		}
		if v, ok, _ := r.Get(ctx, key(int(i))); ok {
			t.Fatalf("key %d=%q exists beyond restored CSN %d", i, v, csn)
		}
	}
}
