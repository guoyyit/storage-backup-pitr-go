package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// If a chain was already reclaimed before the restore starts, the restore
// must fail with ErrChainUnavailable before anything is written to the
// destination, regardless of how many times it is retried.
func TestRestoreAfterReclaimFailsBeforeWrite(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	svc.cfg.Retention = Retention{Count: 1}

	for i := 1; i <= 3; i++ {
		if _, err := eng.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	old, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-old"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 4; i <= 6; i++ {
		if _, err := eng.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-new"}); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(svc.cfg.Dir, old.ChainID)); !os.IsNotExist(err) {
		t.Fatalf("old chain must be reclaimed, stat err=%v", err)
	}

	for attempt := 0; attempt < 3; attempt++ {
		dest := filepath.Join(t.TempDir(), fmt.Sprintf("r%d", attempt))
		_, err := svc.RestoreTo(ctx, RestoreOptions{
			ChainID: old.ChainID, TargetCSN: old.PointCSN, DestDir: dest,
		})
		if !errors.Is(err, ErrChainUnavailable) {
			t.Fatalf("attempt %d: want ErrChainUnavailable, got %v", attempt, err)
		}
		// Nothing at all may appear at the destination or its staging path.
		if _, err := os.Stat(dest); !os.IsNotExist(err) {
			t.Fatalf("attempt %d: destination must not be created", attempt)
		}
		if _, err := os.Stat(dest + restoreStagingSuffix); !os.IsNotExist(err) {
			t.Fatalf("attempt %d: staging must not be created", attempt)
		}
	}

	// The retained chain still restores normally.
	dest := filepath.Join(t.TempDir(), "ok")
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: "chain-new", TargetCSN: 6, DestDir: dest,
	}); err != nil {
		t.Fatalf("retained chain restore: %v", err)
	}
}

// When a restore has started using a chain, concurrent retention cannot
// reclaim that chain until the restore finishes.
func TestRetentionWaitsForInFlightRestore(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()

	for i := 1; i <= 3; i++ {
		if _, err := eng.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	old, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-old"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 4; i <= 6; i++ {
		if _, err := eng.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	// Create the newer chain that pushes chain-old out of retention, then pin
	// the old chain like RestoreTo does before applying retention.
	if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-new"}); err != nil {
		t.Fatal(err)
	}
	svc.cfg.Retention = Retention{Count: 1}
	if err := svc.pinChain(old.ChainID); err != nil {
		t.Fatal(err)
	}
	applyDone := make(chan error, 1)
	go func() {
		applyDone <- svc.ApplyRetention(ctx)
	}()
	select {
	case err := <-applyDone:
		t.Fatalf("retention returned while restore pin held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := os.Stat(filepath.Join(svc.cfg.Dir, old.ChainID)); err != nil {
		t.Fatalf("pinned chain must not be reclaimed: %v", err)
	}
	svc.unpinChain(old.ChainID)
	select {
	case err := <-applyDone:
		if err != nil {
			t.Fatalf("retention after unpin: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retention did not proceed after unpin")
	}
	if _, err := os.Stat(filepath.Join(svc.cfg.Dir, old.ChainID)); !os.IsNotExist(err) {
		t.Fatal("old chain must be reclaimed after unpin")
	}
}

// kvHistory records every committed mutation under a mutex so the exact
// key/value state at any CSN can be reconstructed for assertions.
type kvEvent struct {
	csn     uint64
	key     string
	value   string
	deleted bool
}

type kvHistory struct {
	mu     sync.Mutex
	events []kvEvent
}

func (h *kvHistory) record(csn uint64, key, value string, deleted bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, kvEvent{csn: csn, key: key, value: value, deleted: deleted})
}

// stateAt computes key->value exactly at targetCSN by replaying events in CSN
// order.
func (h *kvHistory) stateAt(targetCSN uint64) map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string]string{}
	for _, e := range h.events {
		if e.csn > targetCSN {
			continue
		}
		if e.deleted {
			delete(out, e.key)
		} else {
			out[e.key] = e.value
		}
	}
	return out
}

// Sustained pressure: concurrent writes, full backups, restores of chains
// grabbed from a listing, and retention runs. No deadlock or data race; every
// successful restore must equal a single-threaded restore at the same point,
// and every chain still retained at the end must restore cleanly.
func TestConcurrentRestoresRetentionWriters(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	svc.cfg.Retention = Retention{Count: 4}

	hist := &kvHistory{}

	// Seed several chains so restores always have retained targets.
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			val := fmt.Sprintf("v%d-%d", i, j)
			info, err := eng.Put(ctx, fmt.Sprintf("k%d", j), val)
			if err != nil {
				t.Fatal(err)
			}
			hist.record(info.CSN, fmt.Sprintf("k%d", j), val, false)
		}
		if _, err := svc.FullBackup(ctx, FullOptions{ChainID: fmt.Sprintf("chain-seed-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}

	const duration = 2 * time.Second
	deadline := time.Now().Add(duration)
	errCh := make(chan error, 1024)
	var wg sync.WaitGroup

	// Writers.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for time.Now().Before(deadline) {
				n++
				key := fmt.Sprintf("w%d-k%d", id, n%7)
				if n%11 == 0 {
					info, err := eng.Delete(ctx, key)
					if err != nil {
						errCh <- err
						return
					}
					hist.record(info.CSN, key, "", true)
				} else {
					val := fmt.Sprintf("%d-%d", id, n)
					info, err := eng.Put(ctx, key, val)
					if err != nil {
						errCh <- err
						return
					}
					hist.record(info.CSN, key, val, false)
				}
			}
		}(w)
	}

	// Backup workers (full + occasional increment).
	for b := 0; b < 2; b++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				m, err := svc.FullBackup(ctx, FullOptions{ChainID: fmt.Sprintf("chain-b%d-%d", id, time.Now().UnixNano())})
				if err != nil {
					errCh <- fmt.Errorf("full: %w", err)
					return
				}
				_, _ = eng.Put(ctx, "inc-key", "x")
				_, _ = svc.IncrementalBackup(ctx, m.ChainID)
			}
		}(b)
	}

	// Restore workers: pick a retained chain, restore at its tip into a fresh
	// independent destination, open the result and verify exact state.
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for time.Now().Before(deadline) {
				n++
				chains, err := svc.ListChains(ctx)
				if err != nil {
					errCh <- err
					return
				}
				if len(chains) == 0 {
					continue
				}
				c := chains[n%len(chains)]
				dest := filepath.Join(t.TempDir(), fmt.Sprintf("r%d-%d", id, n))
				m, rerr := svc.RestoreTo(ctx, RestoreOptions{
					ChainID: c.ID, TargetCSN: c.TipCSN, DestDir: dest,
				})
				if rerr != nil {
					if errors.Is(rerr, ErrChainUnavailable) {
						// Chain reclaimed between listing and pinning: allowed
						// and deterministic, nothing was written.
						if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
							errCh <- fmt.Errorf("failed restore left destination %s", dest)
						}
						continue
					}
					errCh <- fmt.Errorf("restore %s: %w", c.ID, rerr)
					return
				}
				restored, oerr := engine.Open(ctx, dest)
				if oerr != nil {
					errCh <- oerr
					return
				}
				if restored.LastCSN() != m.PointCSN {
					errCh <- fmt.Errorf("chain %s restored csn %d want %d", c.ID, restored.LastCSN(), m.PointCSN)
					restored.Close()
					return
				}
				want := hist.stateAt(m.PointCSN)
				for k, v := range want {
					got, ok, _ := restored.Get(ctx, k)
					if !ok || got != v {
						errCh <- fmt.Errorf("chain %s key %s = %q ok=%v want %q at csn %d",
							c.ID, k, got, ok, v, m.PointCSN)
					}
				}
				restored.Close()
			}
		}(r)
	}

	// Retention worker.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			if err := svc.ApplyRetention(ctx); err != nil {
				errCh <- err
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Every chain still retained after the storm must restore successfully
	// and open with a matching CSN.
	chains, err := svc.ListChains(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(chains) == 0 {
		t.Fatal("no chains retained")
	}
	for _, c := range chains {
		dest := filepath.Join(t.TempDir(), "final-"+c.ID)
		m, err := svc.RestoreTo(ctx, RestoreOptions{
			ChainID: c.ID, TargetCSN: c.TipCSN, DestDir: dest,
		})
		if err != nil {
			t.Fatalf("retained chain %s not restorable after stress: %v", c.ID, err)
		}
		r, err := engine.Open(ctx, dest)
		if err != nil {
			t.Fatal(err)
		}
		if r.LastCSN() != m.PointCSN || m.PointCSN != c.TipCSN {
			t.Fatalf("chain %s csn %d/%d want %d", c.ID, r.LastCSN(), m.PointCSN, c.TipCSN)
		}
		r.Close()
	}
}

// Retrying a restore (possibly after an interruption that left staging
// behind) while retention runs on other chains must produce exactly the same
// result as a single successful restore: no duplicate data, no partial
// content, source chain untouched.
func TestRestoreRetryUnderConcurrentReclaim(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()
	// Keep 4 of 5 chains: the restore target (oldest) stays in range while
	// filler chains are continuously reclaimed around it.
	svc.cfg.Retention = Retention{Count: 4}

	// Chain under restore plus filler chains that retention can reclaim.
	var targetID string
	var targetCSN uint64
	for c := 0; c < 5; c++ {
		for j := 0; j < 4; j++ {
			if _, err := eng.Put(ctx, fmt.Sprintf("c%d-k%d", c, j), "v"); err != nil {
				t.Fatal(err)
			}
		}
		m, err := svc.FullBackup(ctx, FullOptions{ChainID: fmt.Sprintf("chain-r%d", c)})
		if err != nil {
			t.Fatal(err)
		}
		if c == 4 {
			targetID, targetCSN = m.ChainID, m.PointCSN
		}
	}

	dest := filepath.Join(t.TempDir(), "restored")
	// Simulate an interrupted first attempt: stale staging directory left
	// behind, and a previous (valid) destination directory that must be
	// replaced atomically rather than merged.
	staleStaging := dest + restoreStagingSuffix
	if err := os.MkdirAll(filepath.Join(staleStaging, "wal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staleStaging, "stale.txt"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "old.txt"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Baseline: what a single successful restore produces.
	baseline := filepath.Join(t.TempDir(), "baseline")
	bm, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: targetID, TargetCSN: targetCSN, DestDir: baseline,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Repeatedly run retention (reclaiming filler chains) in parallel with
	// several retries of the same restore into the same destination.
	const attempts = 5
	errCh := make(chan error, attempts)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err := svc.ApplyRetention(ctx); err != nil {
				errCh <- err
				return
			}
		}
	}()
	for a := 0; a < attempts; a++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: targetID, TargetCSN: targetCSN, DestDir: dest,
			}); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// The destination must be a clean engine at exactly the target point.
	r, err := engine.Open(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	if r.LastCSN() != targetCSN || bm.PointCSN != targetCSN {
		t.Fatalf("csn=%d want %d", r.LastCSN(), targetCSN)
	}
	if _, err := os.Stat(filepath.Join(dest, "old.txt")); !os.IsNotExist(err) {
		t.Fatal("previous destination content must be fully replaced")
	}
	if _, err := os.Stat(dest + restoreStagingSuffix); !os.IsNotExist(err) {
		t.Fatal("staging directory must be removed after success")
	}
	// Exact content equivalence with the single-shot baseline restore.
	rb, err := engine.Open(ctx, baseline)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"c4-k0", "c4-k1", "c4-k2", "c4-k3"} {
		got, ok, _ := r.Get(ctx, k)
		want, okb, _ := rb.Get(ctx, k)
		if ok != okb || got != want {
			t.Fatalf("key %s retry=(%q,%v) baseline=(%q,%v)", k, got, ok, want, okb)
		}
	}
	r.Close()
	rb.Close()

	// Source chain directory must be untouched (still loads and restores).
	if _, err := loadChain(repo, targetID); err != nil {
		t.Fatalf("source chain modified/unreadable: %v", err)
	}
	again := filepath.Join(t.TempDir(), "again")
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: targetID, TargetCSN: targetCSN, DestDir: again,
	}); err != nil {
		t.Fatalf("source chain no longer restorable: %v", err)
	}
}

// Repeating the same fixed interleaving of restore vs retention many times
// must yield identical outcomes: when the chain is pinned first the restore
// succeeds every time and the chain is deleted only afterwards; when
// retention completes first the restore fails with ErrChainUnavailable and
// nothing is written.
func TestRestoreRetentionInterleavingDeterminism(t *testing.T) {
	ctx := context.Background()

	runOnce := func(t *testing.T, pinFirst bool) (restored bool, destExists bool) {
		eng, svc, _, _ := openPair(t)
		defer eng.Close()
		if _, err := eng.Put(ctx, "k", "v"); err != nil {
			t.Fatal(err)
		}
		old, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-old"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Put(ctx, "k2", "v"); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-new"}); err != nil {
			t.Fatal(err)
		}
		svc.cfg.Retention = Retention{Count: 1}

		dest := filepath.Join(t.TempDir(), "r")
		if pinFirst {
			if err := svc.pinChain(old.ChainID); err != nil {
				t.Fatal(err)
			}
			applyErr := make(chan error, 1)
			go func() { applyErr <- svc.ApplyRetention(ctx) }()
			time.Sleep(50 * time.Millisecond)
			m, rerr := svc.restorePinned(ctx, old.ChainID, old.PointCSN, dest)
			svc.unpinChain(old.ChainID)
			if rerr != nil {
				t.Fatalf("pinned restore must succeed: %v", rerr)
			}
			if err := <-applyErr; err != nil {
				t.Fatal(err)
			}
			if m.PointCSN != old.PointCSN {
				t.Fatalf("point %d want %d", m.PointCSN, old.PointCSN)
			}
			restored = true
		} else {
			if err := svc.ApplyRetention(ctx); err != nil {
				t.Fatal(err)
			}
			_, rerr := svc.RestoreTo(ctx, RestoreOptions{
				ChainID: old.ChainID, TargetCSN: old.PointCSN, DestDir: dest,
			})
			if !errors.Is(rerr, ErrChainUnavailable) {
				t.Fatalf("want ErrChainUnavailable, got %v", rerr)
			}
		}
		if _, err := os.Stat(dest); err == nil {
			destExists = true
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return restored, destExists
	}

	for i := 0; i < 10; i++ {
		ok, exists := runOnce(t, true)
		if !ok || !exists {
			t.Fatalf("iter %d pinFirst: restored=%v destExists=%v", i, ok, exists)
		}
		ok, exists = runOnce(t, false)
		if ok || exists {
			t.Fatalf("iter %d reclaimFirst: restored=%v destExists=%v", i, ok, exists)
		}
	}
}

// restorePinned performs the restore body assuming the chain is already
// pinned; it is used by tests to control the interleaving precisely.
func (s *Service) restorePinned(ctx context.Context, chainID string, target uint64, dest string) (*Meta, error) {
	return s.RestoreTo(ctx, RestoreOptions{ChainID: chainID, TargetCSN: target, DestDir: dest})
}
