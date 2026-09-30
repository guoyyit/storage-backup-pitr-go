package backup

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

func TestConcurrentBackupChainsAreIndependent(t *testing.T) {
	ctx := context.Background()
	engineDir := t.TempDir()
	repo := t.TempDir()
	eng, err := engine.Open(ctx, engineDir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	for i := 1; i <= 10; i++ {
		if _, err := eng.Put(ctx, "k", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	svc, err := NewService(ctx, eng, Config{Dir: repo})
	if err != nil {
		t.Fatal(err)
	}

	// Multiple backup tasks: each starts its own chain and appends increments.
	const tasks = 3
	errs := make(chan error, tasks*2)
	var wg sync.WaitGroup
	for g := 0; g < tasks; g++ {
		chainID := fmt.Sprintf("chain-g%d", g)
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := svc.FullBackup(ctx, FullOptions{ChainID: chainID}); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			// Wait until the full backup exists, then keep producing fresh
			// commits until our increment captures new data.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
				chains, _ := svc.ListChains(ctx)
				fullExists := false
				for _, c := range chains {
					if c.ID == chainID {
						fullExists = true
					}
				}
				if !fullExists {
					continue
				}
				if _, err := eng.Put(ctx, chainID, "late"); err != nil {
					errs <- err
					return
				}
				if _, err := svc.IncrementalBackup(ctx, chainID); err == nil {
					return
				}
			}
			errs <- fmt.Errorf("chain %s never got an increment", chainID)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent backup error: %v", err)
	}

	chains, err := svc.ListChains(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(chains) != tasks {
		t.Fatalf("chains=%d want %d", len(chains), tasks)
	}
	// Each chain must verify and restore to its own tip independently.
	for _, c := range chains {
		dest := t.TempDir() + "/r"
		m, err := svc.RestoreTo(ctx, RestoreOptions{
			ChainID: c.ID, TargetCSN: c.TipCSN, DestDir: dest,
		})
		if err != nil {
			t.Fatalf("chain %s restore: %v", c.ID, err)
		}
		r, err := engine.Open(ctx, dest)
		if err != nil {
			t.Fatal(err)
		}
		if r.LastCSN() != m.PointCSN {
			t.Fatalf("chain %s csn=%d want %d", c.ID, r.LastCSN(), m.PointCSN)
		}
		r.Close()
	}
}
