package backup

import (
	"context"
	"os"
	"testing"
	"time"
)

func seedCommits(t *testing.T, eng interface {
	Put(context.Context, string, string) (interface{}, error)
}) {
}

func TestRetentionByCount(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, repo := openPair(t)
	defer eng.Close()
	svc.cfg.Retention = Retention{Count: 2}

	var ids []string
	for c := 0; c < 4; c++ {
		for i := 0; i < 3; i++ {
			if _, err := eng.Put(ctx, "x", "y"); err != nil {
				t.Fatal(err)
			}
		}
		// Separate chains in time so ids differ.
		time.Sleep(2 * time.Millisecond)
		m, err := svc.FullBackup(ctx, FullOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, m.ChainID)
	}
	chains, err := svc.ListChains(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(chains) != 2 {
		t.Fatalf("retained chains=%d want 2", len(chains))
	}
	for _, dropped := range ids[:2] {
		if _, err := os.Stat(chainPath(repo, dropped)); !os.IsNotExist(err) {
			t.Fatalf("dropped chain %s still on disk", dropped)
		}
	}
	// The retained newest chain must still restore.
	dest := t.TempDir() + "/r"
	if _, err := svc.RestoreTo(ctx, RestoreOptions{
		ChainID: chains[0].ID, TargetCSN: chains[0].TipCSN, DestDir: dest,
	}); err != nil {
		t.Fatalf("retained chain restore failed: %v", err)
	}
}

func TestRetentionByAge(t *testing.T) {
	ctx := context.Background()
	eng, svc, _, _ := openPair(t)
	defer eng.Close()
	svc.cfg.Retention = Retention{Age: time.Hour}

	for i := 0; i < 3; i++ {
		if _, err := eng.Put(ctx, "a", "b"); err != nil {
			t.Fatal(err)
		}
	}
	old, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-old"})
	if err != nil {
		t.Fatal(err)
	}
	// Artificially age the old chain manifest.
	aged := time.Now().Add(-2 * time.Hour).UTC()
	old.PointTime = aged
	if err := writeMetaAtomically(svc.cfg.Dir, *old); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Put(ctx, "a", "c"); err != nil {
		t.Fatal(err)
	}
	newer, err := svc.FullBackup(ctx, FullOptions{ChainID: "chain-new"})
	if err != nil {
		t.Fatal(err)
	}
	chains, _ := svc.ListChains(ctx)
	if len(chains) != 1 || chains[0].ID != newer.ChainID {
		t.Fatalf("age retention kept %+v, want only %s", chainIDs(chains), newer.ChainID)
	}
}

func chainIDs(cs []Chain) []string {
	out := make([]string, len(cs))
	for i := range cs {
		out[i] = cs[i].ID
	}
	return out
}
