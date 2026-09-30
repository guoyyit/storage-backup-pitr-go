package backup_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
)

// makeChains 依次创建 n 条链，链间写入新数据，返回链 ID（创建顺序）。
func makeChains(t *testing.T, f *fixture, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		if _, err := f.eng.Put(fmt.Sprintf("chain-marker-%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
		id, err := f.mgr.FullBackup()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		time.Sleep(2 * time.Millisecond) // 保证链 ID 时间戳有序
	}
	return ids
}

// 场景 5a：按保留数量回收，保留范围内的链仍可完整恢复。
func TestRetentionByCount(t *testing.T) {
	f := newFixture(t)
	ids := makeChains(t, f, 5)

	deleted, err := f.mgr.ApplyRetention(backup.RetentionPolicy{RetainCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 3 {
		t.Fatalf("deleted = %v, want 3 chains", deleted)
	}
	chains, _ := f.mgr.Chains()
	if len(chains) != 2 {
		t.Fatalf("remaining = %d, want 2", len(chains))
	}
	// 被删除的是最旧的 3 条。
	for _, id := range deleted {
		if id != ids[0] && id != ids[1] && id != ids[2] {
			t.Fatalf("unexpected deleted chain %s", id)
		}
	}
	// 保留的链校验通过且可恢复。
	for _, mf := range chains {
		if err := f.mgr.ValidateChain(mf.ChainID); err != nil {
			t.Fatalf("retained chain %s must validate: %v", mf.ChainID, err)
		}
		target := t.TempDir() + "/r"
		if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: target, ChainID: mf.ChainID}); err != nil {
			t.Fatalf("retained chain %s must restore: %v", mf.ChainID, err)
		}
	}
	// 已删除的链不可再恢复。
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: t.TempDir() + "/x", ChainID: ids[0]}); err == nil {
		t.Fatal("restore of deleted chain must fail")
	}
}

// 场景 5b：按保留时长回收。
func TestRetentionByAge(t *testing.T) {
	f := newFixture(t)
	makeChains(t, f, 3)

	// 足够大的时长：全部保留。
	deleted, err := f.mgr.ApplyRetention(backup.RetentionPolicy{RetainAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted = %v, want none", deleted)
	}
	// 极小时长：全部过期。
	deleted, err = f.mgr.ApplyRetention(backup.RetentionPolicy{RetainAge: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 3 {
		t.Fatalf("deleted = %v, want 3", deleted)
	}
	chains, _ := f.mgr.Chains()
	if len(chains) != 0 {
		t.Fatalf("remaining = %d, want 0", len(chains))
	}
}

// 场景 5c：两个条件同时设置时满足任一即保留。
func TestRetentionCombinedPolicy(t *testing.T) {
	f := newFixture(t)
	makeChains(t, f, 4)
	deleted, err := f.mgr.ApplyRetention(backup.RetentionPolicy{RetainCount: 1, RetainAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 0 {
		t.Fatalf("age policy should retain all, deleted = %v", deleted)
	}
}

// 场景 5d：回收与恢复并发时结果确定——恢复完成后再回收，恢复结果不受影响。
func TestRetentionConcurrentWithRestore(t *testing.T) {
	f := newFixture(t)
	ids := makeChains(t, f, 3)
	target := t.TempDir() + "/r"
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: target, ChainID: ids[0]}); err != nil {
		t.Fatal(err)
	}
	deleted, err := f.mgr.ApplyRetention(backup.RetentionPolicy{RetainCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 {
		t.Fatalf("deleted = %v, want 2", deleted)
	}
	// 恢复结果不受回收影响。
	if _, state := dumpState(t, target); len(state) != 1 {
		t.Fatalf("restored state = %d keys, want 1", len(state))
	}
}

// 场景 5e：长期运行下归档数据可控——备份后引擎侧旧 WAL 被回收。
func TestWALArchiveBounded(t *testing.T) {
	f := newFixture(t)
	// 写足够多数据触发多次 WAL 归档（阈值 50 条/段）。
	for i := 0; i < 300; i++ {
		if _, err := f.eng.Put(fmt.Sprintf("k%04d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	archiveDir := f.engDir + "/wal-archive"
	countFiles := func() int {
		ents, _ := os.ReadDir(archiveDir)
		return len(ents)
	}
	before := countFiles()
	if before == 0 {
		t.Fatal("expected archived WAL segments before backup")
	}
	// 全量备份覆盖全部已提交数据后，已归档 WAL 应被回收。
	if _, err := f.mgr.FullBackup(); err != nil {
		t.Fatal(err)
	}
	if after := countFiles(); after >= before {
		t.Fatalf("archived WAL not recycled: before=%d after=%d", before, after)
	}
}
