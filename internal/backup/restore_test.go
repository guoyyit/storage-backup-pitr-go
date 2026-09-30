package backup_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
)

// 场景 3：恢复中断后可重新执行，结果与一次成功恢复完全一致，不残留部分写入。
func TestRestoreRetryAfterInterruption(t *testing.T) {
	f := newFixture(t)
	chainID, _ := buildChain(t, f)

	// 参考结果：一次完整恢复。
	refDir := t.TempDir() + "/ref"
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: refDir, ChainID: chainID}); err != nil {
		t.Fatal(err)
	}
	refSeq, refState := dumpState(t, refDir)

	// 模拟上次恢复中断：目标目录不存在，但残留半个临时目录（部分写入）。
	target := t.TempDir() + "/out"
	tmpDir := target + ".restore-tmp"
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "wal.log"), []byte{0x01, 0x02, 0x03}, 0o644); err != nil {
		t.Fatal(err)
	}

	// 重新执行恢复：必须清掉残留并产出与参考完全一致的结果。
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: target, ChainID: chainID}); err != nil {
		t.Fatalf("retry restore: %v", err)
	}
	gotSeq, gotState := dumpState(t, target)
	if gotSeq != refSeq {
		t.Fatalf("retry seq = %d, want %d", gotSeq, refSeq)
	}
	assertStateEqual(t, gotState, refState)
	if _, err := os.Stat(tmpDir); !os.IsNotExist(err) {
		t.Fatal("temp dir must not remain after successful restore")
	}

	// 幂等：对同一目标重复执行同一恢复，成功且结果不变。
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: target, ChainID: chainID}); err != nil {
		t.Fatalf("idempotent restore: %v", err)
	}
	gotSeq2, gotState2 := dumpState(t, target)
	if gotSeq2 != refSeq {
		t.Fatalf("idempotent seq = %d, want %d", gotSeq2, refSeq)
	}
	assertStateEqual(t, gotState2, refState)
}

// 恢复不得写入运行中引擎目录或备份目录。
func TestRestoreRejectsUnsafeTargets(t *testing.T) {
	f := newFixture(t)
	chainID, _ := buildChain(t, f)

	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: f.engDir, ChainID: chainID}); err == nil {
		t.Fatal("restore into running engine dir must be rejected")
	}
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: filepath.Join(f.bakDir, "x"), ChainID: chainID}); err == nil {
		t.Fatal("restore into backup dir must be rejected")
	}
	// 目标目录被无关内容占用时拒绝覆盖。
	occupied := t.TempDir()
	if err := os.WriteFile(filepath.Join(occupied, "user-data.txt"), []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: occupied, ChainID: chainID}); err == nil {
		t.Fatal("restore over non-empty foreign dir must be rejected")
	}
	if _, err := os.Stat(filepath.Join(occupied, "user-data.txt")); err != nil {
		t.Fatal("foreign content must not be touched")
	}
}

// 恢复到目标点早于链基线时必须明确报错。
func TestRestoreRejectsPointBeforeBase(t *testing.T) {
	f := newFixture(t)
	chainID, _ := buildChain(t, f)
	err := f.mgr.Restore(backup.RestoreOptions{TargetDir: t.TempDir() + "/r", ChainID: chainID, ToSeq: 1})
	if err == nil {
		t.Fatal("restore to seq before chain base must fail")
	}
}
