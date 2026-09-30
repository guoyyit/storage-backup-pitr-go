package backup_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
)

// buildChain 构造一条 1 全量 + 2 增量的链，返回链 ID 与全部段文件路径。
func buildChain(t *testing.T, f *fixture) (string, []string) {
	t.Helper()
	for i := 0; i < 10; i++ {
		f.eng.Put(fmt.Sprintf("a-%d", i), []byte("va"))
	}
	chainID, err := f.mgr.FullBackup()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		f.eng.Put(fmt.Sprintf("b-%d", i), []byte("vb"))
	}
	if _, err := f.mgr.IncrementalBackup(chainID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		f.eng.Put(fmt.Sprintf("c-%d", i), []byte("vc"))
	}
	if _, err := f.mgr.IncrementalBackup(chainID); err != nil {
		t.Fatal(err)
	}
	chains, err := f.mgr.Chains()
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, mf := range chains {
		if mf.ChainID == chainID {
			for _, seg := range mf.Segments {
				files = append(files, filepath.Join(f.bakDir, "chains", chainID, seg.File))
			}
		}
	}
	if len(files) != 3 {
		t.Fatalf("expect 3 segment files, got %v", files)
	}
	return chainID, files
}

// 场景 4a：任意段损坏都必须被校验发现，错误定位到具体段文件。
func TestCorruptionDetectedPerSegment(t *testing.T) {
	for segIdx := 0; segIdx < 3; segIdx++ {
		t.Run(fmt.Sprintf("seg%d", segIdx), func(t *testing.T) {
			f := newFixture(t)
			chainID, files := buildChain(t, f)

			// 完好链：校验必须通过，不得误判。
			if err := f.mgr.ValidateChain(chainID); err != nil {
				t.Fatalf("intact chain must validate: %v", err)
			}

			// 翻转目标段中间的一个字节，模拟磁盘损坏。
			victim := files[segIdx]
			data, err := os.ReadFile(victim)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)/2] ^= 0xff
			if err := os.WriteFile(victim, data, 0o644); err != nil {
				t.Fatal(err)
			}

			// 校验必须报错，且错误能定位到具体损坏段。
			err = f.mgr.ValidateChain(chainID)
			var ce *backup.CorruptionError
			if !errors.As(err, &ce) {
				t.Fatalf("want CorruptionError, got %v", err)
			}
			if ce.File != filepath.Base(victim) {
				t.Fatalf("corruption located at %q, want %q", ce.File, filepath.Base(victim))
			}

			// 不得用损坏数据静默恢复：恢复必须失败。
			target := t.TempDir() + "/r"
			restoreErr := f.mgr.Restore(backup.RestoreOptions{TargetDir: target, ChainID: chainID})
			if restoreErr == nil {
				t.Fatal("restore from corrupted chain must fail")
			}
			if !strings.Contains(restoreErr.Error(), filepath.Base(victim)) {
				t.Fatalf("restore error must name corrupted segment, got %v", restoreErr)
			}
			// 恢复失败不得留下目标目录。
			if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
				t.Fatal("failed restore must not leave target dir behind")
			}
		})
	}
}

// 场景 4b：段文件被截断（部分写入）同样被检测。
func TestTruncatedSegmentDetected(t *testing.T) {
	f := newFixture(t)
	chainID, files := buildChain(t, f)
	victim := files[1]
	data, _ := os.ReadFile(victim)
	if err := os.WriteFile(victim, data[:len(data)-7], 0o644); err != nil {
		t.Fatal(err)
	}
	var ce *backup.CorruptionError
	if err := f.mgr.ValidateChain(chainID); !errors.As(err, &ce) {
		t.Fatalf("truncated segment must be detected, got %v", err)
	}
}

// 场景 4c：段文件丢失也被明确报告。
func TestMissingSegmentDetected(t *testing.T) {
	f := newFixture(t)
	chainID, files := buildChain(t, f)
	if err := os.Remove(files[2]); err != nil {
		t.Fatal(err)
	}
	if err := f.mgr.ValidateChain(chainID); err == nil {
		t.Fatal("missing segment must be detected")
	}
}
