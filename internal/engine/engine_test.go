package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Open(Options{Dir: dir, SyncWrites: true, SnapshotThreshold: 50})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return e
}

func TestBasicPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	e := openTemp(t, dir)
	defer e.Close()
	if _, err := e.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.Get("a")
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("get a = %q,%v,%v", v, ok, err)
	}
	if _, err := e.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := e.Get("a"); ok {
		t.Fatal("a should be deleted")
	}
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	e := openTemp(t, dir)
	for i := 0; i < 120; i++ { // 触发至少一次快照+WAL归档
		if _, err := e.Put(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟崩溃：不 Close，直接重开
	e2 := openTemp(t, dir)
	defer e2.Close()
	for i := 0; i < 120; i++ {
		v, ok, _ := e2.Get(fmt.Sprintf("k%03d", i))
		if !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%03d = %q,%v", i, v, ok)
		}
	}
	if e2.LastSeq() != 120 {
		t.Fatalf("lastSeq = %d, want 120", e2.LastSeq())
	}
}

func TestTornWALTailIgnored(t *testing.T) {
	dir := t.TempDir()
	e := openTemp(t, dir)
	for i := 0; i < 10; i++ {
		if _, err := e.Put(fmt.Sprintf("k%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	e.Close()
	// 在 WAL 尾部追加半个撕裂帧，模拟崩溃时的部分写入
	f, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	e2 := openTemp(t, dir)
	defer e2.Close()
	if e2.LastSeq() != 10 {
		t.Fatalf("lastSeq = %d, want 10 (torn tail must be ignored)", e2.LastSeq())
	}
	for i := 0; i < 10; i++ {
		if _, ok, _ := e2.Get(fmt.Sprintf("k%d", i)); !ok {
			t.Fatalf("k%d missing after recovery", i)
		}
	}
}

func TestConsistentSnapshotPoint(t *testing.T) {
	dir := t.TempDir()
	e := openTemp(t, dir)
	defer e.Close()
	e.Put("a", []byte("1"))
	seq, ts, entries, err := e.ConsistentSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 || ts == 0 || len(entries) != 1 || entries[0].Key != "a" {
		t.Fatalf("snapshot = seq %d ts %d entries %v", seq, ts, entries)
	}
	e.Put("b", []byte("2"))
	// 旧快照内容不随后续写入变化
	if len(entries) != 1 {
		t.Fatalf("snapshot entries mutated: %v", entries)
	}
}

func TestWALRecordsRange(t *testing.T) {
	dir := t.TempDir()
	e := openTemp(t, dir)
	defer e.Close()
	for i := 0; i < 200; i++ { // 跨多次 WAL 归档
		e.Put(fmt.Sprintf("k%03d", i), []byte("v"))
	}
	recs, err := e.WALRecords(50, 120)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 70 || recs[0].Seq != 51 || recs[len(recs)-1].Seq != 120 {
		t.Fatalf("WALRecords(50,120) got %d records, first=%d last=%d",
			len(recs), recs[0].Seq, recs[len(recs)-1].Seq)
	}
}
