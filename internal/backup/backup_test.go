package backup_test

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

type fixture struct {
	eng    *engine.Engine
	mgr    *backup.Manager
	engDir string
	bakDir string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	engDir := root + "/engine"
	bakDir := root + "/backup"
	eng, err := engine.Open(engine.Options{Dir: engDir, SyncWrites: false, SnapshotThreshold: 50})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	mgr, err := backup.NewManager(eng, bakDir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return &fixture{eng: eng, mgr: mgr, engDir: engDir, bakDir: bakDir}
}

// dumpState 以只读方式打开一个引擎目录并导出全部状态。
func dumpState(t *testing.T, dir string) (uint64, map[string]string) {
	t.Helper()
	e, err := engine.Open(engine.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer e.Close()
	seq, _, entries, err := e.ConsistentSnapshot()
	if err != nil {
		t.Fatalf("snapshot %s: %v", dir, err)
	}
	state := make(map[string]string, len(entries))
	for _, en := range entries {
		state[en.Key] = string(en.Value)
	}
	return seq, state
}

// seqTracker 记录并发写入的 (key, value, seq)，用于精确构造某 Seq 时刻的期望状态。
type seqTracker struct {
	mu   sync.Mutex
	recs []committedRec
}

type committedRec struct {
	key, val string
	seq      uint64
}

func (s *seqTracker) add(key, val string, seq uint64) {
	s.mu.Lock()
	s.recs = append(s.recs, committedRec{key, val, seq})
	s.mu.Unlock()
}

// stateAt 构造 Seq <= upto 时的期望状态。
func (s *seqTracker) stateAt(upto uint64) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string)
	for _, r := range s.recs {
		if r.seq <= upto {
			out[r.key] = r.val
		}
	}
	return out
}

func assertStateEqual(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("state size = %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %s = %q, want %q", k, got[k], v)
		}
	}
}

// 场景 1：全量备份期间并发写入，备份必须等于某个已提交 Seq 的精确状态。
func TestFullBackupConsistentUnderConcurrentWrites(t *testing.T) {
	f := newFixture(t)
	tracker := &seqTracker{}
	var counter atomic.Int64

	// 预加载数据
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("pre-%03d", i)
		seq, err := f.eng.Put(key, []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		tracker.add(key, fmt.Sprintf("v%d", i), seq)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := counter.Add(1)
				key := fmt.Sprintf("w%08d", n)
				val := fmt.Sprintf("val-%d", n)
				seq, err := f.eng.Put(key, []byte(val))
				if err == nil {
					tracker.add(key, val, seq)
				}
			}
		}()
	}

	// 写入进行中执行全量备份
	time.Sleep(20 * time.Millisecond)
	chainID, err := f.mgr.FullBackup()
	if err != nil {
		t.Fatalf("full backup: %v", err)
	}
	close(stop)
	wg.Wait()

	chains, err := f.mgr.Chains()
	if err != nil {
		t.Fatal(err)
	}
	var mf *backup.Manifest
	for i := range chains {
		if chains[i].ChainID == chainID {
			mf = &chains[i]
		}
	}
	if mf == nil || len(mf.Segments) != 1 {
		t.Fatalf("chain %s not found or has %d segments", chainID, len(mf.Segments))
	}
	fullSeq := mf.Segments[0].ToSeq

	// 恢复并比对：恢复结果必须恰好等于 fullSeq 时刻的已提交状态。
	target := t.TempDir() + "/restored"
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: target, ChainID: chainID}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	gotSeq, got := dumpState(t, target)
	if gotSeq != fullSeq {
		t.Fatalf("restored seq = %d, want %d", gotSeq, fullSeq)
	}
	assertStateEqual(t, got, tracker.stateAt(fullSeq))

	// 源引擎不受恢复影响，仍可读写。
	if _, err := f.eng.Put("after-restore", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := f.mgr.ValidateChain(chainID); err != nil {
		t.Fatalf("chain must stay valid after restore: %v", err)
	}
}

// 场景 2：增量备份组成可追溯的链，可恢复到任意历史提交时间点。
func TestIncrementalChainAndPITR(t *testing.T) {
	f := newFixture(t)

	writeBatch := func(prefix string, n int) uint64 {
		var last uint64
		for i := 0; i < n; i++ {
			seq, err := f.eng.Put(fmt.Sprintf("%s-%02d", prefix, i), []byte(prefix))
			if err != nil {
				t.Fatal(err)
			}
			last = seq
		}
		return last
	}

	writeBatch("a", 10)
	chainID, err := f.mgr.FullBackup()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	cutAB := time.Now() // A 与 B 之间的时间切点
	time.Sleep(10 * time.Millisecond)

	seqB := writeBatch("b", 10)
	if _, err := f.mgr.IncrementalBackup(chainID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	cutBC := time.Now()
	time.Sleep(10 * time.Millisecond)

	writeBatch("c", 10)
	if _, err := f.mgr.IncrementalBackup(chainID); err != nil {
		t.Fatal(err)
	}

	// 链结构：1 全量 + 2 增量，Seq 区间连续衔接。
	chains, _ := f.mgr.Chains()
	if len(chains) != 1 || len(chains[0].Segments) != 3 {
		t.Fatalf("chain structure: %+v", chains)
	}
	segs := chains[0].Segments
	if segs[0].Kind != backup.SegmentFull || segs[1].FromSeq != segs[0].ToSeq || segs[2].FromSeq != segs[1].ToSeq {
		t.Fatalf("segments not chained: %+v", segs)
	}

	keysWithPrefix := func(state map[string]string, prefixes ...string) map[string]string {
		out := map[string]string{}
		for _, p := range prefixes {
			for i := 0; i < 10; i++ {
				k := fmt.Sprintf("%s-%02d", p, i)
				out[k] = p
			}
		}
		return out
	}

	// PITR 到 A 结束时刻：只有 A，B/C 不得出现。
	targetA := t.TempDir() + "/ra"
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: targetA, ChainID: chainID, PointInTime: cutAB}); err != nil {
		t.Fatal(err)
	}
	_, gotA := dumpState(t, targetA)
	assertStateEqual(t, gotA, keysWithPrefix(nil, "a"))

	// PITR 到 B 结束时刻：A+B，无 C。
	targetB := t.TempDir() + "/rb"
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: targetB, ChainID: chainID, PointInTime: cutBC}); err != nil {
		t.Fatal(err)
	}
	_, gotB := dumpState(t, targetB)
	assertStateEqual(t, gotB, keysWithPrefix(nil, "a", "b"))

	// 按 Seq 恢复到 B 结束：与按时间等价。
	targetB2 := t.TempDir() + "/rb2"
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: targetB2, ChainID: chainID, ToSeq: seqB}); err != nil {
		t.Fatal(err)
	}
	_, gotB2 := dumpState(t, targetB2)
	assertStateEqual(t, gotB2, keysWithPrefix(nil, "a", "b"))

	// 默认恢复到链尖：A+B+C。
	targetAll := t.TempDir() + "/rall"
	if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: targetAll, ChainID: chainID}); err != nil {
		t.Fatal(err)
	}
	_, gotAll := dumpState(t, targetAll)
	assertStateEqual(t, gotAll, keysWithPrefix(nil, "a", "b", "c"))

	// 恢复出的引擎可继续服务（独立目录，可读写）。
	re, err := engine.Open(engine.Options{Dir: targetAll})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := re.Put("new-key", []byte("new")); err != nil {
		t.Fatal(err)
	}
	re.Close()
}

// 场景 6：多个备份任务并发执行，各自链完整且互不污染。
func TestConcurrentBackups(t *testing.T) {
	f := newFixture(t)
	tracker := &seqTracker{}
	var counter atomic.Int64
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for w := 0; w < 2; w++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := counter.Add(1)
				key := fmt.Sprintf("k%08d", n)
				val := fmt.Sprintf("v%d", n)
				if seq, err := f.eng.Put(key, []byte(val)); err == nil {
					tracker.add(key, val, seq)
				}
			}
		}()
	}

	// 4 个并发备份任务：各自建链并追加 3 个增量段。
	const tasks = 4
	chainIDs := make([]string, tasks)
	var wg sync.WaitGroup
	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := f.mgr.FullBackup()
			if err != nil {
				t.Errorf("full backup %d: %v", i, err)
				return
			}
			chainIDs[i] = id
			for j := 0; j < 3; j++ {
				time.Sleep(5 * time.Millisecond)
				if _, err := f.mgr.IncrementalBackup(id); err != nil && err != backup.ErrNoNewData {
					t.Errorf("incr backup chain %s: %v", id, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	writers.Wait()

	// 每条链独立完整：校验通过、段区间自洽、恢复结果等于链尖状态。
	seen := map[string]bool{}
	for _, id := range chainIDs {
		if id == "" || seen[id] {
			t.Fatalf("duplicate or empty chain id %q", id)
		}
		seen[id] = true
		if err := f.mgr.ValidateChain(id); err != nil {
			t.Fatalf("validate %s: %v", id, err)
		}
		chains, _ := f.mgr.Chains()
		var tip uint64
		for _, mf := range chains {
			if mf.ChainID != id {
				continue
			}
			for i := 1; i < len(mf.Segments); i++ {
				if mf.Segments[i].FromSeq != mf.Segments[i-1].ToSeq {
					t.Fatalf("chain %s segment %d not chained", id, i)
				}
			}
			tip = mf.Segments[len(mf.Segments)-1].ToSeq
		}
		target := t.TempDir() + "/r"
		if err := f.mgr.Restore(backup.RestoreOptions{TargetDir: target, ChainID: id}); err != nil {
			t.Fatalf("restore %s: %v", id, err)
		}
		gotSeq, got := dumpState(t, target)
		if gotSeq != tip {
			t.Fatalf("chain %s restored seq %d != tip %d", id, gotSeq, tip)
		}
		assertStateEqual(t, got, tracker.stateAt(tip))
	}
}

// 增量备份在无新数据时返回 ErrNoNewData，不产生空段。
func TestIncrementalNoNewData(t *testing.T) {
	f := newFixture(t)
	f.eng.Put("x", []byte("1"))
	chainID, err := f.mgr.FullBackup()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.mgr.IncrementalBackup(chainID); err != backup.ErrNoNewData {
		t.Fatalf("want ErrNoNewData, got %v", err)
	}
	chains, _ := f.mgr.Chains()
	if len(chains[0].Segments) != 1 {
		t.Fatalf("no empty segment expected, got %+v", chains[0].Segments)
	}
}

// 验证链清单按创建时间排序（供保留策略与“最新链”语义依赖）。
func TestChainsSorted(t *testing.T) {
	f := newFixture(t)
	var ids []string
	for i := 0; i < 3; i++ {
		f.eng.Put(fmt.Sprintf("k%d", i), []byte("v"))
		id, err := f.mgr.FullBackup()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		time.Sleep(2 * time.Millisecond)
	}
	chains, _ := f.mgr.Chains()
	var got []string
	for _, mf := range chains {
		got = append(got, mf.ChainID)
	}
	sort.Strings(ids)
	if fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Fatalf("chains = %v, want sorted %v", got, ids)
	}
}
