package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Options 引擎配置。
type Options struct {
	Dir string
	// SyncWrites 为 true 时每次提交都 fsync，保证崩溃不丢已提交数据。
	SyncWrites bool
	// SnapshotThreshold：WAL 中未快照记录数超过该值时自动做快照并归档 WAL。
	// <= 0 时使用默认值。
	SnapshotThreshold int
}

const (
	walFileName     = "wal.log"
	walArchiveDir   = "wal-archive"
	defaultSnapshot = 10000
)

// Engine 是持久化 KV 引擎：WAL + Memtable + 定期快照。
//
// 一致性模型：写操作在写锁内串行提交，先追加 WAL 再应用内存状态，
// 每个提交获得单调递增的 Seq 与单调不减的提交时间。
// 崩溃后重放 WAL 恢复到最后一个完整提交（一致状态）。
type Engine struct {
	mu      sync.RWMutex
	opts    Options
	mem     *Memtable
	wal     *WAL
	lastSeq uint64
	lastTs  int64

	walBaseSeq   uint64 // 当前 wal.log 中第一条记录的 Seq
	sinceSnap    int
	walRetainSeq uint64
}

// Open 打开引擎目录，加载最新快照并重放 WAL，恢复到最后一个一致状态。
func Open(opts Options) (*Engine, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("engine: Dir is required")
	}
	if opts.SnapshotThreshold <= 0 {
		opts.SnapshotThreshold = defaultSnapshot
	}
	if err := os.MkdirAll(filepath.Join(opts.Dir, walArchiveDir), 0o755); err != nil {
		return nil, err
	}
	e := &Engine{opts: opts, mem: NewMemtable()}
	// 1. 加载最新快照
	snapPath, err := LatestSnapshot(opts.Dir)
	if err != nil {
		return nil, err
	}
	if snapPath != "" {
		seq, ts, entries, err := ReadSnapshot(snapPath)
		if err != nil {
			return nil, err
		}
		for i := range entries {
			e.mem.Apply(&entries[i])
		}
		e.lastSeq, e.lastTs = seq, ts
	}
	// 2. 重放当前 WAL（容忍崩溃造成的撕裂尾部）。
	//    快照之后崩溃时 wal.log 可能仍含已快照记录，重复应用幂等。
	walPath := filepath.Join(opts.Dir, walFileName)
	err = ReplayWAL(walPath, func(rec *Record) error {
		e.mem.Apply(rec)
		if rec.Seq > e.lastSeq {
			e.lastSeq = rec.Seq
		}
		if rec.Time > e.lastTs {
			e.lastTs = rec.Time
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.walBaseSeq = e.lastSeq + 1
	w, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}
	e.wal = w
	return e, nil
}

// Close 关闭引擎。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal != nil {
		_ = e.wal.Sync()
		return e.wal.Close()
	}
	return nil
}

// Dir 返回引擎数据目录。
func (e *Engine) Dir() string {
	return e.opts.Dir
}

// Get 读取 key 的最新已提交值。
func (e *Engine) Get(key string) ([]byte, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.mem.Get(key)
	return v, ok, nil
}

// Put 提交一次写入，返回分配的 Seq。
func (e *Engine) Put(key string, value []byte) (uint64, error) {
	return e.commit(OpPut, key, value)
}

// Delete 提交一次删除，返回分配的 Seq。
func (e *Engine) Delete(key string) (uint64, error) {
	return e.commit(OpDelete, key, nil)
}

func (e *Engine) commit(op Op, key string, value []byte) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastSeq++
	now := time.Now().UnixNano()
	if now <= e.lastTs {
		now = e.lastTs + 1
	}
	e.lastTs = now
	rec := &Record{Seq: e.lastSeq, Time: now, Op: op, Key: key, Value: value}
	if err := e.wal.Append(rec); err != nil {
		return 0, err
	}
	if e.opts.SyncWrites {
		if err := e.wal.Sync(); err != nil {
			return 0, err
		}
	}
	e.mem.Apply(rec)
	e.sinceSnap++
	if e.sinceSnap >= e.opts.SnapshotThreshold {
		if err := e.rotateLocked(); err != nil {
			return 0, err
		}
	}
	return rec.Seq, nil
}

// rotateLocked 写快照、归档当前 WAL 并开启新 WAL。调用方须持有写锁。
func (e *Engine) rotateLocked() error {
	if _, err := WriteSnapshot(e.opts.Dir, e.lastSeq, e.lastTs, e.mem.Entries()); err != nil {
		return err
	}
	if err := e.wal.Close(); err != nil {
		return err
	}
	archiveName := fmt.Sprintf("wal-%020d-%020d.log", e.walBaseSeq, e.lastSeq)
	oldPath := filepath.Join(e.opts.Dir, walFileName)
	newPath := filepath.Join(e.opts.Dir, walArchiveDir, archiveName)
	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("archive wal: %w", err)
	}
	w, err := OpenWAL(oldPath)
	if err != nil {
		return err
	}
	e.wal = w
	e.walBaseSeq = e.lastSeq + 1
	e.sinceSnap = 0
	e.cleanupLocked()
	return nil
}

// cleanupLocked 删除旧快照（仅留最新）与不再需要的已归档 WAL。
func (e *Engine) cleanupLocked() {
	ents, err := os.ReadDir(e.opts.Dir)
	if err == nil {
		latest, _ := LatestSnapshot(e.opts.Dir)
		for _, ent := range ents {
			p := filepath.Join(e.opts.Dir, ent.Name())
			if strings.HasPrefix(ent.Name(), snapshotPrefix) && p != latest {
				_ = os.Remove(p)
			}
		}
	}
	if e.walRetainSeq == 0 {
		return
	}
	for _, seg := range e.listArchivedWALLocked() {
		if seg.end <= e.walRetainSeq {
			_ = os.Remove(seg.path)
		}
	}
}

type archivedWAL struct {
	path       string
	start, end uint64
}

func (e *Engine) listArchivedWALLocked() []archivedWAL {
	dir := filepath.Join(e.opts.Dir, walArchiveDir)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []archivedWAL
	for _, ent := range ents {
		name := ent.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"), "-")
		if len(parts) != 2 {
			continue
		}
		start, err1 := strconv.ParseUint(parts[0], 10, 64)
		end, err2 := strconv.ParseUint(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, archivedWAL{path: filepath.Join(dir, name), start: start, end: end})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out
}

// LastSeq 返回当前已提交的最大 Seq。
func (e *Engine) LastSeq() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastSeq
}

// ConsistentSnapshot 返回某个已提交 Seq 时刻的完整状态（一致点）。
// 返回的 entries 恰好等于该 Seq 时刻已提交写应用后的结果，不撕裂。
func (e *Engine) ConsistentSnapshot() (seq uint64, commitTime int64, entries []Record, err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastSeq, e.lastTs, e.mem.Entries(), nil
}

// WALRecords 返回 (fromSeq, toSeq] 区间内已提交的记录，供增量备份使用。
func (e *Engine) WALRecords(fromSeq, toSeq uint64) ([]Record, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []Record
	collect := func(rec *Record) error {
		if rec.Seq > fromSeq && rec.Seq <= toSeq {
			out = append(out, *rec)
		}
		return nil
	}
	for _, seg := range e.listArchivedWALLocked() {
		if seg.end <= fromSeq || seg.start > toSeq {
			continue
		}
		if err := ReplayWAL(seg.path, collect); err != nil {
			return nil, err
		}
	}
	if err := ReplayWAL(filepath.Join(e.opts.Dir, walFileName), collect); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// SetWALRetainSeq 告知引擎：Seq <= retainSeq 的已归档 WAL 可被回收。
func (e *Engine) SetWALRetainSeq(retainSeq uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if retainSeq > e.walRetainSeq {
		e.walRetainSeq = retainSeq
	}
	e.cleanupLocked()
}
