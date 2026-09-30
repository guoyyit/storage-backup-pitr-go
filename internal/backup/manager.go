package backup

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// ErrNoNewData 表示自上次备份以来没有新的已提交数据。
var ErrNoNewData = errors.New("backup: no new commits since last backup")

// ErrChainNotFound 表示指定的备份链不存在。
var ErrChainNotFound = errors.New("backup: chain not found")

// Manager 管理备份链的创建、追加与枚举。
// 每次全量备份开启一条新链，增量备份追加到指定链（默认最新链）。
// 多条链相互独立，并发备份互不污染。
type Manager struct {
	mu     sync.Mutex
	eng    *engine.Engine
	dir    string
	chains map[string]*chainHandle
}

// chainHandle 持有单条链的并发控制状态：
// 增量追加与回收获取写锁，恢复获取读锁。
type chainHandle struct {
	mu sync.RWMutex
}

// NewManager 创建备份管理器，备份数据存放在 dir 下。
func NewManager(eng *engine.Engine, dir string) (*Manager, error) {
	m := &Manager{eng: eng, dir: dir, chains: make(map[string]*chainHandle)}
	if err := os.MkdirAll(filepath.Join(dir, chainsDirName), 0o755); err != nil {
		return nil, err
	}
	// 启动时加载既有链，并清理上次崩溃残留的临时目录。
	ents, err := os.ReadDir(filepath.Join(dir, chainsDirName))
	if err != nil {
		return nil, err
	}
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}
		name := ent.Name()
		chainDir := m.chainDir(name)
		if _, err := readManifest(chainDir); err != nil {
			// 无清单的目录是未完成的备份残留，删除不影响任何已完成链。
			_ = os.RemoveAll(chainDir)
			continue
		}
		m.chains[name] = &chainHandle{}
	}
	m.refreshWALRetain()
	return m, nil
}

// FullBackup 基于引擎当前一致点创建一条新链的全量备份，返回链 ID。
// 备份内容恰好等于某个已提交 Seq 时刻的状态；备份期间读写服务保持可用。
func (m *Manager) FullBackup() (string, error) {
	// 一致点：读锁内取 (seq, ts, entries)，期间写入被短暂阻塞，
	// 快照内容恰好等于该 Seq 的已提交状态。
	seq, ts, entries, err := m.eng.ConsistentSnapshot()
	if err != nil {
		return "", err
	}
	chainID := newChainID()
	tmpDir := m.chainDir(chainID + ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return "", err
	}
	snapPath, err := engine.WriteSnapshot(tmpDir, seq, ts, entries)
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}
	sum, err := fileSHA256(snapPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}
	mf := &Manifest{
		ChainID:   chainID,
		CreatedAt: time.Now(),
		Segments: []SegmentMeta{{
			Kind:     SegmentFull,
			File:     filepath.Base(snapPath),
			FromSeq:  0,
			ToSeq:    seq,
			Records:  len(entries),
			SHA256:   sum,
			TimeUnix: ts,
		}},
	}
	if err := writeManifest(tmpDir, mf); err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}
	// 清单落盘后原子改名，链才对外可见；崩溃残留的 .tmp 目录启动时清理。
	m.mu.Lock()
	finalDir := m.chainDir(chainID)
	err = os.Rename(tmpDir, finalDir)
	if err == nil {
		m.chains[chainID] = &chainHandle{}
	}
	m.mu.Unlock()
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}
	m.refreshWALRetain()
	return chainID, nil
}

// IncrementalBackup 将 (上次备份, 当前] 的变更追加为增量段。
// chainID 为空时追加到最新链；没有可用链时返回错误。
func (m *Manager) IncrementalBackup(chainID string) (SegmentMeta, error) {
	m.mu.Lock()
	if chainID == "" {
		chainID = m.latestChainIDLocked()
	}
	h, ok := m.chains[chainID]
	m.mu.Unlock()
	if !ok {
		return SegmentMeta{}, fmt.Errorf("%w: %s", ErrChainNotFound, chainID)
	}
	// 同一链上的增量串行，不同链之间互不阻塞。
	h.mu.Lock()
	defer h.mu.Unlock()

	chainDir := m.chainDir(chainID)
	mf, err := readManifest(chainDir)
	if err != nil {
		return SegmentMeta{}, err
	}
	tip := mf.Segments[len(mf.Segments)-1].ToSeq
	target := m.eng.LastSeq()
	if target <= tip {
		return SegmentMeta{}, ErrNoNewData
	}
	recs, err := m.eng.WALRecords(tip, target)
	if err != nil {
		return SegmentMeta{}, err
	}
	// 增量段必须与链尖严格衔接：记录连续覆盖 (tip, target]。
	if len(recs) != int(target-tip) {
		return SegmentMeta{}, fmt.Errorf("backup: wal gap for chain %s: need %d records in (%d,%d], got %d",
			chainID, target-tip, tip, target, len(recs))
	}
	for i, rec := range recs {
		if rec.Seq != tip+1+uint64(i) {
			return SegmentMeta{}, fmt.Errorf("backup: wal discontinuity at seq %d (expect %d)", rec.Seq, tip+1+uint64(i))
		}
	}
	segName := incrSegmentName(tip, target)
	segPath := filepath.Join(chainDir, segName)
	if err := engine.WriteSegment(segPath, recs); err != nil {
		return SegmentMeta{}, err
	}
	sum, err := fileSHA256(segPath)
	if err != nil {
		return SegmentMeta{}, err
	}
	meta := SegmentMeta{
		Kind:     SegmentIncr,
		File:     segName,
		FromSeq:  tip,
		ToSeq:    target,
		Records:  len(recs),
		SHA256:   sum,
		TimeUnix: recs[len(recs)-1].Time,
	}
	mf.Segments = append(mf.Segments, meta)
	if err := writeManifest(chainDir, mf); err != nil {
		return SegmentMeta{}, err
	}
	m.refreshWALRetain()
	return meta, nil
}

// Chains 返回当前全部链的清单。
func (m *Manager) Chains() ([]Manifest, error) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.chains))
	for id := range m.chains {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	sort.Strings(ids)
	var out []Manifest
	for _, id := range ids {
		mf, err := readManifest(m.chainDir(id))
		if err != nil {
			return nil, err
		}
		out = append(out, *mf)
	}
	return out, nil
}

// latestChainIDLocked 返回最新创建的链 ID；调用方须持有 m.mu。
func (m *Manager) latestChainIDLocked() string {
	best := ""
	for id := range m.chains {
		if id > best {
			best = id
		}
	}
	return best
}

// refreshWALRetain 根据所有链的最小链尖 Seq，通知引擎回收不再需要的归档 WAL。
func (m *Manager) refreshWALRetain() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.chains))
	for id := range m.chains {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	minTip := uint64(0)
	for i, id := range ids {
		mf, err := readManifest(m.chainDir(id))
		if err != nil {
			continue
		}
		tip := mf.Segments[len(mf.Segments)-1].ToSeq
		if i == 0 || tip < minTip {
			minTip = tip
		}
	}
	m.eng.SetWALRetainSeq(minTip)
}

func newChainID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("chain-%s-%s", time.Now().UTC().Format("20060102T150405.000000000"), hex.EncodeToString(b[:]))
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
