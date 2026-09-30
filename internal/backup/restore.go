package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

const restoreMarkerName = "RESTORED.json"

type restoreMarker struct {
	ChainID     string `json:"chain_id"`
	ToSeq       uint64 `json:"to_seq"`
	PointInTime int64  `json:"point_in_time"`
}

// Restore 将指定链恢复到 opts.TargetDir（独立目录）。
//
// 恢复语义：结果恰好等于目标时间点（PointInTime / ToSeq 取更严者）
// 已提交的状态，该点之后的写入不出现在恢复结果中。
// 过程可中断：全部内容先写入临时目录，最后原子 rename 到 TargetDir，
// 中断后重新执行的结果与一次成功恢复完全一致，不残留部分写入。
// 恢复不触碰运行中的引擎、源备份与备份链（只读）。
func (m *Manager) Restore(opts RestoreOptions) error {
	if opts.TargetDir == "" {
		return fmt.Errorf("restore: TargetDir is required")
	}
	targetAbs, err := filepath.Abs(opts.TargetDir)
	if err != nil {
		return err
	}
	// 安全检查：目标目录必须独立，不得覆盖引擎目录或备份目录。
	if engAbs, _ := filepath.Abs(m.eng.Dir()); targetAbs == engAbs {
		return fmt.Errorf("restore: target dir must differ from running engine dir")
	}
	if bakAbs, _ := filepath.Abs(m.dir); targetAbs == bakAbs || strings.HasPrefix(targetAbs, bakAbs+string(os.PathSeparator)) {
		return fmt.Errorf("restore: target dir must not be inside backup dir")
	}

	m.mu.Lock()
	chainID := opts.ChainID
	if chainID == "" {
		chainID = m.latestChainIDLocked()
	}
	h, ok := m.chains[chainID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrChainNotFound, chainID)
	}
	// 读锁挡住回收：恢复期间链目录不会被删除。
	h.mu.RLock()
	defer h.mu.RUnlock()

	chainDir := m.chainDir(chainID)
	mf, err := readManifest(chainDir)
	if err != nil {
		return err
	}
	if len(mf.Segments) == 0 {
		return &CorruptionError{ChainID: chainID, File: manifestName, Reason: "empty chain"}
	}

	var maxSeq uint64 = ^uint64(0)
	if opts.ToSeq > 0 {
		maxSeq = opts.ToSeq
	}
	var maxTime int64 = 1<<63 - 1
	if !opts.PointInTime.IsZero() {
		maxTime = opts.PointInTime.UnixNano()
	}

	// 全量段：恢复基线。目标点必须不早于链基线。
	fullMeta := mf.Segments[0]
	fullSeq, fullTs, entries, err := loadFullSegment(chainID, chainDir, fullMeta)
	if err != nil {
		return err
	}
	if fullSeq > maxSeq || fullTs > maxTime {
		return fmt.Errorf("restore: target point is before chain %s base (seq=%d time=%d)",
			chainID, fullSeq, fullTs)
	}

	// 按序回放增量段，严格截断在目标点：回放不得超过该点。
	var replay []engine.Record
	expectedSeq := fullSeq
	for _, seg := range mf.Segments[1:] {
		if seg.FromSeq >= maxSeq {
			break
		}
		recs, err := loadIncrSegment(chainID, chainDir, seg)
		if err != nil {
			return err
		}
		for i := range recs {
			rec := &recs[i]
			if rec.Seq > maxSeq || rec.Time > maxTime {
				break
			}
			replay = append(replay, *rec)
			expectedSeq = rec.Seq
		}
		if expectedSeq < seg.ToSeq {
			break // 已在段内截断，后续段全部越界
		}
	}

	// 写入临时目录，完成后原子 rename；中断只残留临时目录，重试时清除。
	tmpDir := targetAbs + ".restore-tmp"
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	if _, err := engine.WriteSnapshot(tmpDir, fullSeq, fullTs, entries); err != nil {
		return err
	}
	if err := engine.WriteSegment(filepath.Join(tmpDir, "wal.log"), replay); err != nil {
		return err
	}
	marker := restoreMarker{ChainID: chainID, ToSeq: expectedSeq}
	if !opts.PointInTime.IsZero() {
		marker.PointInTime = opts.PointInTime.UnixNano()
	}
	markerData, _ := json.Marshal(marker)
	if err := os.WriteFile(filepath.Join(tmpDir, restoreMarkerName), markerData, 0o644); err != nil {
		return err
	}
	// 自检：以引擎方式打开恢复结果，确认可加载且 Seq 边界正确。
	check, err := engine.Open(engine.Options{Dir: tmpDir})
	if err != nil {
		return fmt.Errorf("restore: self-check open failed: %w", err)
	}
	if got := check.LastSeq(); got != expectedSeq {
		check.Close()
		return fmt.Errorf("restore: self-check seq %d != expected %d", got, expectedSeq)
	}
	if err := check.Close(); err != nil {
		return err
	}

	if _, err := os.Stat(targetAbs); err == nil {
		// 目标已存在：若是同一恢复请求的产物则幂等成功，否则拒绝覆盖。
		if existing, err := os.ReadFile(filepath.Join(targetAbs, restoreMarkerName)); err == nil {
			var em restoreMarker
			if json.Unmarshal(existing, &em) == nil && em == marker {
				return os.RemoveAll(tmpDir)
			}
		}
		os.RemoveAll(tmpDir)
		return fmt.Errorf("restore: target dir %s already exists with different content", targetAbs)
	} else if !errors.Is(err, os.ErrNotExist) {
		os.RemoveAll(tmpDir)
		return err
	}
	return os.Rename(tmpDir, targetAbs)
}
