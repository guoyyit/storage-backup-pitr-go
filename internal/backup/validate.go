package backup

import (
	"fmt"
	"path/filepath"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// CorruptionError 定位到具体损坏段的错误。
type CorruptionError struct {
	ChainID string
	File    string
	Reason  string
}

func (e *CorruptionError) Error() string {
	return "backup segment corrupted: chain=" + e.ChainID + " file=" + e.File + " reason=" + e.Reason
}

// ValidateChain 校验指定链全部段的完整性，损坏时返回 *CorruptionError，
// 错误中带有具体损坏段文件名。校验只认真实损坏，不误伤完好数据。
func (m *Manager) ValidateChain(chainID string) error {
	m.mu.Lock()
	_, ok := m.chains[chainID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrChainNotFound, chainID)
	}
	chainDir := m.chainDir(chainID)
	mf, err := readManifest(chainDir)
	if err != nil {
		return &CorruptionError{ChainID: chainID, File: manifestName, Reason: err.Error()}
	}
	if len(mf.Segments) == 0 || mf.Segments[0].Kind != SegmentFull {
		return &CorruptionError{ChainID: chainID, File: manifestName, Reason: "chain must start with a full segment"}
	}
	for i, seg := range mf.Segments {
		if i > 0 {
			prev := mf.Segments[i-1]
			if seg.Kind != SegmentIncr || seg.FromSeq != prev.ToSeq || seg.ToSeq <= seg.FromSeq {
				return &CorruptionError{ChainID: chainID, File: seg.File,
					Reason: fmt.Sprintf("segment does not chain from seq %d", prev.ToSeq)}
			}
		}
		if seg.Kind == SegmentFull {
			if _, _, _, err := loadFullSegment(chainID, chainDir, seg); err != nil {
				return err
			}
		} else {
			if _, err := loadIncrSegment(chainID, chainDir, seg); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadFullSegment 校验并读取全量段：先核 sha256，再严格解析并交叉核对元数据。
func loadFullSegment(chainID, chainDir string, meta SegmentMeta) (uint64, int64, []engine.Record, error) {
	path := filepath.Join(chainDir, meta.File)
	if err := checkSHA256(path, meta); err != nil {
		return 0, 0, nil, corrupt(chainID, meta, err)
	}
	seq, ts, entries, err := engine.ReadSnapshot(path)
	if err != nil {
		return 0, 0, nil, corrupt(chainID, meta, err)
	}
	if seq != meta.ToSeq {
		return 0, 0, nil, corrupt(chainID, meta,
			fmt.Errorf("snapshot seq %d != meta to_seq %d", seq, meta.ToSeq))
	}
	if len(entries) != meta.Records {
		return 0, 0, nil, corrupt(chainID, meta,
			fmt.Errorf("snapshot entries %d != meta records %d", len(entries), meta.Records))
	}
	return seq, ts, entries, nil
}

// loadIncrSegment 校验并读取增量段：sha256 + 严格帧解析 + 区间连续性核对。
func loadIncrSegment(chainID, chainDir string, meta SegmentMeta) ([]engine.Record, error) {
	path := filepath.Join(chainDir, meta.File)
	if err := checkSHA256(path, meta); err != nil {
		return nil, corrupt(chainID, meta, err)
	}
	recs, err := engine.ReadAllRecords(path)
	if err != nil {
		return nil, corrupt(chainID, meta, err)
	}
	if len(recs) != meta.Records {
		return nil, corrupt(chainID, meta,
			fmt.Errorf("segment records %d != meta records %d", len(recs), meta.Records))
	}
	for i, rec := range recs {
		want := meta.FromSeq + 1 + uint64(i)
		if rec.Seq != want {
			return nil, corrupt(chainID, meta,
				fmt.Errorf("record seq %d out of order, want %d", rec.Seq, want))
		}
	}
	if len(recs) > 0 && recs[len(recs)-1].Seq != meta.ToSeq {
		return nil, corrupt(chainID, meta,
			fmt.Errorf("last record seq %d != meta to_seq %d", recs[len(recs)-1].Seq, meta.ToSeq))
	}
	return recs, nil
}

func checkSHA256(path string, meta SegmentMeta) error {
	sum, err := fileSHA256(path)
	if err != nil {
		return err
	}
	if sum != meta.SHA256 {
		return fmt.Errorf("sha256 mismatch: manifest=%s actual=%s", meta.SHA256, sum)
	}
	return nil
}

func corrupt(chainID string, meta SegmentMeta, err error) error {
	return &CorruptionError{ChainID: chainID, File: meta.File, Reason: err.Error()}
}
