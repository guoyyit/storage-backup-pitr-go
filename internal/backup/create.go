package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// chainLocks serializes heavy work within one chain while allowing different
// chains to back up concurrently.
var chainLocks sync.Map // chainID -> *sync.Mutex

func chainLock(chainID string) *sync.Mutex {
	v, _ := chainLocks.LoadOrStore(chainID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// FullBackup creates a consistent full backup that starts a new chain. The
// point is the engine's latest committed state at a WAL barrier; frames
// committed afterwards go into the new active segment and are excluded.
func (s *Service) FullBackup(ctx context.Context, opts FullOptions) (*Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chainID := opts.ChainID
	if chainID == "" {
		chainID = fmt.Sprintf("chain-%s", newBackupID(time.Now().UTC()))
	}
	backupID := "full-" + newBackupID(time.Now().UTC())
	mu := chainLock(chainID)

	mu.Lock()
	defer mu.Unlock()

	bp, err := s.eng.Barrier(ctx)
	if err != nil {
		return nil, err
	}
	if bp.CSN == 0 {
		return nil, fmt.Errorf("backup: refusing to back up before any commit")
	}

	ops, err := openWalOps(s.eng.WALDir())
	if err != nil {
		return nil, err
	}
	defer ops.close()

	// Copy every live sealed segment up to the barrier. When no segments
	// have been reclaimed these start at 0 and cover all earlier points.
	segs, files, err := copySegmentRange(ctx, ops, s.cfg.Dir, chainID, backupID, 0, bp.SealedSegmentIndex)
	if err != nil {
		return nil, err
	}
	m := Meta{
		Kind:       KindFull,
		ChainID:    chainID,
		BackupID:   backupID,
		PointCSN:   bp.CSN,
		PointTime:  bp.CommittedAt,
		CreatedAt:  time.Now().UTC(),
		Segments:   files,
		FirstIndex: firstIndex(segs),
		LastIndex:  bp.SealedSegmentIndex,
	}
	// When the oldest live segment is not 0, earlier segments were reclaimed
	// and the full backup needs a snapshot base. Take it exactly at the
	// barrier CSN so concurrent post-barrier commits are excluded.
	minIdx := s.eng.MinLiveSegmentIndex()
	if minIdx > 0 {
		snapSrc, cerr := s.eng.CheckpointAt(ctx, bp.CSN)
		if cerr != nil {
			return nil, cerr
		}
		sum, _, cerr := copyFileVerified(snapSrc, filepath.Join(s.cfg.Dir, chainID, backupID, "snapshot.gob"))
		if cerr != nil {
			return nil, cerr
		}
		m.SnapshotSHA256 = sum
		_ = os.Remove(snapSrc)
	}
	if err := writeMetaAtomically(s.cfg.Dir, m); err != nil {
		return nil, err
	}

	if err := s.applyRetentionLocked(ctx); err != nil {
		return &m, err
	}
	return &m, nil
}

// IncrementalBackup appends an increment containing segments sealed since the
// chain's previous backup up to a fresh barrier point.
func (s *Service) IncrementalBackup(ctx context.Context, chainID string) (*Meta, error) {
	if chainID == "" {
		return nil, fmt.Errorf("backup: chain id required for incremental backup")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mu := chainLock(chainID)

	mu.Lock()
	defer mu.Unlock()

	chain, err := loadChain(s.cfg.Dir, chainID)
	if err != nil {
		return nil, err
	}
	bp, err := s.eng.Barrier(ctx)
	if err != nil {
		return nil, err
	}
	prevLast := chain.TipLastIndex
	if bp.SealedSegmentIndex <= prevLast {
		return nil, fmt.Errorf("backup: no new committed segments since %s (tip index %d)",
			chain.Full.BackupID, prevLast)
	}
	if bp.CSN <= chain.TipCSN {
		return nil, fmt.Errorf("backup: no new commits since chain tip CSN %d", chain.TipCSN)
	}

	ops, err := openWalOps(s.eng.WALDir())
	if err != nil {
		return nil, err
	}
	defer ops.close()

	backupID := "inc-" + newBackupID(time.Now().UTC())
	segs, files, err := copySegmentRange(ctx, ops, s.cfg.Dir, chainID, backupID, prevLast+1, bp.SealedSegmentIndex)
	if err != nil {
		return nil, err
	}
	tip := chain.tipMeta()
	m := Meta{
		Kind:       KindIncremental,
		ChainID:    chainID,
		BackupID:   backupID,
		ParentID:   tip.BackupID,
		PointCSN:   bp.CSN,
		PointTime:  bp.CommittedAt,
		CreatedAt:  time.Now().UTC(),
		Segments:   files,
		FirstIndex: firstIndex(segs),
		LastIndex:  bp.SealedSegmentIndex,
	}
	if err := writeMetaAtomically(s.cfg.Dir, m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c Chain) tipMeta() Meta {
	if len(c.Increments) > 0 {
		return c.Increments[len(c.Increments)-1]
	}
	return c.Full
}

func firstIndex(segs []SegmentInfo) uint64 {
	if len(segs) == 0 {
		return 0
	}
	return segs[0].Index
}

// copySegmentRange verifies and copies the live sealed segments with index
// in [from, to] into the backup directory and records their checksums. The
// live segment set may have gaps because older segments were snapshot-
// reclaimed; only segments still present are copied.
func copySegmentRange(
	ctx context.Context,
	ops *walOps,
	root, chainID, backupID string,
	from, to uint64,
) ([]SegmentInfo, []SegmentFile, error) {
	live := ops.Segments()
	var files []SegmentFile
	var copied []SegmentInfo
	for _, seg := range live {
		if seg.Index < from || seg.Index > to || seg.Size == 0 {
			continue
		}
		dst := segmentBackupPath(root, chainID, backupID, seg.Name)
		sum, n, err := ops.CopySegment(ctx, seg.Index, dst)
		if err != nil {
			return nil, nil, err
		}
		if n != seg.Size {
			return nil, nil, fmt.Errorf("backup: copied %d bytes of segment %d, size changed to %d", n, seg.Index, seg.Size)
		}
		copied = append(copied, seg)
		files = append(files, SegmentFile{
			Index: seg.Index, Name: seg.Name,
			StartCSN: seg.StartCSN, EndCSN: seg.EndCSN,
			Size: seg.Size, SHA256: sum,
		})
	}
	return copied, files, nil
}
