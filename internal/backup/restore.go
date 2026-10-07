package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

const restoreStagingSuffix = ".restore-staging"

// RestoreTo rebuilds an independent engine directory at an arbitrary commit
// point. The source repository and any running engine are never modified.
// It is safe to retry after interruption: each attempt starts from a fresh
// staging directory and atomically replaces the destination at the end, so
// a successful retry is identical to a single successful run.
func (s *Service) RestoreTo(ctx context.Context, opts RestoreOptions) (*Meta, error) {
	if opts.DestDir == "" {
		return nil, fmt.Errorf("backup: restore destination dir required")
	}
	if opts.TargetCSN == 0 && opts.TargetTime.IsZero() {
		return nil, fmt.Errorf("backup: restore target (CSN or time) required")
	}

	// Isolate the destination from the running engine and the repository
	// before anything is created, removed or renamed. Relative paths,
	// duplicate separators and symlinks are all resolved to their real target.
	if err := validateRestoreTarget(opts.DestDir, s.eng.Dir(), s.cfg.Dir); err != nil {
		return nil, err
	}

	chainID := opts.ChainID
	if chainID == "" {
		s.mu.Lock()
		id, err := s.newestChainIDLocked()
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		chainID = id
	}

	// Pin the chain while we restore it: retention blocks until this pin is
	// released, and a chain reclaimed before we pin makes us fail here, before
	// the destination (or its staging directory) is touched.
	if err := s.pinChain(chainID); err != nil {
		return nil, err
	}
	defer s.unpinChain(chainID)

	chainLock(chainID).Lock()
	defer chainLock(chainID).Unlock()

	// Re-check existence under the chain lock. Retention deletes chains while
	// holding this lock, so this either observes the chain still present or a
	// definitive ErrChainUnavailable, never a half-deleted directory.
	if _, err := os.Stat(chainPath(s.cfg.Dir, chainID)); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: chain %s was reclaimed before restore started", ErrChainUnavailable, chainID)
		}
		return nil, err
	}

	chain, err := loadChain(s.cfg.Dir, chainID)
	if err != nil {
		return nil, err
	}

	// Choose the prefix of the chain needed to reach the target.
	metas, target, err := selectRestorePrefix(s.cfg.Dir, chain, opts)
	if err != nil {
		return nil, err
	}

	// Verify every source segment and snapshot up front (nothing is written
	// to the destination until all required data is proven intact).
	for _, m := range metas {
		if err := verifyMetaSegments(s.cfg.Dir, m); err != nil {
			return nil, err
		}
		if m.Kind == KindFull && m.SnapshotSHA256 != "" {
			if err := verifyBackupSnapshot(s.cfg.Dir, m); err != nil {
				return nil, err
			}
		}
	}

	staging := opts.DestDir + restoreStagingSuffix
	// Retry case: discard any partial staging from an interrupted attempt.
	if err := os.RemoveAll(staging); err != nil {
		return nil, err
	}
	walStaging := filepath.Join(staging, engine.DataDir)
	if err := os.MkdirAll(walStaging, 0o755); err != nil {
		return nil, err
	}

	// Snapshot from the full backup, if present.
	var snapshotCSN uint64
	full := metas[0]
	if full.SnapshotSHA256 != "" {
		src := filepath.Join(backupDir(s.cfg.Dir, full.ChainID, full.BackupID), engine.SnapshotName)
		dst := filepath.Join(staging, engine.SnapshotName)
		if err := copyRaw(src, dst); err != nil {
			return nil, err
		}
		snapshotCSN, err = snapshotLastCSN(dst)
		if err != nil {
			return nil, err
		}
	}

	if snapshotCSN > target {
		return nil, fmt.Errorf("backup: target CSN %d predates the full backup's snapshot base CSN %d in chain %s",
			target, snapshotCSN, chainID)
	}

	// Materialize segments in order, filtering frames with CSN <= target.
	segments, err := mergedSegmentFiles(metas)
	if err != nil {
		return nil, err
	}
	var reachedCSN uint64
	var reachedAt time.Time
	var maxOutIdx int64 = -1
	for _, seg := range segments {
		includeAll := seg.EndCSN <= target
		if !includeAll && seg.StartCSN > target {
			continue
		}
		// Preserve the source segment index so the restored WAL ordering is
		// identical to the original and no segment collides with the fresh
		// active segment created afterwards.
		written, lastCSN, lastAt, err := writeFilteredSegment(ctx, s.cfg.Dir, seg, walStaging, seg.Index, target)
		if err != nil {
			return nil, err
		}
		if !written {
			continue
		}
		if int64(seg.Index) > maxOutIdx {
			maxOutIdx = int64(seg.Index)
		}
		if lastCSN > reachedCSN {
			reachedCSN = lastCSN
			reachedAt = lastAt
		}
	}
	// With a snapshot and fully filtered-out boundary segment there may be no
	// segment files; ensure at least an empty active segment exists.
	activeIdx := uint64(maxOutIdx + 1)
	if maxOutIdx < 0 {
		activeIdx = 0
	}
	if err := writeEmptySegment(walStaging, activeIdx); err != nil {
		return nil, err
	}

	if reachedCSN != target {
		return nil, fmt.Errorf("backup: target commit CSN %d not found in chain %s (reached %d)",
			target, chainID, reachedCSN)
	}

	// Open the staging engine: this strictly verifies all frames and the
	// snapshot, proving the restored state is exactly the target point.
	restored, err := engine.Open(ctx, staging)
	if err != nil {
		return nil, fmt.Errorf("backup: restored engine failed verification: %w", err)
	}
	if restored.LastCSN() != target {
		restored.Close()
		return nil, fmt.Errorf("backup: restored CSN %d != target %d", restored.LastCSN(), target)
	}
	if err := restored.Close(); err != nil {
		return nil, err
	}

	if err := atomicReplaceDir(opts.DestDir, staging); err != nil {
		return nil, err
	}

	_ = reachedAt
	return &Meta{
		Kind:      KindFull,
		ChainID:   chainID,
		PointCSN:  target,
		PointTime: reachedAt,
	}, nil
}

// selectRestorePrefix returns the backups up to and including the one that
// contains the target commit, plus the resolved target CSN.
func selectRestorePrefix(root string, chain Chain, opts RestoreOptions) ([]Meta, uint64, error) {
	all := append([]Meta{chain.Full}, chain.Increments...)
	target := opts.TargetCSN
	if target != 0 {
		if target > chain.TipCSN {
			return nil, 0, fmt.Errorf("backup: target CSN %d is ahead of chain tip %d", target, chain.TipCSN)
		}
	} else {
		targetCSN, err := resolveTimeTarget(root, chain, opts.TargetTime)
		if err != nil {
			return nil, 0, err
		}
		target = targetCSN
	}
	var out []Meta
	for _, m := range all {
		out = append(out, m)
		if m.PointCSN >= target {
			break
		}
	}
	if len(out) == 0 || out[len(out)-1].PointCSN < target {
		return nil, 0, fmt.Errorf("backup: target CSN %d not contained in any backup of chain %s", target, chain.ID)
	}
	return out, target, nil
}

// resolveTimeTarget finds the highest committed CSN whose commit time <= t by
// scanning every frame in the chain segments.
func resolveTimeTarget(root string, chain Chain, t time.Time) (uint64, error) {
	all := append([]Meta{chain.Full}, chain.Increments...)
	refs, err := mergedSegmentFiles(all)
	if err != nil {
		return 0, err
	}
	var found uint64
	var foundAt time.Time
	for _, ref := range refs {
		p := filepath.Join(root, ref.chainID, ref.backupID, DataDir, ref.Name)
		err := scanStoredSegment(p, ref.Name, func(csn uint64, at time.Time) error {
			if !at.After(t) && (csn > found || (csn == found && at.After(foundAt))) {
				found = csn
				foundAt = at
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	if found == 0 {
		return 0, fmt.Errorf("backup: no committed point <= %s in chain %s", t.UTC().Format(time.RFC3339Nano), chain.ID)
	}
	return found, nil
}
