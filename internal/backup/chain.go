package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// loadChain builds one chain from disk and verifies manifest linkage.
func loadChain(root, chainID string) (Chain, error) {
	metas, err := listChain(root, chainID)
	if err != nil {
		if os.IsNotExist(err) {
			return Chain{}, fmt.Errorf("%w: %s", ErrChainNotFound, chainID)
		}
		return Chain{}, err
	}
	if len(metas) == 0 {
		return Chain{}, fmt.Errorf("%w: chain %s is empty", ErrCorruptBackup, chainID)
	}
	c := Chain{ID: chainID}
	var parent string
	for i, m := range metas {
		switch {
		case i == 0:
			if m.Kind != KindFull {
				return Chain{}, fmt.Errorf("%w: chain %s starts with %s backup", ErrCorruptBackup, chainID, m.Kind)
			}
			c.Full = m
		default:
			if m.Kind != KindIncremental {
				return Chain{}, fmt.Errorf("%w: chain %s has %s backup after start", ErrCorruptBackup, chainID, m.Kind)
			}
			if m.ParentID != parent {
				return Chain{}, fmt.Errorf("%w: chain %s broken parent linkage at %s", ErrCorruptBackup, chainID, m.BackupID)
			}
			c.Increments = append(c.Increments, m)
		}
		parent = m.BackupID
	}
	tip := metas[len(metas)-1]
	c.TipCSN = tip.PointCSN
	c.TipTime = tip.PointTime
	c.TipLastIndex = tip.LastIndex
	return c, nil
}

// ListChains returns all chains ordered by tip time, newest first.
func (s *Service) ListChains(_ context.Context) ([]Chain, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listChainsLocked()
}

func (s *Service) listChainsLocked() ([]Chain, error) {
	ids, err := listChainIDs(s.cfg.Dir)
	if err != nil {
		return nil, err
	}
	chains := make([]Chain, 0, len(ids))
	for _, id := range ids {
		c, err := loadChain(s.cfg.Dir, id)
		if err != nil {
			return nil, err
		}
		chains = append(chains, c)
	}
	sort.Slice(chains, func(i, j int) bool {
		return chains[i].TipTime.After(chains[j].TipTime)
	})
	return chains, nil
}

// newestChainID returns the id of the chain with the latest tip.
func (s *Service) newestChainIDLocked() (string, error) {
	chains, err := s.listChainsLocked()
	if err != nil {
		return "", err
	}
	if len(chains) == 0 {
		return "", fmt.Errorf("%w: no chains exist", ErrChainNotFound)
	}
	return chains[0].ID, nil
}

// verifyMetaSegments checks every segment file referenced by m: existence,
// size and SHA-256, plus strict CRC framing. The error names the bad segment.
func verifyMetaSegments(root string, m Meta) error {
	dir := backupDir(root, m.ChainID, m.BackupID)
	for _, seg := range m.Segments {
		p := filepath.Join(dir, DataDir, seg.Name)
		st, err := os.Stat(p)
		if err != nil {
			return segmentError(m, seg.Name, "stat", err)
		}
		if st.Size() != seg.Size {
			return segmentError(m, seg.Name, "size",
				fmt.Errorf("size %d != manifest %d", st.Size(), seg.Size))
		}
		sum, n, err := hashFile(p)
		if err != nil {
			return segmentError(m, seg.Name, "hash", err)
		}
		if n != seg.Size || sum != seg.SHA256 {
			return segmentError(m, seg.Name, "checksum",
				fmt.Errorf("sha256 %s != manifest %s", sum, seg.SHA256))
		}
		if err := verifyBackupSegment(p, seg.Name); err != nil {
			return segmentError(m, seg.Name, "framing", err)
		}
	}
	return nil
}

func segmentError(m Meta, name, op string, err error) error {
	return fmt.Errorf("%w: chain=%s backup=%s segment=%s op=%s: %v",
		ErrCorruptBackup, m.ChainID, m.BackupID, name, op, err)
}
