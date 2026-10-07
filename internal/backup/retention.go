package backup

import (
	"context"
	"os"
)

// ApplyRetention deletes chains outside the retention policy and reclaims
// engine WAL segments no longer needed by any retained chain. It is invoked
// automatically after each full backup and can also be called directly.
func (s *Service) ApplyRetention(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyRetentionLocked(ctx)
}

func (s *Service) applyRetentionLocked(ctx context.Context) error {
	chains, err := s.listChainsLocked()
	if err != nil {
		return err
	}
	r := s.cfg.Retention
	now := nowUTC()
	retained := chains[:0]
	var dropped []Chain
	for i, c := range chains {
		expired := false
		if r.Count > 0 && i >= r.Count {
			expired = true
		}
		if r.Age > 0 && now.Sub(c.TipTime) > r.Age {
			expired = true
		}
		if expired {
			dropped = append(dropped, c)
			continue
		}
		retained = append(retained, c)
	}

	for _, c := range dropped {
		// A restore that started using this chain pinned it. Wait for the
		// restore to finish before deleting so an in-flight restore never
		// observes a partially reclaimed chain. Conversely, a restore that
		// has not pinned the chain by the time it is dropped fails with
		// ErrChainUnavailable before writing anything. Either outcome is
		// deterministic and free of partial writes.
		for s.chainPinnedLocked(c.ID) {
			s.restoreCond.Wait()
		}
		// Delete under the chain lock: the restore re-checks chain existence
		// after taking this same lock, so it can never read a half-deleted
		// chain directory.
		mu := chainLock(c.ID)
		mu.Lock()
		err := os.RemoveAll(chainPath(s.cfg.Dir, c.ID))
		if err == nil {
			err = syncDir(s.cfg.Dir)
		}
		mu.Unlock()
		if err != nil {
			return err
		}
	}

	// Low water: for every retained chain, future increments only need
	// segments newer than that chain's tip. Segments up to the smallest tip
	// index are already archived by every retained chain; the engine then
	// checkpoints its own state into a snapshot before deleting them, so
	// local crash recovery is unaffected.
	var lowWater *uint64
	for _, c := range retained {
		idx := c.TipLastIndex
		if lowWater == nil || idx < *lowWater {
			v := idx
			lowWater = &v
		}
	}
	if lowWater != nil {
		if err := s.eng.ReclaimThrough(ctx, *lowWater); err != nil {
			return err
		}
	}
	return nil
}
