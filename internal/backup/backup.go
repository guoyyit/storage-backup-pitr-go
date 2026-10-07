// Package backup implements full/incremental backups, PIT restore and retention.
package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// Kind identifies a backup kind.
type Kind string

const (
	KindFull        Kind = "full"
	KindIncremental Kind = "incremental"
)

// SegmentInfo mirrors the engine WAL's segment description.
type SegmentInfo struct {
	Index    uint64
	Name     string
	StartCSN uint64
	EndCSN   uint64
	Size     int64
}

// Engine is the subset of the KV engine used by the backup service.
type Engine interface {
	LastCSN() uint64
	Barrier(ctx context.Context) (engine.BarrierPoint, error)
	ReclaimThrough(ctx context.Context, idx uint64) error
	CheckpointAt(ctx context.Context, csn uint64) (string, error)
	WALDir() string
	// Dir returns the engine data directory (restore target isolation root).
	Dir() string
	// MinLiveSegmentIndex returns the index of the oldest retained segment.
	MinLiveSegmentIndex() uint64
	// SnapshotPath returns the current snapshot file, or "" if none.
	SnapshotPath() string
}

// SegmentCopier verifies and copies a sealed segment.
type SegmentCopier interface {
	Segments() []SegmentInfo
	CopySegment(ctx context.Context, idx uint64) (path string, sum string, size int64, err error)
}

// SegmentFile is one segment copy recorded in backup metadata.
type SegmentFile struct {
	Index    uint64 `json:"index"`
	Name     string `json:"name"`
	StartCSN uint64 `json:"start_csn"`
	EndCSN   uint64 `json:"end_csn"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}

// Meta is the manifest of one full or incremental backup.
type Meta struct {
	Kind       Kind          `json:"kind"`
	ChainID    string        `json:"chain_id"`
	BackupID   string        `json:"backup_id"`
	ParentID   string        `json:"parent_id,omitempty"`
	PointCSN   uint64        `json:"point_csn"`
	PointTime  time.Time     `json:"point_time"`
	CreatedAt  time.Time     `json:"created_at"`
	Segments   []SegmentFile `json:"segments"`
	FirstIndex uint64        `json:"first_index"`
	LastIndex  uint64        `json:"last_index"`
	// SnapshotSHA256 is set when the backup carries an engine snapshot.
	SnapshotSHA256 string `json:"snapshot_sha256,omitempty"`
}

// Chain is one full backup plus its ordered increments.
type Chain struct {
	ID           string
	Full         Meta
	Increments   []Meta
	TipCSN       uint64
	TipTime      time.Time
	TipLastIndex uint64
}

// Retention configures backup reclamation.
type Retention struct {
	// Count keeps the newest N chains; <= 0 means unlimited.
	Count int
	// Age keeps chains whose tip is newer than now-Age; <= 0 means unlimited.
	Age time.Duration
}

// Config configures a Service.
type Config struct {
	// Dir is the backup repository root.
	Dir string
	// Retention is applied after every full backup.
	Retention Retention
}

// FullOptions configures a full backup.
type FullOptions struct {
	// ChainID forces a specific chain id; empty generates one.
	ChainID string
}

// RestoreOptions configures a point-in-time restore.
type RestoreOptions struct {
	// ChainID selects the chain; empty selects the newest chain.
	ChainID string
	// TargetCSN > 0 restores at this exact commit (must exist in the chain).
	TargetCSN uint64
	// TargetTime non-zero restores at the newest commit <= time.
	TargetTime time.Time
	// DestDir is the independent output engine directory.
	DestDir string
}

// Service manages backup chains on top of an engine.
type Service struct {
	eng Engine
	cfg Config

	// Global mutex serializes backup/retention bookkeeping so concurrent
	// operations produce deterministic, non-overlapping chains. Heavy file
	// copies for a chain happen under that chain's lock.
	//
	// restorePins tracks chains currently being restored. Retention waits for
	// a pin to clear before deleting that chain, and a restore refuses a chain
	// that no longer exists when it acquires its pin. The map is guarded by
	// mu; restoreCond wakes retention when pins are released.
	mu          sync.Mutex
	restoreCond *sync.Cond
	restorePins map[string]int
}

// NewService creates a backup service rooted at cfg.Dir.
func NewService(_ context.Context, eng Engine, cfg Config) (*Service, error) {
	if cfg.Dir == "" {
		return nil, errors.New("backup: empty repository dir")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	s := &Service{
		eng:         eng,
		cfg:         cfg,
		restorePins: make(map[string]int),
	}
	s.restoreCond = sync.NewCond(&s.mu)
	return s, nil
}

// pinChain marks chainID as in use by a restore. Callers must hold no locks.
// It returns ErrChainUnavailable if the chain directory does not exist on
// disk: such a chain was never created or was reclaimed before this restore
// started, and no destination write may happen.
func (s *Service) pinChain(chainID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(chainPath(s.cfg.Dir, chainID)); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: chain %s was reclaimed or does not exist", ErrChainUnavailable, chainID)
		}
		return err
	}
	s.restorePins[chainID]++
	return nil
}

// unpinChain releases one restore pin and wakes retention waiting on it.
func (s *Service) unpinChain(chainID string) {
	s.mu.Lock()
	if n := s.restorePins[chainID]; n > 1 {
		s.restorePins[chainID] = n - 1
	} else {
		delete(s.restorePins, chainID)
	}
	s.restoreCond.Broadcast()
	s.mu.Unlock()
}

// chainPinnedLocked reports whether chainID is currently being restored.
func (s *Service) chainPinnedLocked(chainID string) bool {
	return s.restorePins[chainID] > 0
}
