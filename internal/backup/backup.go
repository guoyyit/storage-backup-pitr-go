// Package backup implements full/incremental backups, PIT restore and retention.
package backup

import (
	"context"
	"errors"
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
	mu sync.Mutex
}

// NewService creates a backup service rooted at cfg.Dir.
func NewService(_ context.Context, eng Engine, cfg Config) (*Service, error) {
	if cfg.Dir == "" {
		return nil, errors.New("backup: empty repository dir")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	return &Service{eng: eng, cfg: cfg}, nil
}
