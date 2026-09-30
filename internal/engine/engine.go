// Package engine implements the persistent key/value store.
package engine

import (
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/wal"
)

const (
	// DataDir is the subdirectory holding WAL segments.
	DataDir = "wal"
	// SnapshotName is the serialized consistent-state file name.
	SnapshotName = "snapshot.gob"

	checkpointSnapshotName = "backup-snapshot.gob"

	snapshotName      = SnapshotName
	snapshotTmpSuffix = ".tmp"

	snapshotMagic = "KVSNAP01"
)

var (
	// ErrCorruptSnapshot means the on-disk snapshot failed its checksum.
	ErrCorruptSnapshot = errors.New("engine: corrupt snapshot")
)

// snapshotData is the serialized consistent state at LastCSN.
type snapshotData struct {
	LastCSN uint64
	// Data maps key -> value; keys present are live.
	Data map[string]string
}

// CommitInfo describes a committed mutation.
type CommitInfo struct {
	CSN         uint64
	CommittedAt time.Time
}

// BarrierPoint is a durable consistency point.
type BarrierPoint struct {
	CSN                uint64
	CommittedAt        time.Time
	SealedSegmentIndex uint64 // highest segment frozen at the barrier
	ActiveIndex        uint64 // next segment (may receive later writes)
}

// Engine is a persistent key/value store backed by a CRC-protected WAL.
type Engine struct {
	dir string
	log *wal.WAL

	mu       sync.RWMutex
	data     map[string]string
	lastCSN  uint64
	lastTime time.Time
	closed   bool
}

// SnapshotError locates snapshot checksum/framing failures.
type SnapshotError struct {
	Op  string
	Err error
}

func (e *SnapshotError) Error() string {
	return fmt.Sprintf("engine: %s snapshot: %v", e.Op, e.Err)
}
func (e *SnapshotError) Unwrap() error { return e.Err }

// Open opens (creating if needed) an engine in dir.
func Open(ctx context.Context, dir string) (*Engine, error) {
	if dir == "" {
		return nil, errors.New("engine: empty dir")
	}
	if err := os.MkdirAll(filepath.Join(dir, DataDir), 0o755); err != nil {
		return nil, err
	}
	snap, snapCSN, err := loadSnapshot(dir)
	if err != nil {
		return nil, err
	}
	log, err := wal.Open(wal.Options{Dir: filepath.Join(dir, DataDir)})
	if err != nil {
		return nil, err
	}
	e := &Engine{
		dir:     dir,
		log:     log,
		data:    snap,
		lastCSN: snapCSN,
	}
	if err := e.replay(ctx, snapCSN); err != nil {
		log.Close()
		return nil, err
	}
	return e, nil
}

// replay applies every frame with CSN > afterCSN, in segment/frame order.
func (e *Engine) replay(ctx context.Context, afterCSN uint64) error {
	var lastTime time.Time
	for _, seg := range e.log.Segments() {
		err := e.log.ReadSegment(ctx, seg.Index, func(fr wal.Frame) error {
			if fr.CSN <= afterCSN {
				return nil
			}
			e.apply(fr)
			lastTime = fr.WrittenAt
			return nil
		})
		if err != nil {
			return err
		}
	}
	e.lastTime = lastTime
	return nil
}

func (e *Engine) apply(fr wal.Frame) {
	if e.data == nil {
		e.data = make(map[string]string)
	}
	switch fr.Type {
	case wal.FramePut:
		e.data[fr.Key] = string(fr.Value)
	case wal.FrameDelete:
		delete(e.data, fr.Key)
	}
	if fr.CSN > e.lastCSN {
		e.lastCSN = fr.CSN
	}
}

// commit allocates the next CSN, appends and fsyncs the frame.
func (e *Engine) commit(ft wal.FrameType, key, value string) (CommitInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return CommitInfo{}, errors.New("engine: closed")
	}
	info := CommitInfo{CSN: e.lastCSN + 1, CommittedAt: time.Now().UTC()}
	fr := wal.Frame{
		CSN:       info.CSN,
		Type:      ft,
		Key:       key,
		Value:     []byte(value),
		WrittenAt: info.CommittedAt,
	}
	if err := e.log.Append(context.Background(), fr); err != nil {
		return CommitInfo{}, err
	}
	e.apply(fr)
	e.lastTime = info.CommittedAt
	return info, nil
}

// Put commits a write and returns its commit info.
func (e *Engine) Put(_ context.Context, key, value string) (CommitInfo, error) {
	return e.commit(wal.FramePut, key, value)
}

// Delete commits a deletion.
func (e *Engine) Delete(_ context.Context, key string) (CommitInfo, error) {
	return e.commit(wal.FrameDelete, key, "")
}

// Get returns the committed value of a key.
func (e *Engine) Get(_ context.Context, key string) (string, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.data[key]
	return v, ok, nil
}

// LastCSN returns the latest committed sequence number.
func (e *Engine) LastCSN() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastCSN
}

// WALDir returns the directory holding segment files.
func (e *Engine) WALDir() string { return filepath.Join(e.dir, DataDir) }

// MinLiveSegmentIndex returns the oldest live WAL segment index.
func (e *Engine) MinLiveSegmentIndex() uint64 {
	segs := e.log.Segments()
	if len(segs) == 0 {
		return 0
	}
	return segs[0].Index
}

// SnapshotPath returns the snapshot file path if one exists, else "".
func (e *Engine) SnapshotPath() string {
	p := filepath.Join(e.dir, SnapshotName)
	if _, err := os.Stat(p); err == nil {
		return p
	}
	return ""
}

// Dir returns the engine data directory.
func (e *Engine) Dir() string { return e.dir }

// Barrier seals the WAL at the latest commit and returns a consistency point.
// Concurrent writes that commit after the barrier land in the new active
// segment and can never appear in a backup taken at the point.
func (e *Engine) Barrier(ctx context.Context) (BarrierPoint, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	csn := e.lastCSN
	at := e.lastTime
	active, err := e.log.Rotate(ctx)
	if err != nil {
		return BarrierPoint{}, err
	}
	return BarrierPoint{
		CSN:                csn,
		CommittedAt:        at,
		SealedSegmentIndex: active - 1,
		ActiveIndex:        active,
	}, nil
}

// ReclaimThrough durably snapshots the current committed state and removes
// segment files with index <= idx. The snapshot is atomically replaced and
// fsynced before any segment is deleted, so a crash is always recoverable.
func (e *Engine) ReclaimThrough(ctx context.Context, idx uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.reclaimLocked(ctx, idx)
}

func (e *Engine) reclaimLocked(_ context.Context, idx uint64) error {
	snap := snapshotData{LastCSN: e.lastCSN, Data: make(map[string]string, len(e.data))}
	for k, v := range e.data {
		snap.Data[k] = v
	}
	if err := writeSnapshot(e.dir, snap); err != nil {
		return err
	}
	return e.log.RemoveSegmentsBeforeOrEqual(context.Background(), idx)
}

// CheckpointAt writes a snapshot of the state exactly at CSN csn by replaying
// frames up to csn from the live WAL, independent of newer concurrent commits.
// Backup calls it with the barrier CSN so the embedded snapshot matches the
// consistency point even while writes continue.
func (e *Engine) CheckpointAt(ctx context.Context, csn uint64) (string, error) {
	e.mu.RLock()
	current := e.lastCSN
	e.mu.RUnlock()
	if csn > current {
		return "", fmt.Errorf("engine: checkpoint CSN %d is ahead of committed %d", csn, current)
	}
	data, baseCSN, err := loadSnapshot(e.dir)
	if err != nil {
		return "", err
	}
	if baseCSN > csn {
		return "", fmt.Errorf("engine: existing snapshot CSN %d is newer than checkpoint target %d", baseCSN, csn)
	}
	lastCSN := baseCSN
	for _, seg := range e.log.Segments() {
		err := e.log.ReadSegment(ctx, seg.Index, func(fr wal.Frame) error {
			if fr.CSN > baseCSN && fr.CSN <= csn {
				switch fr.Type {
				case wal.FramePut:
					data[fr.Key] = string(fr.Value)
				case wal.FrameDelete:
					delete(data, fr.Key)
				}
				if fr.CSN > lastCSN {
					lastCSN = fr.CSN
				}
			}
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	if lastCSN != csn {
		return "", fmt.Errorf("engine: checkpoint reached CSN %d, want %d", lastCSN, csn)
	}
	snap := snapshotData{LastCSN: csn, Data: data}
	if err := writeSnapshotAt(e.dir, checkpointSnapshotName, snap); err != nil {
		return "", err
	}
	return filepath.Join(e.dir, checkpointSnapshotName), nil
}

// Close flushes and closes the engine.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.log.Close()
}

func snapshotPath(dir string) string { return filepath.Join(dir, snapshotName) }

// writeSnapshot writes: magic(8) | uint64 csn | gob(map) | uint32 crc32(all).
func writeSnapshotAt(dir, name string, snap snapshotData) error {
	return writeSnapshotNamed(dir, name, snap)
}

func writeSnapshot(dir string, snap snapshotData) error {
	return writeSnapshotNamed(dir, snapshotName, snap)
}

func writeSnapshotNamed(dir, name string, snap snapshotData) error {
	pr, pw := io.Pipe()
	go func() {
		enc := gob.NewEncoder(pw)
		pw.CloseWithError(enc.Encode(snap.Data))
	}()
	mapBytes, err := io.ReadAll(pr)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(snapshotMagic)+8+len(mapBytes)+4)
	buf = append(buf, snapshotMagic...)
	var csnBytes [8]byte
	binary.BigEndian.PutUint64(csnBytes[:], snap.LastCSN)
	buf = append(buf, csnBytes[:]...)
	buf = append(buf, mapBytes...)
	sum := crc32Sum(buf)
	buf = append(buf, sum...)

	tmp := filepath.Join(dir, name+snapshotTmpSuffix)
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return fsyncDirPath(dir)
}

func loadSnapshot(dir string) (map[string]string, uint64, error) {
	buf, err := os.ReadFile(snapshotPath(dir))
	if os.IsNotExist(err) {
		return make(map[string]string), 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	if len(buf) < len(snapshotMagic)+8+4 {
		return nil, 0, &SnapshotError{Op: "read", Err: fmt.Errorf("%w: truncated file", ErrCorruptSnapshot)}
	}
	if string(buf[:len(snapshotMagic)]) != snapshotMagic {
		return nil, 0, &SnapshotError{Op: "read", Err: fmt.Errorf("%w: bad magic", ErrCorruptSnapshot)}
	}
	stored := buf[len(buf)-4:]
	payload := buf[:len(buf)-4]
	if hex.EncodeToString(crc32Sum(payload)) != hex.EncodeToString(stored) {
		return nil, 0, &SnapshotError{Op: "read", Err: fmt.Errorf("%w: checksum mismatch", ErrCorruptSnapshot)}
	}
	csn := binary.BigEndian.Uint64(buf[len(snapshotMagic) : len(snapshotMagic)+8])
	var data map[string]string
	if err := gob.NewDecoder(bytesReader(payload[len(snapshotMagic)+8:])).Decode(&data); err != nil {
		return nil, 0, &SnapshotError{Op: "read", Err: fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)}
	}
	return data, csn, nil
}
