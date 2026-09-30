package backup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/wal"
)

// segRef locates one stored segment and its manifest checksum.
type segRef struct {
	SegmentFile
	chainID  string
	backupID string
}

// mergedSegmentFiles dedups segments (a later backup may include segments
// already present in an earlier one) and sorts them by index.
func mergedSegmentFiles(metas []Meta) ([]segRef, error) {
	byIndex := map[uint64]segRef{}
	var order []uint64
	for _, m := range metas {
		for _, sf := range m.Segments {
			if existing, ok := byIndex[sf.Index]; ok {
				if existing.SHA256 != sf.SHA256 {
					return nil, fmt.Errorf("%w: segment %s has conflicting copies in %s and %s",
						ErrCorruptBackup, sf.Name, existing.backupID, m.BackupID)
				}
				continue
			}
			byIndex[sf.Index] = segRef{SegmentFile: sf, chainID: m.ChainID, backupID: m.BackupID}
			order = append(order, sf.Index)
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	out := make([]segRef, 0, len(order))
	for _, idx := range order {
		out = append(out, byIndex[idx])
	}
	return out, nil
}

func (s *Service) storedSegmentPath(ref segRef) string {
	return filepath.Join(s.cfg.Dir, ref.chainID, ref.backupID, DataDir, ref.Name)
}

// writeFilteredSegment copies frames with CSN <= target into a fresh segment
// in the staging directory, re-encoding them (CRC protected).
func writeFilteredSegment(
	ctx context.Context,
	root string,
	ref segRef,
	walDir string,
	outIdx uint64,
	target uint64,
) (written bool, lastCSN uint64, lastAt time.Time, err error) {
	path := filepath.Join(root, ref.chainID, ref.backupID, DataDir, ref.Name)
	in, err := os.Open(path)
	if err != nil {
		return false, 0, time.Time{}, err
	}
	defer in.Close()

	outPath := filepath.Join(walDir, walSegmentName(outIdx))
	out, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return false, 0, time.Time{}, err
	}
	abort := func(cause error) error {
		out.Close()
		os.Remove(outPath)
		return cause
	}
	scanErr := wal.ReadFrames(in, ref.Name, true, func(fr wal.Frame) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if fr.CSN <= target {
			raw, encErr := wal.EncodeFrame(fr)
			if encErr != nil {
				return encErr
			}
			if _, wErr := out.Write(raw); wErr != nil {
				return wErr
			}
			written = true
			lastCSN = fr.CSN
			lastAt = fr.WrittenAt
		}
		return nil
	})
	if scanErr != nil {
		return false, 0, time.Time{}, abort(scanErr)
	}
	if err := out.Sync(); err != nil {
		return false, 0, time.Time{}, abort(err)
	}
	if err := out.Close(); err != nil {
		os.Remove(outPath)
		return false, 0, time.Time{}, err
	}
	return written, lastCSN, lastAt, nil
}

// copyRaw copies one file verbatim, creating parent dirs.
func copyRaw(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func verifyBackupSnapshot(root string, m Meta) error {
	p := filepath.Join(backupDir(root, m.ChainID, m.BackupID), "snapshot.gob")
	sum, _, err := hashFile(p)
	if err != nil {
		return segmentError(m, "snapshot.gob", "hash", err)
	}
	if sum != m.SnapshotSHA256 {
		return segmentError(m, "snapshot.gob", "checksum",
			fmt.Errorf("sha256 %s != manifest %s", sum, m.SnapshotSHA256))
	}
	return nil
}

// snapshotLastCSN reads the uint64 CSN embedded after the 8-byte magic.
func snapshotLastCSN(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var head [16]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		return 0, fmt.Errorf("%w: snapshot truncated", ErrCorruptBackup)
	}
	if string(head[:8]) != "KVSNAP01" {
		return 0, fmt.Errorf("%w: bad snapshot magic", ErrCorruptBackup)
	}
	return binary.BigEndian.Uint64(head[8:16]), nil
}

// atomicReplaceDir moves staging into place after syncing both directories.
func atomicReplaceDir(dest, staging string) error {
	if err := syncDir(filepath.Dir(staging)); err != nil {
		return err
	}
	if _, err := os.Stat(dest); err == nil {
		backupOld := dest + ".old"
		_ = os.RemoveAll(backupOld)
		if err := os.Rename(dest, backupOld); err != nil {
			return err
		}
		if err := os.Rename(staging, dest); err != nil {
			// Best-effort rollback.
			_ = os.Rename(backupOld, dest)
			return err
		}
		_ = syncDir(filepath.Dir(dest))
		return os.RemoveAll(backupOld)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(staging, dest); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dest))
}

var _ = sha256.New
var _ = hex.EncodeToString

func scanStoredSegment(path, name string, fn func(csn uint64, at time.Time) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return wal.ReadFrames(f, name, true, func(fr wal.Frame) error {
		return fn(fr.CSN, fr.WrittenAt)
	})
}

func writeEmptySegment(walDir string, idx uint64) error {
	f, err := os.OpenFile(filepath.Join(walDir, walSegmentName(idx)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
