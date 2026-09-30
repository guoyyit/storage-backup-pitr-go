package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/guoyyit/storage-backup-pitr-go/internal/wal"
)

// walOps adapts the live engine WAL to the backup package interfaces.
type walOps struct {
	w *wal.WAL
}

func openWalOps(dir string) (*walOps, error) {
	w, err := wal.Open(wal.Options{Dir: dir})
	if err != nil {
		return nil, err
	}
	return &walOps{w: w}, nil
}

func (o *walOps) close() error { return o.w.Close() }

func (o *walOps) Segments() []SegmentInfo {
	src := o.w.Segments()
	out := make([]SegmentInfo, len(src))
	for i, s := range src {
		out[i] = SegmentInfo{
			Index: s.Index, Name: s.Name,
			StartCSN: s.StartCSN, EndCSN: s.EndCSN, Size: s.Size,
		}
	}
	return out
}

// CopySegment verifies and copies a sealed segment into dstPath.
func (o *walOps) CopySegment(ctx context.Context, idx uint64, dstPath string) (string, int64, error) {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return "", 0, err
	}
	out, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0o644)
	if err != nil {
		return "", 0, err
	}
	sum, n, err := o.w.CopySegment(ctx, idx, out)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(dstPath)
		return "", 0, err
	}
	return sum, n, nil
}

func walSegmentName(idx uint64) string {
	return fmt.Sprintf("seg-%010d.log", idx)
}

func parseWalSegmentName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	num := strings.TrimSuffix(strings.TrimPrefix(name, "seg-"), ".log")
	idx, err := strconv.ParseUint(num, 10, 64)
	return idx, err == nil
}

// verifyBackupSegment strictly decodes one stored segment copy.
func verifyBackupSegment(path, name string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return wal.ReadFrames(f, name, true, func(wal.Frame) error { return nil })
}

var _ io.Reader = (*os.File)(nil)
