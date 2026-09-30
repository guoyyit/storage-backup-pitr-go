// Package wal implements the durable write-ahead log backing the KV engine.
package wal

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FrameType marks the kind of mutation encoded by a Frame.
type FrameType uint8

const (
	// FramePut sets a key to a value.
	FramePut FrameType = 1
	// FrameDelete removes a key.
	FrameDelete FrameType = 2
)

const (
	segmentPrefix = "seg-"
	segmentSuffix = ".log"
	segmentWidth  = 10

	// DefaultSegmentBytes is the target size of one segment before rotation.
	DefaultSegmentBytes int64 = 64 << 20

	frameMagic uint32 = 0x574d4652 // "WFRM"

	headerLen    = 4 // uint32 body length
	crcLen       = 4
	bodyFixedLen = 4 + // magic
		1 + // type
		8 + // csn
		8 // timestamp unix nano
	keyLenLen   = 2
	valueLenLen = 4

	maxKeyLen   = 1 << 16
	maxValueLen = 1 << 30
)

var (
	// ErrCorrupt is returned when a segment fails checksum or framing checks.
	ErrCorrupt = errors.New("wal: corrupt segment")
	// ErrNotFound is returned for an unknown segment index.
	ErrNotFound = errors.New("wal: segment not found")

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// Frame is one committed mutation record.
type Frame struct {
	CSN       uint64
	Type      FrameType
	Key       string
	Value     []byte
	WrittenAt time.Time
}

// SegmentInfo describes one sealed or active segment file.
type SegmentInfo struct {
	Index    uint64
	Name     string
	Path     string
	StartCSN uint64
	EndCSN   uint64
	Size     int64
}

// CorruptionError identifies a damaged segment file and byte offset.
type CorruptionError struct {
	Segment string
	Offset  int64
	Op      string
	Err     error
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("%s: corrupt segment %q at offset %d: %v", e.Op, e.Segment, e.Offset, e.Err)
}

func (e *CorruptionError) Unwrap() error { return e.Err }

// Options configures a WAL.
type Options struct {
	// Dir holds segment files.
	Dir string
	// SegmentBytes triggers rotation when the active segment exceeds it.
	SegmentBytes int64
}

// segmentState tracks one segment. The last entry is the active segment.
type segmentState struct {
	info SegmentInfo
	f    *os.File // non-nil only for the active segment
}

// WAL is an append-only set of segment files. Sealed segments are immutable;
// only the highest-index (active) segment receives appends.
type WAL struct {
	opts   Options
	mu     sync.RWMutex
	segs   []segmentState
	active *segmentState
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentWidth, index, segmentSuffix)
}

func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	num := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	if len(num) != segmentWidth {
		return 0, false
	}
	idx, err := strconv.ParseUint(num, 10, 64)
	if err != nil {
		return 0, false
	}
	return idx, true
}

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// encodeFrame serializes a frame as: uint32 bodyLen | body | uint32 crc32(body).
func encodeFrame(fr Frame) ([]byte, error) {
	if len(fr.Key) > maxKeyLen {
		return nil, fmt.Errorf("wal: key too long (%d bytes)", len(fr.Key))
	}
	if len(fr.Value) > maxValueLen {
		return nil, fmt.Errorf("wal: value too long (%d bytes)", len(fr.Value))
	}
	if fr.Type != FramePut && fr.Type != FrameDelete {
		return nil, fmt.Errorf("wal: invalid frame type %d", fr.Type)
	}
	bodyLen := bodyFixedLen + keyLenLen + len(fr.Key) + valueLenLen + len(fr.Value)
	buf := make([]byte, headerLen+bodyLen+crcLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	b := buf[headerLen:]
	binary.BigEndian.PutUint32(b[0:4], frameMagic)
	b[4] = byte(fr.Type)
	binary.BigEndian.PutUint64(b[5:13], fr.CSN)
	binary.BigEndian.PutUint64(b[13:21], uint64(fr.WrittenAt.UnixNano()))
	binary.BigEndian.PutUint16(b[21:23], uint16(len(fr.Key)))
	copy(b[23:23+len(fr.Key)], fr.Key)
	b = b[23+len(fr.Key):]
	binary.BigEndian.PutUint32(b[0:4], uint32(len(fr.Value)))
	copy(b[4:4+len(fr.Value)], fr.Value)
	sum := crc32.Checksum(buf[headerLen:headerLen+bodyLen], crcTable)
	binary.BigEndian.PutUint32(buf[headerLen+bodyLen:], sum)
	return buf, nil
}

// decodeBody decodes a frame body (without length prefix and checksum suffix).
func decodeBody(body []byte) (Frame, error) {
	if len(body) < bodyFixedLen+keyLenLen+valueLenLen {
		return Frame{}, errors.New("frame body too short")
	}
	if binary.BigEndian.Uint32(body[0:4]) != frameMagic {
		return Frame{}, errors.New("bad frame magic")
	}
	fr := Frame{Type: FrameType(body[4])}
	if fr.Type != FramePut && fr.Type != FrameDelete {
		return Frame{}, fmt.Errorf("invalid frame type %d", fr.Type)
	}
	fr.CSN = binary.BigEndian.Uint64(body[5:13])
	fr.WrittenAt = time.Unix(0, int64(binary.BigEndian.Uint64(body[13:21])))
	keyLen := int(binary.BigEndian.Uint16(body[21:23]))
	if bodyFixedLen+keyLenLen+keyLen+valueLenLen > len(body) {
		return Frame{}, errors.New("key length exceeds body")
	}
	fr.Key = string(body[23 : 23+keyLen])
	rest := body[23+keyLen:]
	valLen := int(binary.BigEndian.Uint32(rest[0:4]))
	if valueLenLen+valLen > len(rest) {
		return Frame{}, errors.New("value length exceeds body")
	}
	if valueLenLen+valLen != len(rest) {
		return Frame{}, errors.New("trailing bytes in frame body")
	}
	fr.Value = append([]byte(nil), rest[4:4+valLen]...)
	return fr, nil
}

type frameScanner struct {
	r      io.Reader
	name   string
	strict bool // true rejects torn tails (sealed segments)
	offset int64
}

// scanFrames streams frames from r. If strict is false (active segment), a
// truncated tail from a process crash is tolerated and the callback reports
// each frame's file offset; otherwise every framing/checksum error becomes a
// CorruptionError.
func scanFrames(r io.Reader, name string, strict bool, fn func(Frame, int64) error) error {
	sc := &frameScanner{r: r, name: name, strict: strict}
	for {
		fr, at, err := sc.next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(fr, at); err != nil {
			return err
		}
	}
}

// next returns the next frame and its file offset.
func (sc *frameScanner) next() (Frame, int64, error) {
	frameStart := sc.offset
	var header [headerLen]byte
	if _, err := io.ReadFull(sc.r, header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Frame{}, frameStart, io.EOF // clean end
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, frameStart, sc.tailError(frameStart, "partial frame header")
		}
		return Frame{}, frameStart, err
	}
	bodyLen := binary.BigEndian.Uint32(header[:])
	minBody := bodyFixedLen + keyLenLen + valueLenLen
	maxBody := minBody + maxKeyLen + maxValueLen
	if bodyLen < uint32(minBody) || bodyLen > uint32(maxBody) {
		return Frame{}, frameStart, &CorruptionError{
			Segment: sc.name, Offset: frameStart, Op: "read",
			Err: fmt.Errorf("%w: invalid frame length %d", ErrCorrupt, bodyLen),
		}
	}
	raw := make([]byte, headerLen+int(bodyLen)+crcLen)
	copy(raw, header[:])
	if _, err := io.ReadFull(sc.r, raw[headerLen:]); err != nil {
		return Frame{}, frameStart, sc.tailError(frameStart, "truncated frame body")
	}
	sc.offset += int64(len(raw))
	body := raw[headerLen : headerLen+bodyLen]
	want := binary.BigEndian.Uint32(raw[headerLen+bodyLen:])
	got := crc32.Checksum(body, crcTable)
	if want != got {
		return Frame{}, frameStart, &CorruptionError{
			Segment: sc.name, Offset: frameStart, Op: "read",
			Err: fmt.Errorf("%w: checksum mismatch", ErrCorrupt),
		}
	}
	fr, err := decodeBody(body)
	if err != nil {
		return Frame{}, frameStart, &CorruptionError{
			Segment: sc.name, Offset: frameStart, Op: "read",
			Err: fmt.Errorf("%w: %v", ErrCorrupt, err),
		}
	}
	return fr, frameStart, nil
}

func (sc *frameScanner) tailError(at int64, msg string) error {
	if sc.strict {
		return &CorruptionError{
			Segment: sc.name, Offset: at, Op: "read",
			Err: fmt.Errorf("%w: %s (torn tail in sealed segment)", ErrCorrupt, msg),
		}
	}
	return io.ErrUnexpectedEOF
}

// Open opens or creates a WAL in opts.Dir. The active segment's torn tail
// (from a crash mid-write) is truncated; sealed segments are verified
// strictly and any damage is reported as CorruptionError.
func Open(opts Options) (*WAL, error) {
	if opts.Dir == "" {
		return nil, errors.New("wal: empty dir")
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(opts.Dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if _, ok := parseSegmentName(ent.Name()); ok {
			names = append(names, ent.Name())
		}
	}
	sort.Strings(names)

	w := &WAL{opts: opts}
	for i, name := range names {
		idx, _ := parseSegmentName(name)
		sealed := i < len(names)-1
		info, err := w.scanSegmentFile(name, idx, sealed)
		if err != nil {
			return nil, err
		}
		w.segs = append(w.segs, segmentState{info: info})
	}
	if len(w.segs) == 0 {
		if err := w.createSegmentLocked(0); err != nil {
			return nil, err
		}
	} else {
		active := &w.segs[len(w.segs)-1]
		f, err := os.OpenFile(filepath.Join(opts.Dir, active.info.Name), os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		active.f = f
		w.active = active
	}
	return w, nil
}

// scanSegmentFile verifies one file and truncates a torn tail of the active
// (non-strict) file to the last complete frame.
func (w *WAL) scanSegmentFile(name string, idx uint64, sealed bool) (SegmentInfo, error) {
	path := filepath.Join(w.opts.Dir, name)
	info := SegmentInfo{Index: idx, Name: name, Path: path}
	var goodOffset int64
	f, err := os.Open(path)
	if err != nil {
		return SegmentInfo{}, err
	}
	scanErr := scanFrames(f, name, sealed, func(fr Frame, at int64) error {
		if goodOffset == 0 {
			info.StartCSN = fr.CSN
		}
		info.EndCSN = fr.CSN
		goodOffset = at + frameLen(fr)
		return nil
	})
	f.Close()
	if scanErr != nil && !errors.Is(scanErr, io.ErrUnexpectedEOF) {
		return SegmentInfo{}, scanErr
	}
	if !sealed {
		st, statErr := os.Stat(path)
		if statErr != nil {
			return SegmentInfo{}, statErr
		}
		if st.Size() != goodOffset {
			if terr := os.Truncate(path, goodOffset); terr != nil {
				return SegmentInfo{}, terr
			}
			if serr := syncFile(path); serr != nil {
				return SegmentInfo{}, serr
			}
		}
	}
	info.Size = goodOffset
	return info, nil
}

func frameLen(fr Frame) int64 {
	return int64(headerLen + bodyFixedLen + keyLenLen + len(fr.Key) + valueLenLen + len(fr.Value) + crcLen)
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (w *WAL) createSegmentLocked(idx uint64) error {
	name := segmentName(idx)
	f, err := os.OpenFile(filepath.Join(w.opts.Dir, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if err := fsyncDir(w.opts.Dir); err != nil {
		f.Close()
		return err
	}
	st := segmentState{
		info: SegmentInfo{Index: idx, Name: name, Path: filepath.Join(w.opts.Dir, name)},
		f:    f,
	}
	w.segs = append(w.segs, st)
	w.active = &w.segs[len(w.segs)-1]
	return nil
}

// Append writes one frame and durably fsyncs it before returning.
func (w *WAL) Append(_ context.Context, fr Frame) error {
	raw, err := encodeFrame(fr)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active.info.Size >= w.opts.SegmentBytes && w.active.info.Size > 0 {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	if _, err := w.active.f.Write(raw); err != nil {
		return err
	}
	if err := w.active.f.Sync(); err != nil {
		return err
	}
	w.active.info.Size += int64(len(raw))
	if w.active.info.StartCSN == 0 && fr.CSN != 0 {
		w.active.info.StartCSN = fr.CSN
	}
	w.active.info.EndCSN = fr.CSN
	return nil
}

// Rotate seals the active segment (even if empty) and starts a fresh one.
// After it returns, all frames committed so far live in immutable segments.
func (w *WAL) Rotate(_ context.Context) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateLocked(); err != nil {
		return 0, err
	}
	return w.active.info.Index, nil
}

func (w *WAL) rotateLocked() error {
	active := w.active
	if err := active.f.Sync(); err != nil {
		return err
	}
	if err := active.f.Close(); err != nil {
		return err
	}
	active.f = nil
	if err := w.createSegmentLocked(active.info.Index + 1); err != nil {
		return err
	}
	return nil
}

// Segments returns copies of all segment infos ordered by index.
func (w *WAL) Segments() []SegmentInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]SegmentInfo, len(w.segs))
	for i := range w.segs {
		out[i] = w.segs[i].info
	}
	return out
}

// ActiveIndex returns the index of the segment receiving appends.
func (w *WAL) ActiveIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.active.info.Index
}

// LastCSN returns the CSN of the last durable frame.
func (w *WAL) LastCSN() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for i := len(w.segs) - 1; i >= 0; i-- {
		if w.segs[i].info.EndCSN != 0 {
			return w.segs[i].info.EndCSN
		}
	}
	return 0
}

func (w *WAL) findSegment(idx uint64) (segmentState, bool) {
	for _, s := range w.segs {
		if s.info.Index == idx {
			return s, true
		}
	}
	return segmentState{}, false
}

// verifySegment strictly scans a sealed segment file for damage.
func verifySegment(path, name string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return scanFrames(f, name, true, func(Frame, int64) error { return nil })
}

// CopySegment verifies and copies a sealed segment to out, returning the
// hex-encoded SHA-256 of its bytes. The active segment cannot be copied.
func (w *WAL) CopySegment(_ context.Context, idx uint64, out io.Writer) (string, int64, error) {
	w.mu.RLock()
	s, ok := w.findSegment(idx)
	activeIdx := w.active.info.Index
	w.mu.RUnlock()
	if !ok {
		return "", 0, fmt.Errorf("%w: index %d", ErrNotFound, idx)
	}
	if idx == activeIdx {
		return "", 0, fmt.Errorf("wal: cannot copy active segment %d (rotate first)", idx)
	}
	if err := verifySegment(s.info.Path, s.info.Name); err != nil {
		return "", 0, err
	}
	f, err := os.Open(s.info.Path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	n, sum, err := copyAndHash(out, f)
	if err != nil {
		return "", 0, err
	}
	return sum, n, nil
}

// ReadSegment streams decoded frames of one segment. Sealed segments are read
// strictly; the active segment tolerates a not-yet-visible tail.
func (w *WAL) ReadSegment(ctx context.Context, idx uint64, fn func(Frame) error) error {
	w.mu.RLock()
	s, ok := w.findSegment(idx)
	activeIdx := w.active.info.Index
	w.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: index %d", ErrNotFound, idx)
	}
	f, err := os.Open(s.info.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	strict := idx != activeIdx
	return scanFrames(f, s.info.Name, strict, func(fr Frame, _ int64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		return fn(fr)
	})
}

// RemoveSegmentsBeforeOrEqual deletes segment files with index <= idx. The
// active segment can never be removed.
func (w *WAL) RemoveSegmentsBeforeOrEqual(_ context.Context, idx uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if idx >= w.active.info.Index {
		return fmt.Errorf("wal: cannot remove active segment %d", w.active.info.Index)
	}
	kept := w.segs[:0]
	for _, s := range w.segs {
		if s.info.Index <= idx {
			if err := os.Remove(s.info.Path); err != nil && !os.IsNotExist(err) {
				return err
			}
		} else {
			kept = append(kept, s)
		}
	}
	w.segs = kept
	return fsyncDir(w.opts.Dir)
}

// CapActiveToCSN truncates the active segment so it ends at the last frame
// with CSN <= csn. Used by point-in-time restore finalization.
func (w *WAL) CapActiveToCSN(csn uint64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	active := w.active
	path := active.info.Path
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return 0, err
	}
	var goodOffset int64
	var lastCSN uint64
	scanErr := scanFrames(f, active.info.Name, false, func(fr Frame, at int64) error {
		if fr.CSN <= csn {
			goodOffset = at + frameLen(fr)
			lastCSN = fr.CSN
			return nil
		}
		return errStopScan
	})
	f.Close()
	if scanErr != nil && !errors.Is(scanErr, errStopScan) {
		return 0, scanErr
	}
	if err := os.Truncate(path, goodOffset); err != nil {
		return 0, err
	}
	tf, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return 0, err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return 0, err
	}
	active.f.Close()
	active.f = tf
	active.info.Size = goodOffset
	active.info.EndCSN = lastCSN
	if goodOffset == 0 {
		active.info.StartCSN = 0
	}
	return lastCSN, fsyncDir(w.opts.Dir)
}

var errStopScan = errors.New("wal: stop scan")

func newSha256Hash() hash.Hash {
	return sha256.New()
}

func sumHex(h interface{ Sum([]byte) []byte }) string {
	return hex.EncodeToString(h.Sum(nil))
}

// Sync flushes the active segment to disk.
func (w *WAL) Sync(_ context.Context) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.active.f.Sync()
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active != nil && w.active.f != nil {
		err := w.active.f.Sync()
		if cerr := w.active.f.Close(); err == nil {
			err = cerr
		}
		w.active.f = nil
		return err
	}
	return nil
}

func copyAndHash(dst io.Writer, src io.Reader) (int64, string, error) {
	h := newSha256Hash()
	n, err := io.Copy(io.MultiWriter(dst, h), src)
	if err != nil {
		return 0, "", err
	}
	return n, sumHex(h), nil
}
