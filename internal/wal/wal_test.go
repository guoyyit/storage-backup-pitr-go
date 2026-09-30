package wal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1, 2).UTC()
	for i := uint64(1); i <= 5; i++ {
		fr := Frame{CSN: i, Type: FramePut, Key: "k", Value: []byte("v"), WrittenAt: now}
		if err := w.Append(context.Background(), fr); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.LastCSN(); got != 5 {
		t.Fatalf("LastCSN=%d want 5", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(Options{Dir: dir, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if got := w2.LastCSN(); got != 5 {
		t.Fatalf("reopen LastCSN=%d want 5", got)
	}
	var n int
	err = w2.ReadSegment(context.Background(), w2.ActiveIndex(), func(fr Frame) error {
		n++
		if fr.CSN != uint64(n) {
			t.Fatalf("csn=%d want %d", fr.CSN, n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("frames=%d want 5", n)
	}
}

func TestRotateAndCopy(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := w.Append(context.Background(), Frame{CSN: i, Type: FramePut, Key: "a", Value: []byte("b")}); err != nil {
			t.Fatal(err)
		}
	}
	newIdx, err := w.Rotate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if newIdx != 1 {
		t.Fatalf("new active idx=%d want 1", newIdx)
	}
	if err := w.Append(context.Background(), Frame{CSN: 4, Type: FrameDelete, Key: "a"}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	sum, n, err := w.CopySegment(context.Background(), 0, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || len(sum) != 64 {
		t.Fatalf("copy n=%d sum=%q", n, sum)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := w.Append(context.Background(), Frame{CSN: i, Type: FramePut, Key: "k", Value: []byte("vvv")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Rotate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(context.Background(), Frame{CSN: 4, Type: FramePut, Key: "k", Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Damage a byte inside the body of the sealed segment.
	sealed := filepath.Join(dir, segmentName(0))
	data, err := os.ReadFile(sealed)
	if err != nil {
		t.Fatal(err)
	}
	data[10] ^= 0xff
	if err := os.WriteFile(sealed, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(Options{Dir: dir}); err == nil {
		t.Fatal("expected corruption error")
	} else {
		var ce *CorruptionError
		if !errors.As(err, &ce) {
			t.Fatalf("want *CorruptionError, got %T: %v", err, err)
		}
		if ce.Segment != segmentName(0) {
			t.Fatalf("damaged segment=%q", ce.Segment)
		}
	}
}

func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(context.Background(), Frame{CSN: 1, Type: FramePut, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, segmentName(0))
	data, _ := os.ReadFile(p)
	// Simulate a torn write: append garbage bytes that look like a partial header.
	if err := os.WriteFile(p, append(data, 0x00, 0x00, 0x10, 0x00, 0xaa), 0o644); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("torn tail should truncate, got %v", err)
	}
	if got := w2.LastCSN(); got != 1 {
		t.Fatalf("LastCSN=%d want 1", got)
	}
}
