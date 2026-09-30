package wal

import "io"

// EncodeFrame exports frame serialization for restore-side writers.
func EncodeFrame(fr Frame) ([]byte, error) { return encodeFrame(fr) }

// ReadFrames decodes frames from r. When strict is true, any torn tail or
// checksum mismatch is reported as an error; otherwise a torn tail ends the
// scan successfully.
func ReadFrames(r io.Reader, name string, strict bool, fn func(Frame) error) error {
	return scanFrames(r, name, strict, func(fr Frame, _ int64) error {
		return fn(fr)
	})
}
