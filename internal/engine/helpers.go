package engine

import (
	"bytes"
	"hash/crc32"
	"os"
)

var engineCRCTable = crc32.MakeTable(crc32.Castagnoli)

func crc32Sum(b []byte) []byte {
	sum := crc32.Checksum(b, engineCRCTable)
	out := make([]byte, 4)
	out[0] = byte(sum >> 24)
	out[1] = byte(sum >> 16)
	out[2] = byte(sum >> 8)
	out[3] = byte(sum)
	return out
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func fsyncDirPath(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
