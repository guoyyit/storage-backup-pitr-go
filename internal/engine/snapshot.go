package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// 快照文件格式：
//
//	header: magic(4) + version(4) + seq(8) + commitTime(8) + count(8)
//	entry:  keyLen(4) + key + valueLen(4) + value + entryCRC(4)
//	footer: fileCRC(4)，覆盖此前全部字节
//
// 快照内容等于某个已提交 Seq 时刻的完整状态，即一致点。
// 写入采用 tmp+rename 保证原子性：崩溃不会留下半个快照。
const (
	snapshotMagic   = "KVS1"
	snapshotVersion = 1
	snapshotPrefix  = "snapshot-"
	snapshotSuffix  = ".snap"
)

// SnapshotFileName 返回 seq 对应的快照文件名。
func SnapshotFileName(seq uint64) string {
	return snapshotPrefix + fmt.Sprintf("%020d", seq) + snapshotSuffix
}

// WriteSnapshot 将 entries 原子写入 dir 下的新快照文件，返回文件路径。
// commitTime 为该一致点的最大提交时间，用于恢复后维持时间戳单调。
func WriteSnapshot(dir string, seq uint64, commitTime int64, entries []Record) (string, error) {
	var buf bytes.Buffer
	header := make([]byte, 32)
	copy(header[0:4], snapshotMagic)
	binary.LittleEndian.PutUint32(header[4:8], snapshotVersion)
	binary.LittleEndian.PutUint64(header[8:16], seq)
	binary.LittleEndian.PutUint64(header[16:24], uint64(commitTime))
	binary.LittleEndian.PutUint64(header[24:32], uint64(len(entries)))
	buf.Write(header)
	for i := range entries {
		e := &entries[i]
		var lens [8]byte
		binary.LittleEndian.PutUint32(lens[0:4], uint32(len(e.Key)))
		binary.LittleEndian.PutUint32(lens[4:8], uint32(len(e.Value)))
		crc := crc32.NewIEEE()
		crc.Write(lens[:])
		crc.Write([]byte(e.Key))
		crc.Write(e.Value)
		buf.Write(lens[:])
		buf.WriteString(e.Key)
		buf.Write(e.Value)
		var crcBuf [4]byte
		binary.LittleEndian.PutUint32(crcBuf[:], crc.Sum32())
		buf.Write(crcBuf[:])
	}
	var footer [4]byte
	binary.LittleEndian.PutUint32(footer[:], crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(footer[:])

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	final := filepath.Join(dir, SnapshotFileName(seq))
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return "", fmt.Errorf("write snapshot tmp: %w", err)
	}
	if f, err := os.Open(tmp); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, final); err != nil {
		return "", fmt.Errorf("rename snapshot: %w", err)
	}
	syncDir(dir)
	return final, nil
}

// ReadSnapshot 读取并校验快照文件，任何损坏都会返回明确错误。
func ReadSnapshot(path string) (seq uint64, commitTime int64, entries []Record, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("read snapshot: %w", err)
	}
	if len(raw) < 36 {
		return 0, 0, nil, fmt.Errorf("snapshot %s: file too short (%d bytes)", path, len(raw))
	}
	body, footer := raw[:len(raw)-4], raw[len(raw)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(footer) {
		return 0, 0, nil, fmt.Errorf("snapshot %s: file checksum mismatch", path)
	}
	if string(body[0:4]) != snapshotMagic {
		return 0, 0, nil, fmt.Errorf("snapshot %s: bad magic", path)
	}
	if v := binary.LittleEndian.Uint32(body[4:8]); v != snapshotVersion {
		return 0, 0, nil, fmt.Errorf("snapshot %s: unsupported version %d", path, v)
	}
	seq = binary.LittleEndian.Uint64(body[8:16])
	commitTime = int64(binary.LittleEndian.Uint64(body[16:24]))
	count := binary.LittleEndian.Uint64(body[24:32])
	off := 32
	for i := uint64(0); i < count; i++ {
		if off+8 > len(body) {
			return 0, 0, nil, fmt.Errorf("snapshot %s: truncated entry %d", path, i)
		}
		keyLen := int(binary.LittleEndian.Uint32(body[off : off+4]))
		valueLen := int(binary.LittleEndian.Uint32(body[off+4 : off+8]))
		end := off + 8 + keyLen + valueLen + 4
		if keyLen < 0 || valueLen < 0 || end > len(body) {
			return 0, 0, nil, fmt.Errorf("snapshot %s: entry %d out of bounds", path, i)
		}
		crc := crc32.NewIEEE()
		crc.Write(body[off : off+8+keyLen+valueLen])
		stored := binary.LittleEndian.Uint32(body[end-4 : end])
		if crc.Sum32() != stored {
			return 0, 0, nil, fmt.Errorf("snapshot %s: entry %d checksum mismatch", path, i)
		}
		key := string(body[off+8 : off+8+keyLen])
		value := append([]byte(nil), body[off+8+keyLen:off+8+keyLen+valueLen]...)
		entries = append(entries, Record{Op: OpPut, Key: key, Value: value})
		off = end
	}
	if off != len(body) {
		return 0, 0, nil, fmt.Errorf("snapshot %s: trailing bytes after %d entries", path, count)
	}
	return seq, commitTime, entries, nil
}

// LatestSnapshot 返回 dir 中 Seq 最大的快照文件路径；不存在时返回空串。
func LatestSnapshot(dir string) (string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	best := ""
	var bestSeq uint64
	for _, ent := range ents {
		name := ent.Name()
		if !strings.HasPrefix(name, snapshotPrefix) || !strings.HasSuffix(name, snapshotSuffix) {
			continue
		}
		num := strings.TrimSuffix(strings.TrimPrefix(name, snapshotPrefix), snapshotSuffix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			continue
		}
		if best == "" || seq > bestSeq {
			best, bestSeq = filepath.Join(dir, name), seq
		}
	}
	return best, nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
