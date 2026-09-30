package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// 记录帧格式：[crc32(payload) uint32][payloadLen uint32][payload]
// payload = seq(8) + time(8) + op(1) + keyLen(4) + key + valueLen(4) + value
// 长度与 CRC 双重校验：崩溃造成的尾部撕裂在重放时被安全截断。

const (
	frameHeaderSize = 8
	maxFrameSize    = 64 << 20 // 单条记录上限 64MB，防御长度字段损坏
)

// ErrCorrupt 表示记录帧损坏（长度非法或 CRC 不匹配）。
var ErrCorrupt = errors.New("engine: corrupt record frame")

func encodeRecord(rec *Record) []byte {
	payloadLen := 8 + 8 + 1 + 4 + len(rec.Key) + 4 + len(rec.Value)
	buf := make([]byte, frameHeaderSize+payloadLen)
	payload := buf[frameHeaderSize:]
	off := 0
	binary.LittleEndian.PutUint64(payload[off:], rec.Seq)
	off += 8
	binary.LittleEndian.PutUint64(payload[off:], uint64(rec.Time))
	off += 8
	payload[off] = byte(rec.Op)
	off++
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(rec.Key)))
	off += 4
	copy(payload[off:], rec.Key)
	off += len(rec.Key)
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(rec.Value)))
	off += 4
	copy(payload[off:], rec.Value)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(payloadLen))
	return buf
}

// decodeFrame 从 r 读取一帧。返回 io.EOF 表示干净结束；
// ErrCorrupt 表示帧损坏或撕裂。
func decodeFrame(r io.Reader) (*Record, error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		// 读不满帧头：文件尾撕裂
		return nil, ErrCorrupt
	}
	crc := binary.LittleEndian.Uint32(header[0:4])
	payloadLen := binary.LittleEndian.Uint32(header[4:8])
	if payloadLen < 21 || payloadLen > maxFrameSize {
		return nil, ErrCorrupt
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, ErrCorrupt
	}
	if crc32.ChecksumIEEE(payload) != crc {
		return nil, ErrCorrupt
	}
	rec := &Record{}
	off := 0
	rec.Seq = binary.LittleEndian.Uint64(payload[off:])
	off += 8
	rec.Time = int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	rec.Op = Op(payload[off])
	off++
	keyLen := int(binary.LittleEndian.Uint32(payload[off:]))
	off += 4
	if keyLen < 0 || off+keyLen+4 > len(payload) {
		return nil, ErrCorrupt
	}
	rec.Key = string(payload[off : off+keyLen])
	off += keyLen
	valueLen := int(binary.LittleEndian.Uint32(payload[off:]))
	off += 4
	if valueLen < 0 || off+valueLen > len(payload) {
		return nil, ErrCorrupt
	}
	rec.Value = append([]byte(nil), payload[off:off+valueLen]...)
	if rec.Op != OpPut && rec.Op != OpDelete {
		return nil, ErrCorrupt
	}
	return rec, nil
}

// WAL 是追加写的预写日志，记录带 CRC 校验，崩溃后重放可恢复到最后一致状态。
type WAL struct {
	f *os.File
}

// OpenWAL 打开（不存在则创建）WAL 文件，写入游标定位到文件末尾。
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{f: f}, nil
}

// Append 追加一条记录（写盘但未保证 fsync）。
func (w *WAL) Append(rec *Record) error {
	if _, err := w.f.Write(encodeRecord(rec)); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	return nil
}

// Sync 将已写入数据刷到磁盘。
func (w *WAL) Sync() error {
	return w.f.Sync()
}

// Close 关闭 WAL。
func (w *WAL) Close() error {
	return w.f.Close()
}

// ReplayWAL 顺序读取 WAL 中的完整记录并对每条调用 fn。
// 遇到尾部撕裂/损坏记录时停止扫描并返回已恢复的前缀，不视为错误。
func ReplayWAL(path string, fn func(*Record) error) error {
	return scanWAL(path, false, fn)
}

// ReadAllRecords 严格读取 WAL/段文件中的全部记录：
// 任何损坏（包括尾部撕裂）都返回错误，用于备份校验与恢复。
func ReadAllRecords(path string) ([]Record, error) {
	var out []Record
	err := scanWAL(path, true, func(rec *Record) error {
		out = append(out, *rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func scanWAL(path string, strict bool, fn func(*Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()
	for {
		rec, err := decodeFrame(f)
		if err == io.EOF {
			return nil
		}
		if err == ErrCorrupt {
			if strict {
				return fmt.Errorf("%s: %w", path, ErrCorrupt)
			}
			// 容忍模式：崩溃截断的尾部，停止扫描
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}
