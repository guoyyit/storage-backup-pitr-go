package engine

import (
	"os"
)

// WriteSegment 将一组记录以 WAL 帧格式原子写入 path（tmp+rename）。
// 供备份模块生成增量段文件；读取侧使用 ReadAllRecords 严格校验。
func WriteSegment(path string, recs []Record) error {
	var size int
	for i := range recs {
		size += frameHeaderSize + 8 + 8 + 1 + 4 + len(recs[i].Key) + 4 + len(recs[i].Value)
	}
	buf := make([]byte, 0, size)
	for i := range recs {
		buf = append(buf, encodeRecord(&recs[i])...)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
