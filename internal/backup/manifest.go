package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	chainsDirName  = "chains"
	manifestName   = "manifest.json"
	fullDataName   = "full.snapshot"
	incrDataPrefix = "incr-"
	incrDataSuffix = ".segment"
)

func (m *Manager) chainDir(chainID string) string {
	return filepath.Join(m.dir, chainsDirName, chainID)
}

// writeManifest 原子写入链清单（tmp+rename），崩溃不会留下半个清单。
func writeManifest(chainDir string, mf *Manifest) error {
	data, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(chainDir, manifestName+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(chainDir, manifestName))
}

func readManifest(chainDir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(chainDir, manifestName))
	if err != nil {
		return nil, err
	}
	var mf Manifest
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", chainDir, err)
	}
	return &mf, nil
}

// incrSegmentName 返回覆盖 (fromSeq, toSeq] 的增量段文件名。
func incrSegmentName(fromSeq, toSeq uint64) string {
	return fmt.Sprintf("%s%020d-%020d%s", incrDataPrefix, fromSeq, toSeq, incrDataSuffix)
}
