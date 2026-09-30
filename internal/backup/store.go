package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// MetaName is the manifest file inside a backup directory.
	MetaName = "meta.json"
	// DataDir holds the immutable segment copies.
	DataDir = "segments"

	tmpSuffix = ".tmp"
)

// ErrCorruptBackup identifies manifest/segment verification failures.
var ErrCorruptBackup = errors.New("backup: corrupt")

func backupDir(root, chainID, backupID string) string {
	return filepath.Join(root, chainID, backupID)
}

func metaPath(root, chainID, backupID string) string {
	return filepath.Join(backupDir(root, chainID, backupID), MetaName)
}

func segmentBackupPath(root, chainID, backupID, name string) string {
	return filepath.Join(backupDir(root, chainID, backupID), DataDir, name)
}

func newBackupID(now time.Time) string {
	return fmt.Sprintf("%016d", now.UnixNano())
}

// writeMetaAtomically writes meta.json with fsync of file and directory.
func writeMetaAtomically(root string, m Meta) error {
	dir := backupDir(root, m.ChainID, m.BackupID)
	if err := os.MkdirAll(filepath.Join(dir, DataDir), 0o755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	final := filepath.Join(dir, MetaName)
	tmp := final + tmpSuffix
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(dir)
}

func readMeta(root, chainID, backupID string) (Meta, error) {
	var m Meta
	buf, err := os.ReadFile(metaPath(root, chainID, backupID))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(buf, &m); err != nil {
		return m, fmt.Errorf("%w: %s/%s/%s: %v", ErrCorruptBackup, chainID, backupID, MetaName, err)
	}
	if m.BackupID == "" || m.ChainID == "" {
		return m, fmt.Errorf("%w: %s/%s: missing identity", ErrCorruptBackup, chainID, backupID)
	}
	return m, nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := copyNHash(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// listBackups returns the manifests of one chain: first is the full backup.
func listChain(root, chainID string) ([]Meta, error) {
	entries, err := os.ReadDir(filepath.Join(root, chainID))
	if err != nil {
		return nil, err
	}
	var metas []Meta
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		m, err := readMeta(root, chainID, ent.Name())
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		metas = append(metas, m)
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].BackupID < metas[j].BackupID })
	return metas, nil
}

func listChainIDs(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, ent := range entries {
		if ent.IsDir() && strings.HasPrefix(ent.Name(), "chain-") {
			ids = append(ids, ent.Name())
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func chainPath(root, chainID string) string { return filepath.Join(root, chainID) }
