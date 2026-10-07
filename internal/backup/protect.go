package backup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrRestoreTargetConflict marks a restore destination that resolves to the
// running engine's data directory or the backup repository (or inside them,
// or containing them). The restore is rejected before any write occurs.
var ErrRestoreTargetConflict = errors.New("backup: restore target conflicts with protected directory")

// ProtectedKind identifies which protected location a restore target hit.
type ProtectedKind string

const (
	// ProtectedEngineDir is the running engine's data directory.
	ProtectedEngineDir ProtectedKind = "engine-data-dir"
	// ProtectedBackupRepo is the backup repository root.
	ProtectedBackupRepo ProtectedKind = "backup-repository"
)

// PathRelation describes how the resolved destination relates to a protected
// directory.
type PathRelation string

const (
	// RelationIdentical means the destination is the protected directory itself.
	RelationIdentical PathRelation = "identical"
	// RelationInside means the destination lies inside the protected directory.
	RelationInside PathRelation = "inside"
	// RelationAncestor means the destination contains the protected directory;
	// replacing it would move or delete the protected data.
	RelationAncestor PathRelation = "ancestor-of"
)

// RestoreTargetError pinpoints why a restore destination was rejected.
type RestoreTargetError struct {
	// Dest is the destination path as provided by the caller.
	Dest string
	// Resolved is the canonical absolute form of Dest actually checked.
	Resolved string
	// Protected is the canonical absolute path of the protected directory.
	Protected string
	// Kind names which protected location was hit.
	Kind ProtectedKind
	// Relation describes how Resolved relates to Protected.
	Relation PathRelation
}

func (e *RestoreTargetError) Error() string {
	return fmt.Sprintf("%v: destination %q (resolved %q) is %s %s %q",
		ErrRestoreTargetConflict, e.Dest, e.Resolved, e.Relation, e.Kind, e.Protected)
}

// Is lets errors.Is(err, ErrRestoreTargetConflict) match.
func (e *RestoreTargetError) Is(target error) bool {
	return target == ErrRestoreTargetConflict
}

// protectedDir pairs a protected directory with its identity for errors.
type protectedDir struct {
	kind ProtectedKind
	dir  string
}

// canonicalPath converts p into a canonical absolute path for comparison:
// relative paths are resolved against the process working directory, "." and
// ".." elements and duplicate separators are cleaned, and symlinks in the
// deepest existing ancestor (or the path itself) are resolved. It performs no
// writes.
func canonicalPath(p string) (string, error) {
	if p == "" {
		return "", errors.New("empty path")
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return resolveExisting(abs)
}

// resolveExisting resolves symlinks for the longest existing prefix of abs
// and re-attaches the not-yet-existing tail.
func resolveExisting(abs string) (string, error) {
	var tail []string
	cur := abs
	for {
		if _, err := os.Lstat(cur); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break // reached the root; nothing more to resolve
		}
		tail = append([]string{filepath.Base(cur)}, tail...)
		cur = parent
	}
	resolved, err := filepath.EvalSymlinks(cur)
	if err != nil {
		return "", err
	}
	for _, elem := range tail {
		resolved = filepath.Join(resolved, elem)
	}
	return resolved, nil
}

// pathRelation reports how p relates to dir; both must be canonical absolute
// paths from canonicalPath.
func pathRelation(p, dir string) (PathRelation, bool) {
	if p == dir {
		return RelationIdentical, true
	}
	sep := string(os.PathSeparator)
	if strings.HasPrefix(p, dir+sep) {
		return RelationInside, true
	}
	if strings.HasPrefix(dir, p+sep) {
		return RelationAncestor, true
	}
	return "", false
}

// engineDataDir returns the engine's data directory. It prefers the engine's
// own Dir method and falls back to the parent of the WAL directory.
func engineDataDir(eng Engine) string {
	if d, ok := eng.(interface{ Dir() string }); ok {
		if dir := d.Dir(); dir != "" {
			return dir
		}
	}
	return filepath.Dir(eng.WALDir())
}

// checkRestoreDest rejects restore destinations that alias a protected
// directory (identical, inside it, or an ancestor of it) after resolving
// relative paths, "." / ".." elements, duplicate separators and symlinks.
// It is purely read-only: a nil result means the destination is isolated and
// safe to write; on error nothing may be written.
func checkRestoreDest(dest string, protected ...protectedDir) error {
	resolved, err := canonicalPath(dest)
	if err != nil {
		return fmt.Errorf("backup: cannot resolve restore destination %q: %w", dest, err)
	}
	for _, prot := range protected {
		if prot.dir == "" {
			continue
		}
		canonical, err := canonicalPath(prot.dir)
		if err != nil {
			return fmt.Errorf("backup: cannot resolve %s %q: %w", prot.kind, prot.dir, err)
		}
		if rel, hit := pathRelation(resolved, canonical); hit {
			return &RestoreTargetError{
				Dest:      dest,
				Resolved:  resolved,
				Protected: canonical,
				Kind:      prot.kind,
				Relation:  rel,
			}
		}
	}
	return nil
}
