package backup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	// ErrRestoreTargetConflict means the resolved restore destination is the
	// running engine data directory, lives inside it, or lives inside the
	// backup repository (including relative-path, symlink and duplicate-
	// separator equivalents). It is returned before anything is written.
	ErrRestoreTargetConflict = errors.New("backup: restore target conflicts with a protected directory")

	// ErrChainUnavailable means the chain selected for restore was reclaimed
	// (or never existed) and cannot be used. The restore is rejected before
	// anything is written to the destination.
	ErrChainUnavailable = errors.New("backup: restore chain unavailable")
)

// TargetConflictKind identifies which protected root a destination collided with.
type TargetConflictKind string

const (
	// ConflictEngineDir: destination equals or is inside the engine data dir.
	ConflictEngineDir TargetConflictKind = "engine_data_dir"
	// ConflictBackupRepo: destination equals or is inside the backup repository.
	ConflictBackupRepo TargetConflictKind = "backup_repository"
)

// TargetConflictError is a distinguishable, locatable rejection of a restore
// destination. It names the protected root kind, the protected root itself and
// the fully resolved destination path.
type TargetConflictError struct {
	Kind     TargetConflictKind
	Root     string
	Resolved string
}

func (e *TargetConflictError) Error() string {
	return fmt.Sprintf("%s: kind=%s: resolved destination %q %s protected root %q",
		ErrRestoreTargetConflict.Error(), e.Kind, e.Resolved, conflictRelation(e.Kind), e.Root)
}

func conflictRelation(k TargetConflictKind) string {
	switch k {
	case ConflictEngineDir:
		return "equals or is inside running engine data directory"
	case ConflictBackupRepo:
		return "equals or is inside backup repository"
	default:
		return "conflicts with"
	}
}

// Is makes errors.Is(err, ErrRestoreTargetConflict) match every kind.
func (e *TargetConflictError) Is(target error) bool { return target == ErrRestoreTargetConflict }

// canonicalPath resolves p to an absolute, symlink-free, lexically clean path.
// Every component that exists on disk is followed through (EvalSymlinks);
// non-existent trailing components are joined cleaned. Relative paths are
// resolved against the process working directory first, so "foo", "./foo" and
// duplicate separators cannot bypass the checks.
func canonicalPath(p string) (string, error) {
	if p == "" {
		return "", errors.New("backup: empty path")
	}
	if !filepath.IsAbs(p) {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		p = filepath.Join(wd, p)
	}
	return resolveExistingPrefix(p)
}

// resolveExistingPrefix walks from the deepest existing ancestor and evaluates
// symlinks only over existing components, then appends the missing tail.
func resolveExistingPrefix(p string) (string, error) {
	p = filepath.Clean(p)
	if _, err := os.Lstat(p); err == nil {
		resolved, rerr := filepath.EvalSymlinks(p)
		if rerr != nil {
			return "", rerr
		}
		return filepath.Clean(resolved), nil
	}
	parent, tail := filepath.Split(p)
	parent = filepath.Clean(parent)
	if parent == p {
		// Reached a filesystem root.
		return p, nil
	}
	base, err := resolveExistingPrefix(parent)
	if err != nil {
		return "", err
	}
	if tail == "" || tail == "." || tail == string(filepath.Separator) {
		return base, nil
	}
	return filepath.Join(base, tail), nil
}

// pathEqualOrWithin reports whether child is root itself or located below it.
// Both arguments must be absolute and cleaned.
func pathEqualOrWithin(root, child string) bool {
	if child == root {
		return true
	}
	rel, err := filepath.Rel(root, child)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != ".." && !filepath.IsAbs(rel) && !hasParentPrefix(rel)
}

func hasParentPrefix(rel string) bool {
	// ".." or "../..." escapes root; "foo/../.." is already cleaned away
	// because callers pass cleaned paths.
	return len(rel) >= 2 && rel[0:2] == ".." &&
		(len(rel) == 2 || rel[2] == filepath.Separator)
}

// validateRestoreTarget resolves dest and its staging sibling and rejects any
// collision with the engine data directory or the backup repository. Nothing
// is created or removed by this check.
func validateRestoreTarget(dest, engineDir, repoDir string) error {
	resolvedDest, err := canonicalPath(dest)
	if err != nil {
		return fmt.Errorf("backup: resolve restore destination %q: %w", dest, err)
	}
	// The staging directory is a direct sibling of the destination.
	resolvedStaging := resolvedDest + restoreStagingSuffix

	engineRoot, err := canonicalPath(engineDir)
	if err != nil {
		return fmt.Errorf("backup: resolve engine data dir: %w", err)
	}
	repoRoot, err := canonicalPath(repoDir)
	if err != nil {
		return fmt.Errorf("backup: resolve backup repository dir: %w", err)
	}

	conflicts := []struct {
		root string
		kind TargetConflictKind
	}{
		{engineRoot, ConflictEngineDir},
		{repoRoot, ConflictBackupRepo},
	}
	for _, c := range conflicts {
		// The destination itself (or a symlink-equivalent of it).
		if pathEqualOrWithin(c.root, resolvedDest) {
			return &TargetConflictError{Kind: c.kind, Root: c.root, Resolved: resolvedDest}
		}
		// The staging path, which must never be written inside a protected
		// root either. It can only collide when the destination is directly
		// inside the root (covered above via parent), but checking it
		// explicitly keeps the rule independent of suffix details.
		if pathEqualOrWithin(c.root, resolvedStaging) {
			return &TargetConflictError{Kind: c.kind, Root: c.root, Resolved: resolvedStaging}
		}
		// A protected root must not be inside the destination either: the
		// final atomic replace removes the destination's previous content.
		if pathEqualOrWithin(resolvedDest, c.root) {
			return &TargetConflictError{Kind: c.kind, Root: c.root, Resolved: resolvedDest}
		}
	}
	return nil
}
