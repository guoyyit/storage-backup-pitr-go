package backup

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCanonicalPath(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatal(err)
	}
	// On some systems (e.g. macOS /var -> /private/var) the temp base itself
	// is reached through a symlink; canonicalize expectations the same way.
	var err error
	real, err = canonicalPath(real)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(real, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"clean", filepath.Join(base, "real"), real},
		{"dup separators", filepath.Join(base, "real//sub"), filepath.Join(real, "sub")},
		{"dot segments", filepath.Join(base, "real", ".", "sub", "..", "sub"), filepath.Join(real, "sub")},
		{"symlink existing", filepath.Join(link, "sub"), filepath.Join(real, "sub")},
		{"missing tail", filepath.Join(link, "sub", "nope", "deep"), filepath.Join(real, "sub", "nope", "deep")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalPath(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("canonicalPath(%q)=%q want %q", tc.in, got, tc.want)
			}
		})
	}

	// Relative path resolves against the working directory.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	want, err := canonicalPath(filepath.Join(wd, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := canonicalPath(filepath.Join("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("relative canonical=%q want %q", got, want)
	}
}

func TestValidateRestoreTargetConflicts(t *testing.T) {
	work := t.TempDir()
	engineDir := filepath.Join(work, "engine")
	repoDir := filepath.Join(work, "repo")
	if err := os.MkdirAll(engineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Symlink pointing at the engine dir.
	engineLink := filepath.Join(work, "engine-link")
	if err := os.Symlink(engineDir, engineLink); err != nil {
		t.Fatal(err)
	}
	// Symlink pointing inside the repo.
	repoInnerLink := filepath.Join(work, "repo-inner-link")
	if err := os.MkdirAll(filepath.Join(repoDir, "chain-x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(repoDir, "chain-x"), repoInnerLink); err != nil {
		t.Fatal(err)
	}

	bad := []struct {
		name string
		dest string
		kind TargetConflictKind
	}{
		{"engine itself", engineDir, ConflictEngineDir},
		{"inside engine", filepath.Join(engineDir, "restore"), ConflictEngineDir},
		{"engine via dup separator", filepath.Join(work, "engine", "", "x"), ConflictEngineDir},
		{"engine via symlink", engineLink, ConflictEngineDir},
		{"inside engine via symlink", filepath.Join(engineLink, "sub"), ConflictEngineDir},
		{"repo itself", repoDir, ConflictBackupRepo},
		{"inside repo", filepath.Join(repoDir, "chain-x"), ConflictBackupRepo},
		{"inside repo via symlink", repoInnerLink, ConflictBackupRepo},
		{"engine trailing slash", engineDir + string(filepath.Separator), ConflictEngineDir},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRestoreTarget(tc.dest, engineDir, repoDir)
			if err == nil {
				t.Fatalf("dest %q must be rejected", tc.dest)
			}
			var ce *TargetConflictError
			if !errors.As(err, &ce) {
				t.Fatalf("want *TargetConflictError, got %T: %v", err, err)
			}
			if ce.Kind != tc.kind {
				t.Fatalf("kind=%s want %s", ce.Kind, tc.kind)
			}
			if !errors.Is(err, ErrRestoreTargetConflict) {
				t.Fatalf("errors.Is must match ErrRestoreTargetConflict: %v", err)
			}
		})
	}

	good := []string{
		filepath.Join(work, "independent", "r"),
		filepath.Join(work, "r"),
		filepath.Join(work, "sibling", "deep", "r"),
	}
	for _, dest := range good {
		if err := validateRestoreTarget(dest, engineDir, repoDir); err != nil {
			t.Fatalf("dest %q must be allowed: %v", dest, err)
		}
	}
}
