package backup

import (
	"context"

	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

func openEngine(ctx context.Context, dir string) (*engine.Engine, error) {
	return engine.Open(ctx, dir)
}
