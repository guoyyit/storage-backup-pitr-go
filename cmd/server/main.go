// Command server runs the persistent KV store with backup/restore HTTP API.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
	"github.com/guoyyit/storage-backup-pitr-go/internal/server"
)

func main() {
	var (
		addr      = flag.String("addr", envOr("ADDR", ":8080"), "HTTP listen address")
		dataDir   = flag.String("data-dir", envOr("DATA_DIR", "./data/engine"), "engine data directory")
		backupDir = flag.String("backup-dir", envOr("BACKUP_DIR", "./data/backups"), "backup repository directory")
		keepCount = flag.Int("retention-count", envInt("RETENTION_COUNT", 0), "number of chains to keep (0 = unlimited)")
		keepAge   = flag.Duration("retention-age", envDuration("RETENTION_AGE", 0), "max age of chains to keep (0 = unlimited)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng, err := engine.Open(ctx, *dataDir)
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	svc, err := backup.NewService(ctx, eng, backup.Config{
		Dir:       *backupDir,
		Retention: backup.Retention{Count: *keepCount, Age: *keepAge},
	})
	if err != nil {
		log.Fatalf("backup service: %v", err)
	}

	srv := server.New(server.Config{Addr: *addr}, eng, svc)
	log.Printf("kv store listening on %s (data=%s backups=%s)", *addr, *dataDir, *backupDir)
	if err := srv.Run(ctx); err != nil {
		log.Fatalf("server: %v", err)
	}
	if err := eng.Close(); err != nil {
		log.Printf("engine close: %v", err)
	}
	log.Print("shutdown complete")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
