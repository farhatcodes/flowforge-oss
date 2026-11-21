package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/farhatcodes/flowforge-oss/internal/engine"
	"github.com/farhatcodes/flowforge-oss/internal/workflow"
	"github.com/farhatcodes/flowforge-oss/worker"
)

// The child stays alive after publishing to make the crash window deterministic.
func TestCrashWorker(t *testing.T) {
	if os.Getenv("FF_IMPORT_TEST_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	db, err := sql.Open("postgres", os.Getenv("FF_IMPORT_TEST_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := os.Getenv("FF_IMPORT_TEST_DIR")
	h := func(ctx context.Context, task worker.Task) error {
		if err := report("../import.csv", dir)(ctx, task); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	if err := worker.Run(context.Background(), db, "original", map[string]worker.Handler{"csv-report": h}); err != nil {
		t.Fatal(err)
	}
}
func TestImportAfterWorkerCrash(t *testing.T) {
	dsn := os.Getenv("FLOWFORGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FLOWFORGE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("ff_import_%d", time.Now().UnixNano())
	if _, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec("DROP SCHEMA " + schema + " CASCADE")
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = engine.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../import.json")
	if err != nil {
		t.Fatal(err)
	}
	var g workflow.Graph
	if err = json.Unmarshal(data, &g); err != nil {
		t.Fatal(err)
	}
	id, err := engine.Submit(ctx, db, g)
	if err != nil {
		t.Fatal(err)
	}
	if c, err := engine.Next(ctx, db, "receipt-only", time.Minute); err != nil || c != nil {
		t.Fatalf("receipt worker claimed import: %v %v", c, err)
	}
	dir := t.TempDir()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashWorker$")
	child.Env = append(os.Environ(), "FF_IMPORT_TEST_CHILD=1", "FF_IMPORT_TEST_URL="+u.String(), "FF_IMPORT_TEST_DIR="+dir)
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			child.Process.Kill()
			child.Wait()
		}
	}()
	for {
		files, err := filepath.Glob(filepath.Join(dir, "report-*.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Wait beyond the initial lease to prove a live handler renewed it.
	time.Sleep(11 * time.Second)
	var live bool
	if err = db.QueryRowContext(ctx, "SELECT available_at>clock_timestamp() FROM ff_tasks WHERE run_id=$1", id).Scan(&live); err != nil || !live {
		t.Fatalf("lease renewal: %t %v", live, err)
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	child.Wait()
	replacement, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(replacement, db, "replacement", map[string]worker.Handler{"csv-report": report("../import.csv", dir)})
	}()
	defer func() {
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("worker exit: %v", err)
		}
	}()
	for {
		var state string
		var attempt int
		if err = db.QueryRowContext(ctx, "SELECT state,attempt FROM ff_tasks WHERE run_id=$1", id).Scan(&state, &attempt); err != nil {
			t.Fatal(err)
		}
		if state == "succeeded" {
			if attempt != 2 {
				t.Fatal(attempt)
			}
			break
		}
		if state == "failed" {
			t.Fatal(state)
		}
		time.Sleep(100 * time.Millisecond)
	}
	files, err := filepath.Glob(filepath.Join(dir, "report-*.json"))
	if err != nil || len(files) != 1 {
		t.Fatal(files, err)
	}
	data, err = os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Rows, Total int
		Key         string
	}
	if err = json.Unmarshal(data, &result); err != nil || result.Rows != 2 || result.Total != 8 || result.Key != fmt.Sprintf("%d/summarize", id) {
		t.Fatal(string(data), err)
	}
}
