// An external worker can use the public worker package without importing FlowForge internals.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"

	"github.com/farhatcodes/flowforge-oss/worker"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: import-worker INPUT.csv REPORT_DIRECTORY")
	}
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	h := report(os.Args[1], os.Args[2])
	err = worker.Run(ctx, db, fmt.Sprintf("import-%d", os.Getpid()), map[string]worker.Handler{"csv-report": h})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func report(source, dir string) worker.Handler {
	return func(ctx context.Context, t worker.Task) error {
		var input struct {
			Column string `json:"column"`
		}
		if err := json.Unmarshal(t.Input, &input); err != nil {
			return err
		}
		key := sha256.Sum256([]byte(t.Key()))
		dest := filepath.Join(dir, fmt.Sprintf("report-%x.json", key))
		if _, err := os.Stat(dest); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		f, err := os.Open(source)
		if err != nil {
			return err
		}
		defer f.Close()
		reader := csv.NewReader(f)
		header, err := reader.Read()
		if err != nil {
			return err
		}
		index := -1
		for i, v := range header {
			if v == input.Column {
				if index != -1 {
					return fmt.Errorf("duplicate column %q", v)
				}
				index = i
			}
		}
		if index < 0 {
			return fmt.Errorf("missing column %q", input.Column)
		}
		var rows, total int64
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			n, err := strconv.ParseInt(row[index], 10, 32)
			if err != nil || n < 0 {
				return fmt.Errorf("invalid quantity on data row %d", rows+1)
			}
			total += n
			rows++
		}
		if err = os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		tmp, err := os.CreateTemp(dir, ".report-*")
		if err != nil {
			return err
		}
		defer os.Remove(tmp.Name())
		defer tmp.Close()
		if err = json.NewEncoder(tmp).Encode(map[string]any{"key": t.Key(), "rows": rows, "total": total}); err != nil {
			return err
		}
		if err = tmp.Sync(); err != nil {
			return err
		}
		if err = tmp.Close(); err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		// Publish a complete file once, even if two deliveries overlap.
		if err = os.Link(tmp.Name(), dest); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		log.Printf("published %s for %s", dest, t.Key())
		return nil
	}
}
