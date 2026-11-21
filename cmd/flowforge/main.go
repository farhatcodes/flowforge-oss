package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/farhatcodes/flowforge-oss/internal/engine"
	"github.com/farhatcodes/flowforge-oss/internal/workflow"
)

const usage = "usage: flowforge plan FILE | migrate | submit FILE | worker | status RUN"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func load(path string) (workflow.Graph, error) {
	var g workflow.Graph
	f, err := os.Open(path)
	if err != nil {
		return g, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err = d.Decode(&g); err != nil {
		return g, fmt.Errorf("%s: %w", path, err)
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return g, fmt.Errorf("%s: expected one JSON document", path)
	}
	if len(g.Tasks) == 0 {
		return g, errors.New("workflow needs at least one task")
	}
	return g, nil
}
func run(ctx context.Context, args []string, out io.Writer) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if len(args) == 0 {
		demo := workflow.Graph{Tasks: []workflow.Task{
			{ID: "validate"},
			{ID: "reserve", DependsOn: []string{"validate"}},
			{ID: "notify", DependsOn: []string{"reserve"}},
		}}
		order, err := demo.Order()
		if err != nil {
			return err
		}
		return enc.Encode(order)
	}
	command := args[0]
	var g workflow.Graph
	var err error
	var id int64
	switch command {
	case "plan", "submit":
		if len(args) != 2 {
			return errors.New(usage)
		}
		g, err = load(args[1])
		if err != nil {
			return err
		}
		if command == "plan" {
			order, err := g.Order()
			if err != nil {
				return err
			}
			return enc.Encode(order)
		}
	case "status":
		if len(args) != 2 {
			return errors.New(usage)
		}
		id, err = strconv.ParseInt(args[1], 10, 64)
		if err != nil || id < 1 {
			return errors.New("expected a positive run ID")
		}
	case "migrate", "worker":
		if len(args) != 1 {
			return errors.New(usage)
		}
	default:
		return errors.New(usage)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("set DATABASE_URL to a PostgreSQL connection URL")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	switch command {
	case "migrate":
		return engine.Migrate(ctx, db)
	case "submit":
		id, err = engine.Submit(ctx, db, g)
		if err != nil {
			return err
		}
		return enc.Encode(id)
	case "status":
		status, err := engine.Status(ctx, db, id)
		if err != nil {
			return err
		}
		return enc.Encode(status)
	default:
		return work(ctx, db)
	}
}

func work(ctx context.Context, db *sql.DB) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	worker := fmt.Sprintf("%s-%d", host, os.Getpid())
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("worker", worker)
	delay := time.Duration(0)
	if value := os.Getenv("FLOWFORGE_RECEIPT_DELAY"); value != "" {
		delay, err = time.ParseDuration(value)
		if err != nil || delay < 0 || delay >= 10*time.Second {
			return errors.New("FLOWFORGE_RECEIPT_DELAY must be between 0 and 10s (exclusive upper bound)")
		}
	}
	for ctx.Err() == nil {
		claim, err := engine.Next(ctx, db, worker, 10*time.Second)
		if err != nil {
			return err
		}
		if claim == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		log := logger.With("run", claim.Run, "task", claim.Task, "attempt", claim.Attempt)
		taskErr := engine.Record(ctx, db, *claim)
		if taskErr == nil {
			log.Info("receipt recorded")
			if delay > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
		}
		err = engine.Finish(ctx, db, *claim, taskErr)
		if errors.Is(err, engine.ErrStale) {
			log.Warn("claim expired")
			continue
		}
		if err != nil {
			return err
		}
		if taskErr != nil {
			log.Error("attempt failed", "error", taskErr)
		} else {
			log.Info("attempt succeeded")
		}
	}
	return ctx.Err()
}
