// Package worker runs application-defined FlowForge tasks against PostgreSQL.
package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/farhatcodes/flowforge-oss/internal/engine"
)

// Task describes one delivery. Key stays constant across retries within the database.
type Task struct {
	RunID   int64
	ID      string
	Attempt int
	Input   json.RawMessage
}

func (t Task) Key() string { return fmt.Sprintf("%d/%s", t.RunID, t.ID) }

// Handler must honor cancellation and use Task.Key to deduplicate side effects.
type Handler func(context.Context, Task) error

// Run processes tasks whose handler is registered. Each invocation runs one task at a time.
// Stop the worker by cancelling ctx. Interrupted work becomes eligible after lease expiry.
func Run(ctx context.Context, db *sql.DB, name string, handlers map[string]Handler) error {
	if name == "" || len(handlers) == 0 {
		return errors.New("worker needs a name and at least one handler")
	}
	names := make([]string, 0, len(handlers))
	for name, h := range handlers {
		if name == "" || h == nil {
			return errors.New("invalid handler registration")
		}
		names = append(names, name)
	}
	for ctx.Err() == nil {
		claim, err := engine.NextFor(ctx, db, name, 10*time.Second, names)
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
		err = execute(ctx, db, *claim, handlers[claim.Handler])
		if errors.Is(err, engine.ErrStale) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func execute(ctx context.Context, db *sql.DB, c engine.Claim, h Handler) error {
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	renewed := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				renewed <- nil
				return
			case <-taskCtx.Done():
				renewed <- taskCtx.Err()
				return
			case <-ticker.C:
				err := engine.Renew(taskCtx, db, c, 10*time.Second)
				if err != nil {
					cancel()
					renewed <- err
					return
				}
			}
		}
	}()
	taskErr := h(taskCtx, Task{RunID: c.Run, ID: c.Task, Attempt: c.Attempt, Input: c.Input})
	close(done)
	if err := <-renewed; err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if taskErr != nil {
		slog.Error("handler failed", "run", c.Run, "task", c.Task, "attempt", c.Attempt, "error", taskErr)
	}
	return engine.Finish(ctx, db, c, taskErr)
}
