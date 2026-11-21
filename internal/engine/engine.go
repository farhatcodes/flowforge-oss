package engine

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/farhatcodes/flowforge-oss/internal/workflow"
	"github.com/lib/pq"
)

//go:embed schema.sql
var schema string

func Migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(70539102)"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	return tx.Commit()
}

func Submit(ctx context.Context, db *sql.DB, g workflow.Graph) (int64, error) {
	if _, err := g.Order(); err != nil {
		return 0, err
	}
	if len(g.Tasks) == 0 {
		return 0, errors.New("workflow needs at least one task")
	}
	def, err := json.Marshal(g)
	if err != nil {
		return 0, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var run int64
	if err = tx.QueryRowContext(ctx, "INSERT INTO ff_runs(definition) VALUES($1) RETURNING id", string(def)).Scan(&run); err != nil {
		return 0, err
	}
	for _, t := range g.Tasks {
		if _, err = tx.ExecContext(ctx, "INSERT INTO ff_tasks(run_id,id,parents,handler,input) VALUES($1,$2,coalesce($3::text[],'{}'),coalesce(nullif($4,''),'receipt'),nullif($5,'')::jsonb)", run, t.ID, pq.Array(t.DependsOn), t.Handler, string(t.Input)); err != nil {
			return 0, err
		}
	}
	return run, tx.Commit()
}

type Claim struct {
	Handler string
	Input   json.RawMessage
	Run     int64
	Task    string
	Attempt int
}

const maxAttempts = 3

// A worker locks a single eligible task. Parent successes are terminal.
const eligible = `SELECT t.run_id,t.id,t.attempt,t.state,t.handler,coalesce(t.input,'null'::jsonb) FROM ff_tasks t
 WHERE t.state IN ('running','pending') AND t.available_at<=clock_timestamp()
 AND t.handler=ANY($1::text[])
 AND NOT EXISTS (SELECT 1 FROM ff_tasks p WHERE p.run_id=t.run_id AND p.id=ANY(t.parents) AND p.state<>'succeeded')
 ORDER BY t.available_at,t.run_id,t.id LIMIT 1 FOR UPDATE OF t SKIP LOCKED`

func Next(ctx context.Context, db *sql.DB, worker string, lease time.Duration) (*Claim, error) {
	return NextFor(ctx, db, worker, lease, []string{"receipt"})
}

func NextFor(ctx context.Context, db *sql.DB, worker string, lease time.Duration, handlers []string) (*Claim, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var c Claim
	var state string
	var input []byte
	err = tx.QueryRowContext(ctx, eligible, pq.Array(handlers)).Scan(&c.Run, &c.Task, &c.Attempt, &state, &c.Handler, &input)
	c.Input = input
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if state == "running" {
		if err = endAttempt(ctx, tx, c, "lease expired"); err != nil {
			return nil, err
		}
		if err = retry(ctx, tx, c, "lease expired"); err != nil {
			return nil, err
		}
		return nil, tx.Commit()
	}
	c.Attempt++
	_, err = tx.ExecContext(ctx, `UPDATE ff_tasks SET state='running',attempt=$3,available_at=clock_timestamp()+$4 * interval '1 millisecond',reason='' WHERE run_id=$1 AND id=$2`, c.Run, c.Task, c.Attempt, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO ff_attempts(run_id,task,number,worker) VALUES($1,$2,$3,$4)", c.Run, c.Task, c.Attempt, worker)
	if err != nil {
		return nil, err
	}
	return &c, tx.Commit()
}

func endAttempt(ctx context.Context, tx *sql.Tx, c Claim, outcome string) error {
	_, err := tx.ExecContext(ctx, "UPDATE ff_attempts SET ended_at=clock_timestamp(),outcome=$4 WHERE run_id=$1 AND task=$2 AND number=$3", c.Run, c.Task, c.Attempt, outcome)
	return err
}

func retry(ctx context.Context, tx *sql.Tx, c Claim, reason string) error {
	if c.Attempt < maxAttempts {
		_, err := tx.ExecContext(ctx, `UPDATE ff_tasks SET state='pending',reason=$3,available_at=clock_timestamp()+$4 * interval '1 second' WHERE run_id=$1 AND id=$2`, c.Run, c.Task, reason, 1<<(c.Attempt-1))
		return err
	}
	_, err := tx.ExecContext(ctx, "UPDATE ff_tasks SET state='failed',reason=$3 WHERE run_id=$1 AND id=$2", c.Run, c.Task, reason)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `WITH RECURSIVE descendants(id) AS (
 SELECT id FROM ff_tasks WHERE run_id=$1 AND $2=ANY(parents)
 UNION SELECT t.id FROM ff_tasks t JOIN descendants d ON d.id=ANY(t.parents) WHERE t.run_id=$1)
 UPDATE ff_tasks SET state='blocked',reason='failed ancestor: ' || $2 WHERE run_id=$1 AND id IN (SELECT id FROM descendants) AND state='pending'`, c.Run, c.Task)
	return err
}

var ErrStale = errors.New("attempt lease expired or superseded")

func Renew(ctx context.Context, db *sql.DB, c Claim, lease time.Duration) error {
	result, err := db.ExecContext(ctx, `UPDATE ff_tasks
 SET available_at=clock_timestamp()+$4 * interval '1 millisecond'
 WHERE run_id=$1 AND id=$2 AND attempt=$3 AND state='running' AND available_at>clock_timestamp()`, c.Run, c.Task, c.Attempt, lease.Milliseconds())
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrStale
	}
	return nil
}

func Finish(ctx context.Context, db *sql.DB, c Claim, taskErr error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var valid bool
	err = tx.QueryRowContext(ctx, `SELECT available_at>clock_timestamp() FROM ff_tasks WHERE run_id=$1 AND id=$2 AND attempt=$3 AND state='running' FOR UPDATE`, c.Run, c.Task, c.Attempt).Scan(&valid)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStale
	}
	if err != nil {
		return err
	}
	if !valid {
		return ErrStale
	}
	outcome := "succeeded"
	if taskErr != nil {
		outcome = taskErr.Error()
		err = retry(ctx, tx, c, outcome)
	} else {
		_, err = tx.ExecContext(ctx, "UPDATE ff_tasks SET state='succeeded' WHERE run_id=$1 AND id=$2", c.Run, c.Task)
	}
	if err != nil {
		return err
	}
	if err = endAttempt(ctx, tx, c, outcome); err != nil {
		return err
	}
	return tx.Commit()
}

// Record is the built-in task. Its stable key makes replay safe across attempts.
func Record(ctx context.Context, db *sql.DB, c Claim) error {
	_, err := db.ExecContext(ctx, "INSERT INTO ff_receipts(run_id,task) VALUES($1,$2) ON CONFLICT DO NOTHING", c.Run, c.Task)
	return err
}

func Status(ctx context.Context, db *sql.DB, run int64) (json.RawMessage, error) {
	var result []byte
	err := db.QueryRowContext(ctx, `SELECT jsonb_build_object('run',run_id,'state',
 CASE WHEN bool_and(state='succeeded') THEN 'succeeded' WHEN bool_or(state IN ('pending','running')) THEN 'active' ELSE 'failed' END,
 'tasks',jsonb_agg(jsonb_build_object('id',id,'handler',handler,'state',state,'reason',reason,'attempts',
 COALESCE((SELECT jsonb_agg(to_jsonb(a)-'run_id'-'task' ORDER BY number) FROM ff_attempts a WHERE a.run_id=t.run_id AND a.task=t.id),'[]'::jsonb)) ORDER BY id))
 FROM ff_tasks t WHERE run_id=$1 GROUP BY run_id`, run).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("unknown run %d", run)
	}
	return result, err
}
