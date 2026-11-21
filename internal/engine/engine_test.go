package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/farhatcodes/flowforge-oss/internal/workflow"
)

func database(t testing.TB) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOWFORGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FLOWFORGE_TEST_DATABASE_URL for PostgreSQL tests")
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := fmt.Sprintf("ff_test_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Error(err)
		}
	})
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
	t.Cleanup(func() { db.Close() })
	if err = Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}
func TestRecoveryAndJoin(t *testing.T) {
	db := database(t)
	ctx := context.Background()
	g := workflow.Graph{Tasks: []workflow.Task{{ID: "a"}, {ID: "b"}, {ID: "join", DependsOn: []string{"a", "b"}}}}
	run, err := Submit(ctx, db, g)
	if err != nil {
		t.Fatal(err)
	}
	claims := make(chan *Claim, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := Next(ctx, db, fmt.Sprint(i), time.Minute)
			if err != nil {
				t.Error(err)
			}
			if c != nil {
				claims <- c
			}
		}(i)
	}
	wg.Wait()
	close(claims)
	if len(claims) != 2 {
		t.Fatalf("got %d claims", len(claims))
	}
	first, other := <-claims, <-claims
	if first.Task == other.Task || first.Task == "join" || other.Task == "join" {
		t.Fatal(first, other)
	}
	if err = Finish(ctx, db, *other, nil); err != nil {
		t.Fatal(err)
	}
	if err = Record(ctx, db, *first); err != nil {
		t.Fatal(err)
	}
	expire := func() {
		t.Helper()
		if _, err := db.Exec("UPDATE ff_tasks SET available_at=now() WHERE run_id=$1 AND id=$2", run, first.Task); err != nil {
			t.Fatal(err)
		}
	}
	expire()
	next := func() *Claim {
		t.Helper()
		c, err := Next(ctx, db, "replacement", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	if c := next(); c != nil {
		t.Fatal(c)
	} // expires the previous attempt
	expire() // advance through backoff
	replacement := next()
	if replacement == nil || replacement.Task != first.Task || replacement.Attempt != 2 {
		t.Fatal(replacement)
	}
	if err = Finish(ctx, db, *first, nil); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if err = Record(ctx, db, *replacement); err != nil {
		t.Fatal(err)
	}
	if err = Finish(ctx, db, *replacement, nil); err != nil {
		t.Fatal(err)
	}
	var receipts int
	if err = db.QueryRow("SELECT count(*) FROM ff_receipts WHERE run_id=$1", run).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatal(receipts, err)
	}
	join := next()
	if join == nil || join.Task != "join" {
		t.Fatal(join)
	}
	if err = Finish(ctx, db, *join, nil); err != nil {
		t.Fatal(err)
	}
	data, err := Status(ctx, db, run)
	var status struct{ State string }
	if err != nil || json.Unmarshal(data, &status) != nil || status.State != "succeeded" {
		t.Fatal(string(data), err)
	}
}
func TestFailureBlocksDescendants(t *testing.T) {
	db := database(t)
	ctx := context.Background()
	run, err := Submit(ctx, db, workflow.Graph{Tasks: []workflow.Task{{ID: "a"}, {ID: "b", DependsOn: []string{"a"}}, {ID: "c", DependsOn: []string{"b"}}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= maxAttempts; i++ {
		c, err := Next(ctx, db, "w", time.Minute)
		if err != nil || c == nil || c.Attempt != i {
			t.Fatal(c, err)
		}
		if err = Finish(ctx, db, *c, errors.New("handler failed")); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec("UPDATE ff_tasks SET available_at=now() WHERE run_id=$1", run); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err = db.QueryRow("SELECT count(*) FROM ff_tasks WHERE run_id=$1 AND state='blocked'", run).Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
}

func BenchmarkExecution(b *testing.B) {
	for _, shape := range []string{"wide", "deep"} {
		for _, workers := range []int{1, 4} {
			b.Run(fmt.Sprintf("%s/workers-%d", shape, workers), func(b *testing.B) {
				db := database(b)
				ctx := context.Background()
				g := workflow.Graph{}
				for i := 0; i < 32; i++ {
					task := workflow.Task{ID: fmt.Sprintf("task-%02d", i)}
					if shape == "deep" && i > 0 {
						task.DependsOn = []string{g.Tasks[i-1].ID}
					}
					g.Tasks = append(g.Tasks, task)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := Submit(ctx, db, g); err != nil {
						b.Fatal(err)
					}
					var wg sync.WaitGroup
					for w := 0; w < workers; w++ {
						wg.Add(1)
						go func(w int) {
							defer wg.Done()
							for {
								c, err := Next(ctx, db, fmt.Sprint(w), time.Minute)
								if err != nil {
									b.Error(err)
									return
								}
								if c == nil {
									var active bool
									if err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM ff_tasks WHERE state IN ('pending','running'))").Scan(&active); err != nil {
										b.Error(err)
										return
									}
									if !active {
										return
									}
									time.Sleep(time.Millisecond)
									continue
								}
								if err = Record(ctx, db, *c); err == nil {
									err = Finish(ctx, db, *c, nil)
								}
								if err != nil {
									b.Error(err)
									return
								}
							}
						}(w)
					}
					wg.Wait()
				}
				b.ReportMetric(float64(b.N*32)/b.Elapsed().Seconds(), "tasks/s")
			})
		}
	}
}
