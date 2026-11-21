# FlowForge

FlowForge is a Go CLI and PostgreSQL-backed worker for dependency graphs. Submit a JSON workflow, run workers, and inspect each attempt. The CLI handles receipt tasks. Applications can register their own Go handlers with the public `worker` package.

## Run a CSV import

With Go and a PostgreSQL `DATABASE_URL`, stop any workers from the previous alpha before upgrading. Run the additive migration and restart workers with the new binaries before submitting typed tasks. Existing workflows default to the receipt handler.

```sh
go run ./cmd/flowforge migrate
go run ./cmd/flowforge submit examples/import.json
go run ./examples/import-worker examples/import.csv reports
```

In another terminal run `go run ./cmd/flowforge status RUN_ID` with the ID printed by submit. The report in `reports/` should contain two rows and a total of eight. To process your own file, pass a CSV with a nonnegative integer quantity column. Workers serving this example queue must use the same immutable input file and shared report directory. Use a filesystem that supports hard links. The worker publishes a complete report under a stable key so replay after a crash produces one report.

To implement another worker, import `github.com/farhatcodes/flowforge-oss/worker` and pass named functions to `worker.Run`. A handler receives a context, JSON input, attempt number, and a stable `Task.Key()` for deduplication. Return an error to enter the retry policy. Workers claim only registered handler names. A task waits until a capable worker is running. The SDK renews leases during execution and cancels the handler context on lease loss or shutdown. Handlers must observe cancellation and make external effects idempotent.

## Try the planner

With Go 1.23 or later, run:

```sh
go run ./cmd/flowforge plan examples/order.json
```

The example orders `validate`, `invoice`, `reserve`, then `notify`. Both middle tasks become eligible after validation. Notification waits for both. The planner breaks ties alphabetically. Each task needs a unique, nonempty `id` and may list prerequisite IDs in `depends_on`. Invalid dependencies and cycles return a nonzero exit code.

## Run the example with Docker Compose

```sh
docker compose build cli
docker compose run --rm cli migrate
docker compose run --rm cli submit /examples/order.json
docker compose run --rm cli worker
```

Submission prints a numeric run ID. The worker stays up to claim more tasks. In another terminal, replace `1` with the returned ID:

```sh
docker compose run --rm cli status 1
```

Status includes run state, task state, the reason for a failure or block, and attempt history with worker IDs and timestamps. Start another worker with the same command to process eligible tasks concurrently. Use Ctrl+C to stop a foreground worker. Compose stores the database in a named volume.

For a native binary, use `go build -o flowforge ./cmd/flowforge` (use `flowforge.exe` on Windows). Set `DATABASE_URL` to a PostgreSQL URL, then use the same subcommands with local file paths. Run `migrate` once before the first submission. It is safe to repeat.

## Reproduce worker recovery

1. Stop existing workers and submit a fresh copy of the example.
2. Start a worker with a pause between recording the receipt and acknowledging completion:

   ```sh
   docker compose run --rm --name ff-recovery -e FLOWFORGE_RECEIPT_DELAY=8s cli worker
   ```

3. As soon as its first `receipt recorded` log appears, run `docker kill ff-recovery` from another terminal.
4. Start an ordinary worker. After the ten-second lease expires and the retry delay elapses, it reclaims the task. Inspect the run. The interrupted task should show `lease expired` followed by `succeeded`. Each other task should have one successful attempt.

Claims use PostgreSQL row locks. The attempt number fences completion by a superseded worker. Task execution is at least once. The receipt's unique key makes repeating its side effect safe. A failed or expired attempt becomes eligible again after one second, then two seconds. The third failure marks the task failed and blocks its descendants. Independent branches can finish. To see this failure path, repeat the interruption on the same root task for all three attempts, then leave a worker running to process the last expiry.

## Test and benchmark

`go test ./...` runs planner and CLI tests. Set `FLOWFORGE_TEST_DATABASE_URL` to a PostgreSQL URL with permission to create schemas to also run the integration suite. Each test creates and cleans up its own schema. CI supplies PostgreSQL and runs the race detector and `go vet`.

```sh
go test ./internal/engine -bench BenchmarkExecution -benchtime=3x
```

Local reference run on Windows amd64, Intel Core Ultra 7 165U, Go 1.23.12 and PostgreSQL 16.4 on loopback. Each measurement averages three 32-task runs with the built-in receipt handler.

| Graph | Workers | Mean workflow duration | Tasks/second |
| --- | ---: | ---: | ---: |
| Independent tasks | 1 | 91 ms | 353 |
| Independent tasks | 4 | 117 ms | 274 |
| Dependency chain | 1 | 105 ms | 304 |
| Dependency chain | 4 | 243 ms | 132 |

Extra workers increased latency on the serial chain in this sample. The benchmark workers poll on a one-millisecond interval while waiting for peers. The CLI uses a 200-millisecond idle interval. Measure queue wait and database lock time separately before tuning worker count or changing the scheduler.
