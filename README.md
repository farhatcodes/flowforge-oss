# FlowForge

FlowForge is a Go CLI and PostgreSQL-backed worker for dependency graphs. Submit a JSON workflow, run one or more workers, and inspect every attempt. The alpha has one built-in task handler: it writes a durable receipt keyed by run and task. Task IDs such as `invoice` name steps in the example. Each step runs that same receipt handler.

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
