CREATE TABLE IF NOT EXISTS ff_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    definition jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS ff_tasks (
    run_id bigint REFERENCES ff_runs,
    id text NOT NULL,
    parents text[] NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','running','succeeded','failed','blocked')),
    attempt integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    reason text NOT NULL DEFAULT '',
    PRIMARY KEY (run_id,id)
);
CREATE INDEX IF NOT EXISTS ff_ready ON ff_tasks (available_at,run_id,id) WHERE state IN ('pending','running');
CREATE TABLE IF NOT EXISTS ff_attempts (
    run_id bigint NOT NULL,
    task text NOT NULL,
    number integer NOT NULL,
    worker text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    ended_at timestamptz,
    outcome text NOT NULL DEFAULT 'running',
    PRIMARY KEY (run_id,task,number),
    FOREIGN KEY (run_id,task) REFERENCES ff_tasks
);
CREATE TABLE IF NOT EXISTS ff_receipts (
    run_id bigint NOT NULL,
    task text NOT NULL,
    PRIMARY KEY (run_id,task),
    FOREIGN KEY (run_id,task) REFERENCES ff_tasks
);
