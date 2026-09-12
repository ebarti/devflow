CREATE TABLE works (
 id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL, repository TEXT, issue TEXT,
 branch TEXT, "commit" TEXT, status TEXT, stage TEXT, blocker TEXT,
 started_at TEXT, ended_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 details TEXT
);
CREATE TABLE runs (
 id TEXT PRIMARY KEY NOT NULL, work_id TEXT NOT NULL REFERENCES works(id),
 agent TEXT, role TEXT, model TEXT, effort TEXT, status TEXT,
 started_at TEXT, ended_at TEXT, duration_seconds REAL CHECK(duration_seconds >= 0),
 source_ref TEXT, recorded_at TEXT NOT NULL, details TEXT,
 UNIQUE(id, work_id)
);
CREATE TABLE results (
 id TEXT PRIMARY KEY NOT NULL, work_id TEXT NOT NULL REFERENCES works(id),
 run_id TEXT, kind TEXT NOT NULL, status TEXT NOT NULL, "commit" TEXT,
 evidence_ref TEXT, summary TEXT, recorded_at TEXT NOT NULL, details TEXT,
 FOREIGN KEY(run_id, work_id) REFERENCES runs(id, work_id)
);
CREATE TABLE findings (
 id TEXT PRIMARY KEY NOT NULL, work_id TEXT NOT NULL REFERENCES works(id),
 summary TEXT NOT NULL, severity TEXT, status TEXT, "commit" TEXT,
 evidence_ref TEXT, fix_ref TEXT, thread_ref TEXT,
 recorded_at TEXT NOT NULL, updated_at TEXT NOT NULL, details TEXT
);
CREATE TABLE usage (
 id TEXT PRIMARY KEY NOT NULL, run_id TEXT REFERENCES runs(id), agent TEXT, model TEXT,
 input_tokens INTEGER CHECK(input_tokens >= 0),
 cached_input_tokens INTEGER CHECK(cached_input_tokens >= 0),
 cache_write_tokens INTEGER CHECK(cache_write_tokens >= 0),
 output_tokens INTEGER CHECK(output_tokens >= 0),
 reasoning_output_tokens INTEGER CHECK(reasoning_output_tokens >= 0),
 estimated_cost_usd REAL CHECK(estimated_cost_usd >= 0),
 estimated_credits REAL CHECK(estimated_credits >= 0),
 source_ref TEXT, recorded_at TEXT NOT NULL, details TEXT,
 CHECK(cached_input_tokens IS NULL OR input_tokens IS NULL OR cached_input_tokens <= input_tokens),
 CHECK(cache_write_tokens IS NULL OR input_tokens IS NULL OR cache_write_tokens <= input_tokens),
 CHECK(cached_input_tokens IS NULL OR cache_write_tokens IS NULL OR input_tokens IS NULL
       OR cached_input_tokens + cache_write_tokens <= input_tokens),
 CHECK(reasoning_output_tokens IS NULL OR output_tokens IS NULL OR reasoning_output_tokens <= output_tokens)
);
CREATE TABLE usage_allocations (
 usage_id TEXT NOT NULL REFERENCES usage(id), work_id TEXT NOT NULL REFERENCES works(id),
 weight REAL NOT NULL CHECK(weight >= 0 AND weight <= 1),
 PRIMARY KEY(usage_id, work_id)
);
CREATE TRIGGER usage_allocation_total BEFORE INSERT ON usage_allocations
 WHEN NEW.weight + COALESCE((SELECT SUM(weight) FROM usage_allocations WHERE usage_id=NEW.usage_id),0) > 1.000000000001
 BEGIN SELECT RAISE(ABORT, 'usage allocation exceeds one'); END;
CREATE TRIGGER usage_allocation_update BEFORE UPDATE ON usage_allocations
 WHEN NEW.weight + COALESCE((SELECT SUM(weight) FROM usage_allocations WHERE usage_id=NEW.usage_id AND NOT (usage_id=OLD.usage_id AND work_id=OLD.work_id)),0) > 1.000000000001
 BEGIN SELECT RAISE(ABORT, 'usage allocation exceeds one'); END;
CREATE TABLE history (
 id TEXT PRIMARY KEY NOT NULL, work_id TEXT REFERENCES works(id),
 entity TEXT NOT NULL, entity_id TEXT NOT NULL, action TEXT NOT NULL,
 occurred_at TEXT, recorded_at TEXT NOT NULL, stage TEXT, previous_stage TEXT,
 status TEXT, started_at TEXT, ended_at TEXT, note TEXT, source_ref TEXT, details TEXT
);
CREATE TABLE imports (
 source TEXT PRIMARY KEY NOT NULL, imported_at TEXT NOT NULL, report TEXT NOT NULL
);
CREATE INDEX runs_work ON runs(work_id);
CREATE INDEX results_work ON results(work_id);
CREATE INDEX findings_work ON findings(work_id);
CREATE INDEX allocations_work ON usage_allocations(work_id);
CREATE INDEX history_work ON history(work_id);
