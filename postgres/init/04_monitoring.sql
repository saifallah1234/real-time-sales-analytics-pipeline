-- Pipeline health-check results written by the cdc-monitor collector and read
-- by Grafana. Idempotent, so it can also be applied to an existing volume.

\connect cdc_project

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.check_results (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id      UUID NOT NULL,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    check_name  TEXT NOT NULL,
    subject     TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('ok', 'warn', 'fail')),
    value       DOUBLE PRECISION,
    detail      TEXT
);

CREATE INDEX IF NOT EXISTS check_results_checked_at_idx
    ON monitoring.check_results (checked_at);

CREATE INDEX IF NOT EXISTS check_results_check_idx
    ON monitoring.check_results (check_name, subject, checked_at);

-- The collector connects as saif through cdc_administrator.
GRANT USAGE ON SCHEMA monitoring TO cdc_administrator;
GRANT SELECT, INSERT, DELETE ON monitoring.check_results TO cdc_administrator;

-- Read-only Grafana login (local development credentials).
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader WITH LOGIN PASSWORD 'grafana_reader';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE cdc_project TO grafana_reader;
GRANT USAGE ON SCHEMA monitoring TO grafana_reader;
GRANT SELECT ON monitoring.check_results TO grafana_reader;
