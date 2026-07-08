-- ducklake.worker_ping() -- diagnostic for the shared DuckDB worker.
-- Hands the per-cluster worker a shared-memory queue; the worker serializes a
-- one-row DataChunk holding 42 into it and this returns the deserialized value.
-- Exercises the cross-process dsm + shm_mq + DuckDB DataChunk transport.
-- Unstable/debug API: superuser-only.
CREATE FUNCTION ducklake.worker_ping()
    RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'ducklake_worker_ping';
REVOKE ALL ON FUNCTION ducklake.worker_ping() FROM PUBLIC;

-- ducklake.worker_eval(query) -- diagnostic: runs `query` in the shared engine
-- worker and returns the first column of the first row as bigint. Unstable/debug
-- API: superuser-only.
CREATE FUNCTION ducklake.worker_eval(query text)
    RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'ducklake_worker_eval';
REVOKE ALL ON FUNCTION ducklake.worker_eval(text) FROM PUBLIC;

-- ducklake.worker_stats() -- sessions this database's shared engine worker has
-- accepted since it started; lets tests assert a query really dispatched.
-- Unstable/debug API: superuser-only.
CREATE FUNCTION ducklake.worker_stats()
    RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'ducklake_worker_stats';
REVOKE ALL ON FUNCTION ducklake.worker_stats() FROM PUBLIC;
