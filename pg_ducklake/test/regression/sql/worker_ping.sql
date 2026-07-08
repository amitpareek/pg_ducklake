-- M0: round-trips a serialized DuckDB DataChunk (holding 42) through the shared
-- analytical DuckDB worker over shared memory.
SELECT ducklake.worker_ping();
