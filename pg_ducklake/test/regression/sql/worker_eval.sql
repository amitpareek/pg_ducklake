-- Phase 1: ship analytical queries to the shared DuckDB worker and return the
-- first result cell, exercising SQL execution and result-chunk streaming over
-- shared memory.
SELECT ducklake.worker_eval('SELECT 6 * 7');
SELECT ducklake.worker_eval('SELECT sum(x) FROM (VALUES (10), (20), (12)) t(x)');
