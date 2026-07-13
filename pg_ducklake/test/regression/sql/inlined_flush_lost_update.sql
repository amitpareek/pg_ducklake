-- Regression: flush_inlined_data must not revert a row to an older version.
--
-- On the Postgres backend, PgDuckLakeMetadataManager::ReadAllInlinedDataForFlush
-- read the inline versions ORDER BY row_id only. When a row_id has several versions
-- (two or more updates before one flush), the tie fell to Postgres heap/scan order,
-- which the end_snapshot-stamping UPDATEs scramble away from begin_snapshot order.
-- The flush's delete-position query derives ordinals from
-- ROW_NUMBER() OVER (ORDER BY [sort,] row_id, begin_snapshot), so the physical read
-- order must match it; when it did not, the delete file tombstoned the live version
-- and the committed value silently reverted after the flush.
--
-- Fix: order the read by (row_id, begin_snapshot), matching the delete-position query
-- and upstream DuckLake commit 8dd38ce0. This test reverts (all values correct) with
-- the fix and fails (older values resurface) without it.

CALL ducklake.set_option('data_inlining_row_limit', 1000);  -- keep everything inline until the explicit flush

CREATE TABLE flr_fact (part int, id int, v int) USING ducklake;
CALL ducklake.set_partition('flr_fact'::regclass, 'part');

INSERT INTO flr_fact VALUES (0,1,100),(0,2,200),(0,3,300),(0,4,400);

-- Interleaved updates: several versions chained under one row_id. The UPDATEs that
-- stamp end_snapshot relocate heap tuples, so physical order != begin_snapshot order.
UPDATE flr_fact SET v=101 WHERE id=1;
UPDATE flr_fact SET v=201 WHERE id=2;
UPDATE flr_fact SET v=102 WHERE id=1;
UPDATE flr_fact SET v=301 WHERE id=3;
UPDATE flr_fact SET v=103 WHERE id=1;
UPDATE flr_fact SET v=202 WHERE id=2;
UPDATE flr_fact SET v=999 WHERE id=1;

-- Latest committed values, before the flush.
SELECT id, v FROM flr_fact ORDER BY id;

-- The flush that mis-tombstoned the live version pre-fix.
SELECT * FROM ducklake.flush_inlined_data('flr_fact'::regclass);

-- Post-flush values must be identical to the pre-flush values above.
SELECT id, v FROM flr_fact ORDER BY id;

-- Explicit verdict: 0 == no row reverted (GREEN); pre-fix this was 3.
SELECT count(*) AS reverted_rows
FROM flr_fact f JOIN (VALUES (1,999),(2,202),(3,301),(4,400)) e(id,ev) ON e.id = f.id
WHERE f.v <> e.ev;

DROP TABLE flr_fact;
CALL ducklake.set_option('data_inlining_row_limit', 0);
