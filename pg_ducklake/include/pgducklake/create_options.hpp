#pragma once

/*
 * create_options.hpp -- WITH (ducklake.*) clause support for
 * CREATE TABLE ... USING ducklake.
 *
 * Stock PG rejects the ducklake.* reloption namespace (HEAP_RELOPT_NAMESPACES
 * only allows "toast"). The utility hook strips the ducklake.* DefElems from
 * the parsetree before standard_ProcessUtility validates the remaining
 * options, stashes them in a per-process scratchpad, and the CREATE TABLE
 * event trigger drains the scratchpad to apply them.
 *
 * v1 options:
 *   ducklake.table_path -- per-table data path override, pushed via the
 *                          ducklake_default_table_path DuckDB session option
 *                          that DuckLake's CreateTable reads.
 *
 * Per-table options routed through ducklake.set_option (e.g.
 * data_inlining_row_limit) are intentionally NOT supported here: upstream
 * set_option refuses transaction-local table_ids, and inside the CREATE TABLE
 * event trigger the new table is still transaction-local. Set those via
 * CALL ducklake.set_option(opt, val, 'schema.table'::regclass) afterwards.
 */

#include <string>

struct List;

namespace pgducklake {

struct PendingCreateOptions {
	bool present = false;
	bool has_table_path = false;
	std::string table_path;
};

/*
 * Walk a CREATE TABLE options list (DefElem nodes), validate any
 * defnamespace=="ducklake" entries against the v1 allow-list, stash them in
 * the per-process scratchpad, and rewrite *options_ref to the remainder.
 * Returns true if any ducklake.* option was stripped. Raises ereport(ERROR)
 * on unknown ducklake.* names or invalid values.
 */
bool StripDucklakeCreateOptions(List **options_ref);

/* Snapshot + clear the scratchpad. present=false if nothing was stashed. */
PendingCreateOptions TakePendingCreateOptions();

/* Discard any pending scratchpad entry without applying it (hook error path). */
void ClearPendingCreateOptions();

/* Push opts.table_path into the DuckDB session as ducklake_default_table_path
 * before the generated CREATE TABLE DDL. No-op when !opts.has_table_path. */
void ApplyTablePathBeforeCreate(const PendingCreateOptions &opts);

/* RESET ducklake_default_table_path after the DDL so the WITH override does
 * not leak to later CREATE TABLEs (RefreshConnectionState re-pushes the
 * ducklake.default_table_path GUC on the next statement). No-op when
 * !opts.has_table_path. */
void RestoreTablePathAfterCreate(const PendingCreateOptions &opts);

} // namespace pgducklake
