#pragma once

/*
 * pgducklake_duckdb.hpp -- C++ interface for DuckDB/DuckLake operations
 *
 * Provides functions for DuckLake extension lifecycle management and the
 * pg_ducklake-flavored DuckDBManager subclass.
 */

#include "pgddb/pgddb_duckdb.hpp"

#include <exception>
#include <string>

namespace pgducklake {

class DuckDBManager : public ::pgddb::DuckDBManager {
public:
	static bool IsInitialized();
	static DuckDBManager &Get();
	static void Reset();

protected:
	void OnPostInit(duckdb::ClientContext &context) override;
	// Syncs the ducklake.default_table_path GUC to DuckDB before each statement
	// (runs per GetConnection), so a runtime SET is picked up by the next
	// CREATE TABLE. OnPostInit runs only once per instance and would miss it.
	void RefreshConnectionState(duckdb::ClientContext &context) override;

private:
	static duckdb::unique_ptr<DuckDBManager> instance_;
};

// Installs pgddb_get_connection_hook; called from _PG_init.
void InitDuckDBManager();

/*
 * Execute a query against the pg_ducklake DuckDB connection and return its
 * result, throwing a duckdb exception on error. Mirrors pg_duckdb's
 * DuckDBQueryOrThrow. Thrown exceptions are turned into PG errors by the
 * DECLARE_PG_FUNCTION / InvokeCPPFunc guard at the entry point (see
 * utility/cpp_wrapper.hpp); call sites that need cleanup or non-fatal
 * handling catch them locally instead.
 *
 * The (query) overload runs on DuckDBManager::Get()'s cached connection
 * with the standard transaction policy applied.
 */
duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query);
duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query);
duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(const std::string &query);

/*
 * Extract a human-readable message from an exception thrown by
 * DuckDBQueryOrThrow. Those exceptions carry a JSON-serialized duckdb
 * ErrorData blob in what(); this unwraps it to the plain message (matching
 * the DECLARE_PG_FUNCTION / InvokeCPPFunc guard). Use at local catch sites
 * that surface the message via elog/ereport instead of re-throwing.
 */
std::string DuckDBErrorMessage(const std::exception &e);

} // namespace pgducklake

/* Detach the "pgducklake" DuckLake catalog.  Called by the utility hook
 * after DROP EXTENSION so that a subsequent CREATE EXTENSION can
 * attach a fresh catalog. */
void ducklake_detach_catalog();

/* Attach the "pgducklake" DuckLake catalog.  Called during initial
 * extension load (DuckDBManager::OnPostInit) and on re-create
 * (ducklake_initialize). */
void ducklake_attach_catalog();

namespace pgducklake {

/* Installs pg_ducklake's libpgddb ruleutils hooks (pgddb_db_and_schema_hook)
 * so pgddb_get_tabledef etc. resolve the DuckDB catalog/schema correctly
 * for ducklake-AM tables. Called from _PG_init. */
void InitRuleutilsHooks();

/* Register PG XactCallback that mirrors PG PRE_COMMIT/ABORT to DuckDB's
 * DuckLake transaction. Without this, DuckDB never commits its in-memory
 * transaction state and subsequent statements see stale snapshots. Called
 * from _PG_init. */
void RegisterXactCallback();

/*
 * Toggle the SUBXACT_EVENT_START_SUB guard. Set true around code paths that
 * legitimately need to open a PG subtransaction while DuckDB has an active
 * transaction (e.g. DuckLake metadata commit's FlushChanges retry loop);
 * set false everywhere else. Backs the pgduckdb::DuckdbAllowSubtransaction
 * contract shim.
 */
void SetAllowSubtransaction(bool allow);

} // namespace pgducklake
