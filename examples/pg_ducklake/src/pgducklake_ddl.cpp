/*
 * pgducklake_ddl.cpp -- Extension-scope DDL bootstrap entry points.
 *
 * @scope extension: ducklake_initialize, ducklake_only_procedure
 *
 * ducklake_initialize() bootstraps the DuckDB catalog during CREATE EXTENSION.
 * ducklake_only_procedure() is an error stub for DuckDB-only SQL procedures.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgddb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "commands/extension.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
}

extern "C" {

DECLARE_PG_FUNCTION(ducklake_initialize) {
  elog(LOG, "ducklake_initialize() called");

  if (!creating_extension) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ducklake_initialize() can only be called during "
                                                                     "CREATE EXTENSION")));
  }

  if (pgducklake::PgDuckLakeMetadataManager::IsInitialized()) {
    ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_SCHEMA), errmsg("DuckLake reserved schema \"ducklake\" is already in use")));
  }

  // Force DuckDB initialization (no-op if already alive).
  //   First CREATE: the SELECT 1 triggers DuckDBManager::Initialize(), whose
  //     OnPostInit() calls ducklake_attach_catalog().
  //   DROP+CREATE in the same backend: DuckDB is already alive, so SELECT 1
  //     does not re-run OnPostInit; the catalog was detached by the utility
  //     hook during DROP, so re-attach it here.
  bool duckdb_already_initialized = pgducklake::DuckDBManager::IsInitialized();

  pgducklake::DuckDBQueryOrThrow("SELECT 1");

  if (duckdb_already_initialized) {
    ducklake_attach_catalog();
  }

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_only_procedure) {
  char *proc_name = DatumGetCString(DirectFunctionCall1(regprocout, ObjectIdGetDatum(fcinfo->flinfo->fn_oid)));
  elog(ERROR, "Procedure '%s' only works with DuckDB execution", proc_name);
}

} // extern "C"
