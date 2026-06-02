/*
 * pgducklake_duckdb.cpp -- DuckLake catalog lifecycle in DuckDB
 *
 * Manages the "pgducklake" DuckLake catalog attached inside DuckDB.
 * Three lifecycles exist:
 *
 *   _PG_init() (once per backend):
 *     DuckLakeMetadataManager::Register("pgducklake", ...)
 *
 *   First CREATE EXTENSION (DuckDB not yet initialized):
 *     ducklake_initialize()          -- SQL script entry point
 *       -> DuckDBQueryOrThrow("SELECT 1")
 *           -> DuckDBManager::Initialize()
 *               -> DuckDBManager::OnPostInit()
 *                   -> LoadStaticExtension
 *                   -> ducklake_attach_catalog()
 *
 *   DROP + CREATE EXTENSION (DuckDB already alive):
 *     DROP EXTENSION pg_ducklake
 *       -> DucklakeUtilityHook        [pgducklake_hooks.cpp]
 *           -> ducklake_detach_catalog()
 *     CREATE EXTENSION pg_ducklake
 *       -> ducklake_initialize()
 *           -> DuckDBQueryOrThrow("SELECT 1")   (no-op, DuckDB exists)
 *           -> ducklake_attach_catalog()        (catalog was detached)
 *
 *   duckdb.recycle_ddb() (DuckDB instance destroyed and recreated):
 *     recycle_ddb()
 *       -> DuckDBManager::Reset()               [destroys DuckDB instance]
 *     next query
 *       -> DuckDBManager::Initialize()
 *           -> DuckDBManager::OnPostInit()
 *               -> LoadStaticExtension
 *               -> ducklake_attach_catalog()
 *     (metadata manager already registered in _PG_init, no re-registration)
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_functions.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_time_travel.hpp"

#include "pgddb/catalog/pgddb_storage.hpp"
#include "pgddb/pgddb_duckdb.hpp"
#include "pgddb/pg/transactions.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "ducklake_extension.hpp"

extern "C" {
#include "pgddb/pgddb_ruleutils.h"

#include "nodes/pg_list.h"
}

#include <cstring>
#include <filesystem>

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
}

void ducklake_detach_catalog() {
  try {
    pgducklake::DuckDBQueryOrThrow("DETACH DATABASE IF EXISTS " PGDUCKLAKE_DUCKDB_CATALOG);
  } catch (const std::exception &e) {
    elog(WARNING, "Failed to detach DuckLake catalog: %s", pgducklake::DuckDBErrorMessage(e).c_str());
  }
}

void ducklake_attach_catalog() {
  /* METADATA_CATALOG points the DuckLakeTransaction metadata connection's
   * search path at the pgducklake catalog itself (instead of the default
   * __ducklake_metadata_pgducklake, which does not exist because pg_ducklake
   * keeps metadata in PostgreSQL, not in a separate DuckDB database).
   * This lets DuckDB-native queries (read_blob, etc.) on the metadata
   * connection resolve system functions through normal catalog search. */
  duckdb::string query =
      "ATTACH 'ducklake:" PGDUCKLAKE_DUCKDB_CATALOG ":' AS " PGDUCKLAKE_DUCKDB_CATALOG
      "(METADATA_SCHEMA " PGDUCKLAKE_PG_SCHEMA_QUOTED ", METADATA_CATALOG " PGDUCKLAKE_DUCKDB_CATALOG;
    /* First-time init: create local data directory and pass it as DATA_PATH
     * so DuckLake stores it in the catalog metadata. */
    auto data_path = duckdb::StringUtil::Format("%s/pg_ducklake", DataDir);
    try {
      std::filesystem::create_directory(data_path);
    } catch (const std::filesystem::filesystem_error &e) {
      ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
                      errmsg("failed to create DuckLake data directory \"%s\": %s", data_path.c_str(), e.what())));
    }
    query += ", DATA_PATH '" + data_path + "'";
  /* On subsequent ATTACHes, omit DATA_PATH so DuckLake reads it from its
   * stored catalog metadata. This avoids mismatch errors when the data_path
   * has been changed (e.g. to an S3 bucket via ducklake.set_option). */
  query += ")";

  elog(DEBUG1, "Executing query: %s", query.c_str());

  try {
    pgducklake::DuckDBQueryOrThrow(query);
  } catch (const std::exception &e) {
    elog(ERROR, "Failed to attach DuckLake catalog: %s", pgducklake::DuckDBErrorMessage(e).c_str());
  }
}

namespace pgducklake {
void ResetDirectInsertCaches();
} // namespace pgducklake

class PostgresScannerExtension : public duckdb::Extension {
public:
  std::string Name() override {
    return "postgres_scanner";
  }
  void Load(duckdb::ExtensionLoader &loader) override;
};

// libpgddb manager binding. Subclasses pgddb::DuckDBManager and overrides
// OnPostInit so the first DuckDBQueryOrThrow() in a backend brings up DuckDB
// with pg_ducklake's PostgresStorageExtension registered under "pgducklake"
// (DuckLake's ATTACH connection string asks for it), loads the DuckLake +
// postgres_scanner static extensions, registers the wrapper macros, and
// attaches the catalog -- all in process, no registered callback.
namespace pgducklake {

void
DuckDBManager::OnPostInit(duckdb::ClientContext &context) {
	auto &dbconfig = duckdb::DBConfig::GetConfig(*database->instance);
	database->LoadStaticExtension<duckdb::DucklakeExtension>();
    database->LoadStaticExtension<PostgresScannerExtension>();
	duckdb::StorageExtension::Register(dbconfig, PGDUCKLAKE_PG_STORAGE_CATALOG,
	                                   duckdb::make_shared_ptr<::pgddb::PostgresStorageExtension>());
	pgducklake::ResetDirectInsertCaches();
    pgducklake::RegisterTimeTravelFunction(*context.db);
    pgducklake::RegisterWrapperMacros(*context.db);
    pgducklake::RegisterScalarMacros(*context.db);
    pgducklake::RegisterCleanupFunction(*context.db);
    pgducklake::RegisterCleanupOrphanedFilesFunction(*context.db);
    pgducklake::RegisterCompactionFunctions(*context.db);
    pgducklake::RegisterFlushInlinedDataFunction(*context.db);

  ducklake_attach_catalog();
	/*
	 * Now ATTACH the storage catalog so queries can resolve
	 * `<storage_catalog>.schema.table` references. Mirrors upstream
	 * pg_duckdb's `ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)`.
	 */
	QueryOrThrow(context,
	                   "ATTACH DATABASE '" PGDUCKLAKE_PG_STORAGE_CATALOG "' (TYPE " PGDUCKLAKE_PG_STORAGE_CATALOG ")");
}

void
DuckDBManager::RefreshConnectionState(duckdb::ClientContext &context) {
	// Push the ducklake.default_table_path GUC to DuckDB so DuckLake's
	// CreateTable writes data files under the custom path. Runs on every
	// GetConnection (right before the next statement), so a runtime
	// SET ducklake.default_table_path is observed by the following CREATE
	// TABLE. Uses the (context, query) overload to avoid recursing back into
	// GetConnection.
	if (default_table_path && default_table_path[0] != '\0') {
		try {
			DuckDBQueryOrThrow(context, "SET ducklake_default_table_path = " +
			                                duckdb::KeywordHelper::WriteQuoted(std::string(default_table_path)));
		} catch (const std::exception &e) {
			elog(WARNING, "failed to sync ducklake.default_table_path to DuckDB: %s", DuckDBErrorMessage(e).c_str());
		}
	}
}

} // namespace pgducklake

namespace pgducklake {

duckdb::unique_ptr<DuckDBManager> DuckDBManager::instance_;

bool
DuckDBManager::IsInitialized() {
	return instance_ != nullptr && instance_->database != nullptr;
}

DuckDBManager &
DuckDBManager::Get() {
	if (!instance_) {
		instance_ = duckdb::make_uniq<DuckDBManager>();
	}
	if (!instance_->database) {
		instance_->Initialize();
	}
	return *instance_;
}

void
DuckDBManager::Reset() {
	if (!instance_) {
		return;
	}
	instance_->connection = nullptr;
	delete instance_->database;
	instance_->database = nullptr;
}

static duckdb::Connection *
GetConnectionForScan(bool force_transaction) {
	return DuckDBManager::Get().GetConnection(force_transaction);
}

void
InitDuckDBManager() {
	::pgddb::pgddb_get_connection_hook = GetConnectionForScan;
}

} // namespace pgducklake

namespace pgducklake {

/*
 * pgddb_db_and_schema_hook impl: route the relation to the right DuckDB
 * catalog based on its table-AM.
 *
 *   ducklake AM   -> PGDUCKLAKE_DUCKDB_CATALOG    (DuckLake catalog)
 *   anything else -> PGDUCKLAKE_PG_STORAGE_CATALOG (PostgresStorageExtension)
 *
 * The companion storage catalog is ATTACHed in OnPostInit and lets DuckDB
 * queries reach PG heap tables, foreign tables, and views. Schema name is
 * the PG schema name unchanged in both cases. No MotherDuck /
 * multi-database routing applies in pg_ducklake.
 */
static List *
DbAndSchemaForDucklake(const char *postgres_schema_name, const char *table_am_name) {
	// pgducklake_table.cpp registers the ducklake AM under
	// PGDUCKLAKE_TABLE_AM. Any other relation (PG heap, foreign table,
	// view) returns nullptr or a different identifier; route those to the
	// PostgresStorageExtension catalog so DuckDB can still read them.
	if (table_am_name == nullptr || strcmp(PGDUCKLAKE_TABLE_AM, table_am_name) != 0)
		return list_make2((void *)PGDUCKLAKE_PG_STORAGE_CATALOG, (void *)postgres_schema_name);
	return list_make2((void *)PGDUCKLAKE_DUCKDB_CATALOG, (void *)postgres_schema_name);
}

/*
 * pgddb_column_type_name_hook impl: the standard PG deparser writes
 * `ducklake.variant` for a variant column, which DuckDB resolves as
 * type "variant" in schema "ducklake" -- which doesn't exist. Map it
 * to plain VARCHAR; field extraction works through the pg_variant_extract*
 * scalar macros (registered in pgducklake_functions.cpp) that parse the
 * underlying JSON-as-text representation. Matches what
 * ConvertPostgresToBaseDuckColumnTypeHook returns for the same OID.
 *
 * Returns NULL for non-variant types so the deparser falls through to
 * PG's format_type_with_typemod.
 */
static char *
DucklakeColumnTypeName(Oid type_oid, int32_t /*typemod*/) {
	if (!OidIsValid(type_oid))
		return nullptr;
	Oid ducklake_nsp = get_namespace_oid(PGDUCKLAKE_PG_SCHEMA, /*missing_ok=*/true);
	if (!OidIsValid(ducklake_nsp))
		return nullptr;
	Oid variant_oid =
	    GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, PointerGetDatum("variant"), ObjectIdGetDatum(ducklake_nsp));
	if (type_oid == variant_oid)
		return pstrdup("VARCHAR");
	return nullptr;
}

/*
 * pgddb_function_name_hook impl: when a ducklake-only function (prosrc
 * 'duckdb_only_function') is deparsed for DuckDB execution, rewrite the
 * name to "system.main.<func_name>" so DuckDB resolves the wrapper macro
 * registered under DEFAULT_SCHEMA (see RegisterWrapperMacros). Returns
 * NULL for any other function so libpgddb deparser falls through to its
 * standard name resolution.
 */
static char *
DucklakeFunctionName(Oid function_oid, bool *use_variadic_p) {
	if (!IsDucklakeOnlyFunction(function_oid))
		return nullptr;
	if (use_variadic_p)
		*use_variadic_p = false;
	char *func_name = get_func_name(function_oid);
	// ducklake.duckdb_query() is a thin wrapper over DuckDB's built-in
	// query() table function. Deparse directly to "query(...)" rather
	// than routing through a system.main wrapper macro.
	if (std::strcmp(func_name, "duckdb_query") == 0)
		return pstrdup("query");
	return psprintf("system.main.%s", quote_identifier(func_name));
}

void
InitRuleutilsHooks() {
	pgddb_db_and_schema_hook = DbAndSchemaForDucklake;
	pgddb_function_name_hook = DucklakeFunctionName;
	pgddb_column_type_name_hook = DucklakeColumnTypeName;
}

/*
 * PG -> DuckDB transaction sync.
 *
 * DuckLake materializes its catalog and inlined data in PG tables via SPI.
 * Those writes ride PG's transaction. DuckDB itself maintains its own
 * transaction (DuckLakeTransaction) on the pgducklake catalog: bumping
 * snapshot ids, tracking inlined inserts, etc. Without a callback that
 * mirrors PG's PRE_COMMIT / ABORT to DuckDB, DuckDB's transaction stays
 * open after each implicit-autocommit statement, so subsequent statements
 * see a stale view and the next backend never observes the writes
 * (DuckLake metadata Iterators rely on snapshot_id from a committed row).
 *
 * Lifted from pg_duckdb's DuckdbXactCallback minus its MotherDuck-, mixed-
 * write-, and command-id-tracking machinery (pg_ducklake doesn't need them
 * since its writes go through PG's heap, not a separate DuckDB store).
 */
static void
DuckLakeXactCallback(XactEvent event, void * /*arg*/) {
	if (!pgducklake::DuckDBManager::IsInitialized()) {
		return;
	}
	auto *connection = pgducklake::DuckDBManager::Get().GetConnectionUnsafe();
	if (!connection) {
		return;
	}
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		return;
	}
	switch (event) {
	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
		context.transaction.Commit();
		break;
	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
		context.transaction.Rollback(nullptr);
		break;
	case XACT_EVENT_PREPARE:
	case XACT_EVENT_PRE_PREPARE:
	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
		break;
	}
}

/*
 * Backing store for the DuckdbAllowSubtransaction() contract. DuckLake's
 * metadata-commit path needs to open a PG subtransaction so its retry loop
 * can catch unique-violation conflicts; we flip this guard on around that
 * specific BeginInternalSubTransaction. Any other SAVEPOINT attempt while
 * DuckDB has an active transaction is rejected by DuckLakeSubXactCallback
 * below.
 */
bool allow_subtransaction = false;

void
SetAllowSubtransaction(bool allow) {
	allow_subtransaction = allow;
}

/*
 * PG -> DuckDB SAVEPOINT guard. Mirrors pg_duckdb's DuckdbSubXactCallback:
 * if DuckDB is holding an active transaction and the user issues a
 * SAVEPOINT (or BeginInternalSubTransaction without the allow_subtransaction
 * gate), throw so the inconsistency surfaces immediately instead of
 * corrupting PG's snapshot bookkeeping at parent commit time.
 */
static void
DuckLakeSubXactCallback(SubXactEvent event, SubTransactionId /*my_subid*/, SubTransactionId /*parent_subid*/,
                        void * /*arg*/) {
	if (!pgducklake::DuckDBManager::IsInitialized()) {
		return;
	}
	auto *connection = pgducklake::DuckDBManager::Get().GetConnectionUnsafe();
	if (!connection) {
		return;
	}
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		return;
	}
	if (event == SUBXACT_EVENT_START_SUB && !allow_subtransaction) {
		throw duckdb::NotImplementedException("SAVEPOINT is not supported in DuckDB");
	}
}

void
RegisterXactCallback() {
	::pgddb::pg::RegisterXactCallback(DuckLakeXactCallback, nullptr);
	::pgddb::pg::RegisterSubXactCallback(DuckLakeSubXactCallback, nullptr);
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query) {
  auto res = context.Query(query, false);
  if (res->HasError()) {
    res->ThrowError();
  }
  return res;
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query) {
  return DuckDBQueryOrThrow(*connection.context, query);
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(const std::string &query) {
  auto *connection = DuckDBManager::Get().GetConnection();
  return DuckDBQueryOrThrow(*connection, query);
}

std::string
DuckDBErrorMessage(const std::exception &e) {
  const char *what = e.what();
  // Exceptions thrown by QueryResult::ThrowError() carry a JSON ErrorData
  // blob; unwrap it the same way the cpp_wrapper guard does. Non-duckdb
  // exceptions keep their plain message.
  if (what && what[0] == '{') {
    return duckdb::ErrorData(what).Message();
  }
  return what ? what : "unknown error";
}

} // namespace pgducklake

/*
 * DuckDB-related SQL admin UDFs (exposed in the ducklake schema). Mirror
 * pg_duckdb's duckdb.recycle_ddb / duckdb.raw_query.
 *
 *   ducklake.recycle_ddb()           -- tear down + recreate DuckDBManager
 *   ducklake.duckdb_raw_query(text)  -- run an arbitrary string on DuckDB
 *
 * ducklake.duckdb_query(text) is a duckdb_only_function SQL stub routed by
 * the planner (DucklakeFunctionName rewrites it to DuckDB's query() table
 * function); it has no C entry point here.
 */
extern "C" {

PG_FUNCTION_INFO_V1(ducklake_recycle_ddb);
Datum
ducklake_recycle_ddb(PG_FUNCTION_ARGS) {
	// Recycling tears down a DuckDB instance that may have an open
	// transaction tied to the current PG transaction. Match pg_duckdb's
	// guard.
	::pgddb::pg::PreventInTransactionBlock(true, "ducklake.recycle_ddb()");
	pgducklake::DuckDBManager::Reset();
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_duckdb_raw_query);
Datum
ducklake_duckdb_raw_query(PG_FUNCTION_ARGS) {
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("query must not be NULL")));
	const char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	try {
		pgducklake::DuckDBQueryOrThrow(query);
	} catch (const std::exception &e) {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", pgducklake::DuckDBErrorMessage(e).c_str())));
	}
	PG_RETURN_VOID();
}

} // extern "C"
