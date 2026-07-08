/*
 * The pg_ducklake duckdb worker.
 *
 * The generic machinery -- per-database worker registry, session-pool dispatch,
 * scan inversion + the scan-producer pool, session threads -- is the kernel
 * pgddb::DuckdbWorker base class. This subclass supplies pg_ducklake's policy: the
 * dispatch gate (read-only SELECTs plus autocommit DML when
 * ducklake.use_shared_worker is on), the engine accessors, the DuckLake metadata RPC
 * servicing (MetaQuery reads / MetaExec commit writes run on the requesting backend,
 * so the worker stays PG-free), and the background-worker entrypoints.
 *
 * ducklake.worker_ping()/worker_eval(text)/worker_stats() are diagnostics.
 */

#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

#include <exception>
#include <string>

#include "pgddb/pgddb_planner.hpp"
#include "pgddb/worker/duckdb_worker.hpp"
#include "pgddb/worker/transport/session_channel.hpp"
#include "pgddb/worker/transport/session_protocol.hpp"

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/query_result.hpp>

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "postmaster/bgworker.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
}

namespace pgducklake {

class DuckdbWorker final : public pgddb::DuckdbWorker {
public:
	DuckdbWorker()
	    : pgddb::DuckdbWorker(Settings {
	          "PgDuckLakeWorker",
	          "pg_ducklake",
	          "ducklake_duckdb_worker_main",
	          "ducklake_scan_worker_main",
	          "pg_ducklake",
	          &worker_max_sessions,
	          &worker_arrow_pool_pages,
	          &worker_arrow_page_size,
	          &worker_scan_pool_size,
	          &worker_scan_producers,
	      }) {
	}

protected:
	/* No Configure step: the engine reads ducklake.threads etc. from the GUCs at init. */

	void
	Prime() override {
		/* GetConnection (not Get) so secrets, extensions, and the DuckLake catalog attach
		 * are all primed; per-session connections never refresh (they run PG-free). */
		(void)DuckDBManager::Get().GetConnection();
	}

	duckdb::DuckDB &
	Database() override {
		return DuckDBManager::Get().GetDatabase();
	}

	/* Backend side: service the worker's DuckLake metadata RPCs under this backend's
	 * snapshot (reads) / transaction (commit writes), keeping the worker PG-free. */
	bool
	ServeFrame(pgddb::SessionChannel &ch, pgddb::FrameTag tag, const char *data, std::size_t len) override {
		if (tag != pgddb::FrameTag::MetaQuery && tag != pgddb::FrameTag::MetaExec)
			return false;
		std::string sql(data, len);
		try {
			duckdb::unique_ptr<duckdb::QueryResult> result;
			if (tag == pgddb::FrameTag::MetaQuery)
				result = ExecuteMetadataQueryLocally(sql);
			else
				result = ExecuteMetadataExecLocally(sql);
			pgddb::SendMetadataReply(ch, *result);
		} catch (const std::exception &e) {
			duckdb::ErrorData err(e);
			pgddb::SendMetadataError(ch, err.Type(), err.RawMessage());
		}
		return true;
	}
};

static DuckdbWorker *
TheWorker() {
	static DuckdbWorker worker;
	return &worker;
}

} // namespace pgducklake

namespace {

/* Kernel CustomScan dispatch hook: route eligible queries to the worker. */
duckdb::unique_ptr<pgddb::WorkerResultStream>
DispatchToWorker(const Query *query) {
	if (pgddb::DuckdbWorker::InWorker() || !pgducklake::use_shared_worker)
		return nullptr;
	bool is_write =
	    query->commandType == CMD_INSERT || query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE;
	if (query->commandType != CMD_SELECT && !is_write)
		return nullptr; /* utility / DDL never dispatch */
	if (!ActiveSnapshotSet())
		return nullptr;
	/* The shipped snapshot cannot see this transaction's own uncommitted changes, so
	 * only dispatch when nothing has been written yet in this transaction. */
	if (GetTopTransactionIdIfAny() != InvalidTransactionId)
		return nullptr;
	if (is_write) {
		/* A dispatched write's DuckLake metadata commit runs on this backend (MetaExec),
		 * inside this backend's transaction; keep the gate at a single autocommit
		 * statement without RETURNING so statement and metadata commit coincide. */
		if (IsTransactionBlock() || query->returningList != NIL)
			return nullptr;
	}

	return pgducklake::TheWorker()->OpenSession(pgddb::DeparseQuery(query));
}

/* Ship `sql` to the per-database worker and return the first cell of the first
 * chunk as int64. Worker errors become PG ERRORs. */
int64
WorkerEvalFirstBigint(const char *sql) {
	auto stream = pgducklake::TheWorker()->OpenSession(sql);
	int64 result = 0;
	bool have_value = false;
	bool failed = false;
	std::string error_text;
	try {
		for (;;) {
			auto chunk = stream->Fetch();
			if (!chunk)
				break;
			if (!have_value && chunk->size() > 0) {
				result = chunk->GetValue(0, 0).GetValue<int64_t>();
				have_value = true;
			}
		}
	} catch (const std::exception &e) {
		failed = true;
		error_text = e.what();
	}
	stream.reset(); /* release the session slot before any ereport longjmp */

	if (failed)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("ducklake worker: %s", error_text.c_str())));
	return result;
}

} // namespace

extern "C" {

PGDLLEXPORT void
ducklake_duckdb_worker_main(Datum main_arg) {
	pgducklake::TheWorker()->Main((uint32_t)DatumGetObjectId(main_arg));
}

PGDLLEXPORT void
ducklake_scan_worker_main(Datum main_arg) {
	pgducklake::TheWorker()->ScanWorkerMain((uint32_t)DatumGetObjectId(main_arg));
}

PG_FUNCTION_INFO_V1(ducklake_worker_ping);
Datum
ducklake_worker_ping(PG_FUNCTION_ARGS) {
	PG_RETURN_INT64(WorkerEvalFirstBigint("SELECT 42::BIGINT"));
}

PG_FUNCTION_INFO_V1(ducklake_worker_eval);
Datum
ducklake_worker_eval(PG_FUNCTION_ARGS) {
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("query must not be NULL")));
	char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	PG_RETURN_INT64(WorkerEvalFirstBigint(query));
}

/* Sessions this database's duckdb worker accepted since it started. */
PG_FUNCTION_INFO_V1(ducklake_worker_stats);
Datum
ducklake_worker_stats(PG_FUNCTION_ARGS) {
	PG_RETURN_INT64((int64)pgducklake::TheWorker()->DispatchCount());
}

} // extern "C"

namespace pgducklake {

void
InitDuckdbWorker() {
	TheWorker()->Init();
	pgddb::pgddb_dispatch_to_worker_hook = DispatchToWorker;
}

} // namespace pgducklake
