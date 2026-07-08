#include "pgddb/worker/duckdb_worker.hpp"

#include <atomic>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "pgddb/catalog/relation_desc.hpp"
#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/worker/scan_producer.hpp"
#include "pgddb/worker/transport/session_pool.hpp"
#include "pgddb/worker/transport/page_pool.hpp"
#include "pgddb/worker/transport/scan_queue.hpp"
#include "pgddb/worker/transport/scan_ring.hpp"
#include "pgddb/worker/transport/session_protocol.hpp"
#include "pgddb/worker/worker_session.hpp"

#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/connection.hpp>

extern "C" {
#include "postgres.h"

#include <signal.h>

#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
}

namespace pgddb {

/* Internal: session threads run as free functions but need the subclass's engine
 * accessor; this keeps the virtual protected in the public API. */
struct WorkerAccess {
	static duckdb::DuckDB &
	Database(DuckdbWorker *worker) {
		return worker->Database();
	}
};

namespace {

#define MAX_DUCKDB_WORKERS 16

/* Pending-session ring depth per worker. A backend acquires a session-pool slot before
 * enqueuing, so the queue can hold at most max_sessions entries; if it ever fills
 * (max_sessions set very high), the backend waits and retries the enqueue. */
#define MAX_PENDING_SESSIONS 1024

/* A queued session: the session-pool slot plus its generation at enqueue time, so
 * the worker never serves a stale entry whose backend already released the slot. */
struct PendingSession {
	uint32 conn_slot;
	uint32 conn_generation;
};

struct DuckdbWorkerSlot {
	Oid db_oid;
	pid_t worker_pid; /* 0 while a spawn is in progress */
	Latch *worker_latch;
	uint32 generation;
	bool in_use;
	uint64 dispatched; /* sessions accepted since this worker started (diagnostics) */
	/* Pending sessions enqueued by backends and drained by the worker, which spawns a
	 * session thread per entry. Guarded by the shmem lock; head/tail are monotonic,
	 * indexed mod MAX_PENDING_SESSIONS. */
	PendingSession pending[MAX_PENDING_SESSIONS];
	uint32 pending_head;
	uint32 pending_tail;
};

struct DuckdbWorkerShmem {
	slock_t lock;
	DuckdbWorkerSlot workers[MAX_DUCKDB_WORKERS];
};

DuckdbWorker::Settings g_settings = {};
DuckdbWorker *g_worker = nullptr;

DuckdbWorkerShmem *g_duckdb_workers = nullptr;

/* True inside the duckdb worker process itself, so it never recurses into dispatch. */
bool g_in_duckdb_worker = false;

/* Per-session worker context, keyed by the executing DuckDB ClientContext. A scan's
 * init_global (where the remote scan is opened) runs on a DuckDB scheduler thread, not the
 * session thread, so thread-local state is invisible there; the ClientContext is the one
 * handle reachable from both. Holds the session's channel (heap scans route their inner SQL
 * back to the requesting backend over it) and the backend's serialized snapshot (so scan
 * worker tasks read the same MVCC view). */
struct WorkerSessionContext {
	SessionChannel *channel;
	std::string snapshot_bytes;
};
std::mutex g_session_ctx_lock;
std::unordered_map<duckdb::ClientContext *, WorkerSessionContext> g_session_ctx;

/* Number of scan workers this duckdb worker spawned at startup; 0 means the scan
 * worker pool is unavailable and scans fall back to the in-backend inversion path. */
int g_scan_workers_spawned = 0;

/* Active session threads, each running one query on its own DuckDB connection.
 * File-scope so the worker's shmem-exit callback can drain them on any exit path. */
struct WorkerSession {
	std::thread thread;
	std::shared_ptr<std::atomic<bool>> done;
};
std::vector<WorkerSession> g_worker_sessions;

/* SIGTERM flag for the duckdb worker. A custom handler (not die) so the main loop can
 * stop its session threads before the process exits. */
volatile sig_atomic_t g_duckdb_worker_got_sigterm = false;

void
WorkerSigterm(SIGNAL_ARGS) {
	int save_errno = errno;
	g_duckdb_worker_got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_shmem_request_hook = nullptr;
#endif
shmem_startup_hook_type prev_shmem_startup_hook = nullptr;

Size
ArrowPoolBytes() {
	if (*g_settings.arrow_pool_pages <= 0)
		return 0;
	return PagePool::ShmemSize(*g_settings.arrow_pool_pages, *g_settings.arrow_page_size);
}

std::string
ShmemKey(const char *suffix) {
	return std::string(g_settings.shmem_name) + suffix;
}

void
ShmemRequest() {
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif
	RequestAddinShmemSpace(sizeof(DuckdbWorkerShmem));
	RequestAddinShmemSpace(SessionPool::ShmemSize(*g_settings.max_sessions));
	if (ArrowPoolBytes() > 0) {
		RequestAddinShmemSpace(ArrowPoolBytes());
		/* scan worker transports via the page pool: task queue + per-scan ready rings */
		RequestAddinShmemSpace(ScanQueue::ShmemSize());
		RequestAddinShmemSpace(ScanRing::ShmemSize());
	}
}

void
ShmemStartup() {
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	bool found;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	g_duckdb_workers =
	    (DuckdbWorkerShmem *)ShmemInitStruct(ShmemKey("Engine").c_str(), sizeof(DuckdbWorkerShmem), &found);
	if (!found) {
		MemSet(g_duckdb_workers, 0, sizeof(DuckdbWorkerShmem));
		SpinLockInit(&g_duckdb_workers->lock);
	}

	bool cp_found;
	int nconn = *g_settings.max_sessions;
	void *cp = ShmemInitStruct(ShmemKey("SessionPool").c_str(), SessionPool::ShmemSize(nconn), &cp_found);
	if (!cp_found)
		SessionPool::Init(cp, nconn);
	SessionPool::Attach(cp);

	if (ArrowPoolBytes() > 0) {
		bool pool_found;
		void *pool = ShmemInitStruct(ShmemKey("ArrowPagePool").c_str(), ArrowPoolBytes(), &pool_found);
		if (!pool_found)
			PagePool::Init(pool, *g_settings.arrow_pool_pages, *g_settings.arrow_page_size);
		PagePool::Attach(pool); // every process attaches to the identically-mapped region

		bool sq_found;
		void *sq = ShmemInitStruct(ShmemKey("ScanQueue").c_str(), ScanQueue::ShmemSize(), &sq_found);
		if (!sq_found)
			ScanQueue::Init(sq);
		ScanQueue::Attach(sq);

		bool sr_found;
		void *sr = ShmemInitStruct(ShmemKey("ScanRing").c_str(), ScanRing::ShmemSize(), &sr_found);
		if (!sr_found)
			ScanRing::Init(sr);
		ScanRing::Attach(sr);
	}
	LWLockRelease(AddinShmemInitLock);
}

/* Find a live worker slot for db_oid, or -1. Caller holds the lock. */
int
FindWorkerSlot(Oid db_oid) {
	for (int i = 0; i < MAX_DUCKDB_WORKERS; i++) {
		if (g_duckdb_workers->workers[i].in_use && g_duckdb_workers->workers[i].db_oid == db_oid)
			return i;
	}
	return -1;
}

/* Enqueue a session onto a worker's pending ring. Caller holds the lock.
 * Returns false if the ring is full (the backend waits and retries). */
bool
EnqueuePendingSession(int slot_idx, uint32 conn_slot, uint32 conn_generation) {
	DuckdbWorkerSlot &w = g_duckdb_workers->workers[slot_idx];
	if (w.pending_tail - w.pending_head >= MAX_PENDING_SESSIONS)
		return false;
	w.pending[w.pending_tail % MAX_PENDING_SESSIONS] = PendingSession {conn_slot, conn_generation};
	w.pending_tail++;
	w.dispatched++;
	return true;
}

/* Duckdb-worker startup (main thread): spawn the scan worker pool for this database.
 * Fire-and-forget: producers publish their latch and claim queued tasks once up, so no
 * startup wait is needed. Sets g_scan_workers_spawned; 0 disables the pool path. */
void
SpawnScanWorkers(Oid db_oid) {
	int n = *g_settings.scan_pool_size;
	if (n <= 0 || g_settings.bgw_scan_entrypoint == nullptr || !ScanQueue::Available() || !ScanRing::Available() ||
	    !PagePool().Available())
		return;

	int spawned = 0;
	for (int i = 0; i < n; i++) {
		BackgroundWorker worker;
		MemSet(&worker, 0, sizeof(BackgroundWorker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", g_settings.bgw_library);
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "%s", g_settings.bgw_scan_entrypoint);
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s scan worker (db %u)", g_settings.display_name, db_oid);
		snprintf(worker.bgw_type, BGW_MAXLEN, "%s scan worker", g_settings.display_name);
		worker.bgw_restart_time = BGW_NEVER_RESTART;
		worker.bgw_main_arg = ObjectIdGetDatum(db_oid);
		worker.bgw_notify_pid = MyProcPid;

		BackgroundWorkerHandle *handle = NULL;
		if (RegisterDynamicBackgroundWorker(&worker, &handle))
			spawned++;
		else
			elog(WARNING, "%s: could not register scan worker (out of background worker slots)",
			     g_settings.display_name);
	}
	g_scan_workers_spawned = spawned;
	elog(LOG, "%s scan worker pool: spawned %d/%d scan workers for database %u", g_settings.display_name, spawned, n,
	     db_oid);
}

bool
PidIsAlive(pid_t pid) {
	return pid != 0 && kill(pid, 0) == 0;
}

/* Ensure a worker exists for MyDatabaseId, spawning one if needed; returns its latch
 * and pid. A slot whose worker died (crash, SIGKILL) is reclaimed and respawned.
 * Raises an error if the worker registry is full or the worker fails to start. */
Latch *
EnsureWorkerForMyDatabase(pid_t *out_pid) {
	Oid db_oid = MyDatabaseId;

	for (;;) {
		SpinLockAcquire(&g_duckdb_workers->lock);
		int idx = FindWorkerSlot(db_oid);
		if (idx >= 0) {
			pid_t pid = g_duckdb_workers->workers[idx].worker_pid;
			Latch *latch = g_duckdb_workers->workers[idx].worker_latch;
			if (pid != 0 && latch != NULL && !PidIsAlive(pid)) {
				/* The worker died without cleaning up; reclaim so it respawns. */
				g_duckdb_workers->workers[idx].in_use = false;
				SpinLockRelease(&g_duckdb_workers->lock);
				continue;
			}
			SpinLockRelease(&g_duckdb_workers->lock);
			if (pid != 0 && latch != NULL) {
				*out_pid = pid;
				return latch;
			}
			CHECK_FOR_INTERRUPTS();
			pg_usleep(10000); /* spawn in progress; wait for the worker to publish */
			continue;
		}
		int free_idx = -1;
		for (int i = 0; i < MAX_DUCKDB_WORKERS; i++) {
			if (!g_duckdb_workers->workers[i].in_use) {
				free_idx = i;
				break;
			}
		}
		if (free_idx < 0) {
			SpinLockRelease(&g_duckdb_workers->lock);
			ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
			                errmsg("no free duckdb worker slot (max %d)", MAX_DUCKDB_WORKERS)));
		}
		g_duckdb_workers->workers[free_idx].in_use = true;
		g_duckdb_workers->workers[free_idx].db_oid = db_oid;
		g_duckdb_workers->workers[free_idx].worker_pid = 0;
		g_duckdb_workers->workers[free_idx].worker_latch = NULL;
		g_duckdb_workers->workers[free_idx].dispatched = 0;
		g_duckdb_workers->workers[free_idx].pending_head = 0;
		g_duckdb_workers->workers[free_idx].pending_tail = 0;
		g_duckdb_workers->workers[free_idx].generation++;
		SpinLockRelease(&g_duckdb_workers->lock);

		BackgroundWorker worker;
		MemSet(&worker, 0, sizeof(BackgroundWorker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", g_settings.bgw_library);
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "%s", g_settings.bgw_worker_entrypoint);
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s duckdb worker (db %u)", g_settings.display_name, db_oid);
		snprintf(worker.bgw_type, BGW_MAXLEN, "%s duckdb worker", g_settings.display_name);
		worker.bgw_restart_time = BGW_NEVER_RESTART; /* re-spawned on demand by backends */
		worker.bgw_main_arg = ObjectIdGetDatum(db_oid);
		worker.bgw_notify_pid = MyProcPid;

		BackgroundWorkerHandle *handle = NULL;
		if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
			SpinLockAcquire(&g_duckdb_workers->lock);
			g_duckdb_workers->workers[free_idx].in_use = false;
			SpinLockRelease(&g_duckdb_workers->lock);
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			                errmsg("could not register duckdb worker (out of background worker slots)")));
		}
		/* Poll the slot for the worker's published latch (set early in its main).
		 * WaitForBackgroundWorkerStartup is avoided: it does not report startup when
		 * called from inside the executor here. GetBackgroundWorkerPid still detects
		 * a worker that dies during startup. */
		for (int i = 0; i < 6000; i++) {
			CHECK_FOR_INTERRUPTS();
			SpinLockAcquire(&g_duckdb_workers->lock);
			pid_t wpid = g_duckdb_workers->workers[free_idx].worker_pid;
			Latch *wlatch = g_duckdb_workers->workers[free_idx].worker_latch;
			SpinLockRelease(&g_duckdb_workers->lock);
			if (wpid != 0 && wlatch != NULL) {
				*out_pid = wpid;
				return wlatch;
			}
			pid_t bpid;
			if (GetBackgroundWorkerPid(handle, &bpid) == BGWH_STOPPED) {
				SpinLockAcquire(&g_duckdb_workers->lock);
				g_duckdb_workers->workers[free_idx].in_use = false;
				SpinLockRelease(&g_duckdb_workers->lock);
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("duckdb worker stopped during startup")));
			}
			pg_usleep(10000); /* 10ms */
		}
		SpinLockAcquire(&g_duckdb_workers->lock);
		g_duckdb_workers->workers[free_idx].in_use = false;
		SpinLockRelease(&g_duckdb_workers->lock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("timed out waiting for duckdb worker to start")));
	}
}

class BackendSession;

/* Backend-side cleanup state: sessions the kernel CustomScan holds are palloc'd, so a
 * transaction abort frees the containing memory without running C++ destructors. The
 * registry lets the abort callback (and process exit) release the channel and the
 * connection slot explicitly. Backend-local, single-threaded. */
std::unordered_set<BackendSession *> g_open_streams;
int g_pending_conn_slot = -1; /* claimed but not yet owned by a stream */
bool g_backend_cleanup_registered = false;

void ReleaseOpenSessions(SubTransactionId subid);

void
WorkerXactCallback(XactEvent event, void *) {
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		ReleaseOpenSessions(InvalidSubTransactionId);
}

/* Release only streams opened inside the aborting subtransaction: a subtransaction
 * abort can be an internal, handled event (the backend services a worker's MetaExec
 * in a subtransaction that aborts on a DuckLake commit conflict and is retried) --
 * killing the servicing stream there would free the channel out from under it. */
void
WorkerSubXactCallback(SubXactEvent event, SubTransactionId my_subid, SubTransactionId, void *) {
	if (event == SUBXACT_EVENT_ABORT_SUB)
		ReleaseOpenSessions(my_subid);
}

void
WorkerShmemExitCallback(int, Datum) {
	ReleaseOpenSessions(InvalidSubTransactionId);
}

void
EnsureBackendCleanupRegistered() {
	if (g_backend_cleanup_registered)
		return;
	RegisterXactCallback(WorkerXactCallback, nullptr);
	RegisterSubXactCallback(WorkerSubXactCallback, nullptr);
	before_shmem_exit(WorkerShmemExitCallback, (Datum)0);
	g_backend_cleanup_registered = true;
}

/* Pull-based result stream over a worker session, consumed by the kernel CustomScan.
 * Releases its slot and detaches on destruction; on early teardown it cancels so the
 * worker is not left blocked on a full result queue. */
class BackendSession : public WorkerResultStream {
public:
	BackendSession(std::unique_ptr<SessionChannel> ch, int conn_slot, Latch *worker_latch, pid_t worker_pid)
	    : ch_(std::move(ch)), conn_slot_(conn_slot), worker_latch_(worker_latch), worker_pid_(worker_pid),
	      created_in_subxact_(GetCurrentSubTransactionId()) {
		g_open_streams.insert(this);
	}

	SubTransactionId
	CreatedInSubxact() const {
		return created_in_subxact_;
	}

	~BackendSession() override {
		g_open_streams.erase(this);
		if (!done_ && worker_latch_) {
			ch_->RequestCancel();
			SetLatch(worker_latch_);
		}
		ch_.reset();                         /* detach the backend end (unblocks the worker's result-queue send) */
		SessionPool().DetachEnd(conn_slot_); /* return the slot once the worker also detaches */
	}

	duckdb::unique_ptr<duckdb::DataChunk>
	Fetch() override {
		int idle_ticks = 0;
		for (;;) {
			FrameTag tag = FrameTag::Error;
			const char *data = nullptr;
			std::size_t len = 0;
			FrameResult fr = ch_->RecvResult(&tag, &data, &len, /*nowait=*/true);

			if (fr == FrameResult::WouldBlock) {
				if (QueryCancelPending) {
					ch_->RequestCancel();
					if (worker_latch_)
						SetLatch(worker_latch_);
					done_ = true;
					throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "Query cancelled");
				}
				/* A crashed worker never detaches its rings, so an empty queue would
				 * spin forever; probe its pid once a second and bail if it is gone. */
				if (++idle_ticks >= 1 && !PidIsAlive(worker_pid_)) {
					done_ = true;
					throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "duckdb worker terminated unexpectedly");
				}
				WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000L, PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);
				continue;
			}
			idle_ticks = 0;
			if (fr == FrameResult::Detached) {
				done_ = true;
				throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "duckdb worker detached");
			}
			if (tag == FrameTag::ScanFetch) {
				ServiceScanFetch(data, len);
				continue;
			}
			if (tag == FrameTag::DescribeRel) {
				ServeDescribeRelation(*ch_, data, len);
				continue;
			}
			if (g_worker != nullptr && g_worker->ServeFrame(*ch_, tag, data, len)) {
				continue;
			}
			if (tag == FrameTag::Chunk) {
				auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
				DeserializeDataChunk(data, len, *chunk);
				return chunk;
			}
			if (tag == FrameTag::Complete) {
				done_ = true;
				return nullptr;
			}
			if (tag == FrameTag::Error) {
				std::string msg(data, len);
				done_ = true;
				throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, msg);
			}
			/* ignore unexpected frames */
		}
	}

private:
	// Reply to a scan fetch; every reply is prefixed with the scan_id so the worker's
	// channel demux routes it to the right scan lane (scans + metadata RPCs interleave
	// on one channel).
	void
	SendScanReply(uint32_t scan_id, FrameTag tag, const char *data, std::size_t len) {
		std::string payload(reinterpret_cast<const char *>(&scan_id), sizeof(scan_id));
		if (len > 0)
			payload.append(data, len);
		ch_->SendControl(tag, payload.data(), payload.size());
	}

	// Service a worker's heap-scan fetch: payload is [uint32 scan_id][optional inner
	// SQL on the first fetch]. Opens a streaming producer on first fetch, then streams
	// one chunk per fetch until exhausted (ScanDone).
	void
	ServiceScanFetch(const char *data, std::size_t len) {
		uint32_t scan_id = 0;
		std::memcpy(&scan_id, data, sizeof(scan_id));

		auto it = open_scans_.find(scan_id);
		if (it == open_scans_.end()) {
			// First fetch: payload is [scan_id][uint8 count_only][inner SQL]. Open
			// errors are reported but the scan is not registered.
			try {
				bool count_only = data[sizeof(scan_id)] != 0;
				std::string sql(data + sizeof(scan_id) + 1, len - sizeof(scan_id) - 1);
				auto st = std::make_unique<BackendScanState>();
				InitBackendScan(*st, sql, count_only);
				it = open_scans_.emplace(scan_id, std::move(st)).first;
			} catch (const std::exception &e) {
				std::string msg = e.what();
				SendScanReply(scan_id, FrameTag::ScanError, msg.c_str(), msg.size());
				return;
			}
		}
		BackendScanState &st = *it->second;

		// A scan is kept (not erased) after it finishes/errors so the extra requests
		// from the worker's read-ahead window get a terminal reply, not a re-open.
		if (st.errored) {
			SendScanReply(scan_id, FrameTag::ScanError, st.err_msg.c_str(), st.err_msg.size());
			return;
		}
		if (st.done) {
			SendScanReply(scan_id, FrameTag::ScanDone, nullptr, 0);
			return;
		}

		int page_idx = -1; // an acquired-but-not-handed-off page; freed in catch on error
		try {
			if (st.is_count) {
				// COUNT(*): one BIGINT, serialized inline so the response is self-contained
				// (the read-ahead window keeps several fetches in flight).
				if (!ProduceCountChunk(st)) {
					SendScanReply(scan_id, FrameTag::ScanDone, nullptr, 0);
					return;
				}
				duckdb::MemoryStream stream;
				SerializeDataChunk(st.chunk, stream);
				SendScanReply(scan_id, FrameTag::ScanChunk, reinterpret_cast<const char *>(stream.GetData()),
				              stream.GetPosition());
				return;
			}

			// Data scans use the Arrow transport only (no serialize fallback). A page is
			// essentially always available -- bounded by the read-ahead window -- but
			// briefly retry rather than fail if the pool is momentarily empty.
			PagePool pool;
			PageSlot *slot = nullptr;
			for (int tries = 0; (slot = pool.Acquire()) == nullptr; tries++) {
				if (tries >= 2000)
					throw std::runtime_error("duckdb worker: Arrow page pool exhausted");
				CHECK_FOR_INTERRUPTS();
				pg_usleep(500);
			}
			page_idx = slot->index;
			char desc[sizeof(ArrowBatchHeader) + 64 * sizeof(ArrowColDesc)];
			ssize_t desc_len =
			    ProduceArrowChunk(st, slot->data, (std::size_t)pool.PageSize(), page_idx, desc, sizeof(desc));
			if (desc_len == 0) { // end of scan
				pool.Release(slot);
				page_idx = -1;
				SendScanReply(scan_id, FrameTag::ScanDone, nullptr, 0);
				return;
			}
			SendScanReply(scan_id, FrameTag::ScanChunkArrow, desc, (std::size_t)desc_len);
			page_idx = -1; // the worker owns the page now (frees it on chunk drop)
		} catch (const std::exception &e) {
			if (page_idx >= 0)
				PagePool().Release(PagePool().Slot(page_idx));
			st.errored = true;
			st.err_msg = e.what();
			SendScanReply(scan_id, FrameTag::ScanError, st.err_msg.c_str(), st.err_msg.size());
		}
	}

	std::unique_ptr<SessionChannel> ch_;
	int conn_slot_;
	Latch *worker_latch_;
	pid_t worker_pid_;
	bool done_ = false;
	SubTransactionId created_in_subxact_;
	std::unordered_map<uint32_t, std::unique_ptr<BackendScanState>> open_scans_;
};

/* Release open sessions: all of them (InvalidSubTransactionId) on transaction abort
 * or process exit, or only those created inside `subid` on a subtransaction abort. */
void
ReleaseOpenSessions(SubTransactionId subid) {
	for (;;) {
		BackendSession *victim = nullptr;
		for (auto *stream : g_open_streams) {
			if (subid == InvalidSubTransactionId || stream->CreatedInSubxact() == subid) {
				victim = stream;
				break;
			}
		}
		if (victim == nullptr)
			break;
		delete victim; /* destructor cancels, detaches, unregisters */
	}
	if (g_pending_conn_slot >= 0) {
		SessionPool().DetachEnd(g_pending_conn_slot);
		g_pending_conn_slot = -1;
	}
}

/* Kernel remote-scan hook (worker only): open a heap scan. Prefer the shared
 * scan worker pool (parallel producers) when it is up and the backend's snapshot is
 * available; otherwise fall back to the in-backend scan-inversion stream. */
duckdb::unique_ptr<RemoteScanStream>
OpenRemoteScan(duckdb::ClientContext &context, const std::string &scan_sql, bool count_only,
               const RemoteScanInfo &info) {
	SessionChannel *channel = nullptr;
	std::string snapshot_bytes;
	{
		std::lock_guard<std::mutex> lock(g_session_ctx_lock);
		auto it = g_session_ctx.find(&context);
		if (it == g_session_ctx.end())
			return nullptr; /* not a worker session -> run in-process */
		channel = it->second.channel;
		snapshot_bytes = it->second.snapshot_bytes;
	}

	// Temp tables are backend-local: only the requesting backend can see them, so they
	// must use the inversion path, never the separate scan-worker processes.
	if (!info.local_relation && g_scan_workers_spawned > 0 && ScanQueue::Available() && ScanRing::Available() &&
	    PagePool().Available() && !snapshot_bytes.empty()) {
		auto stream =
		    OpenPoolScanStream(MyDatabaseId, scan_sql, count_only, info, snapshot_bytes, *g_settings.scan_producers);
		if (stream)
			return stream;
		/* open/enqueue failed -> fall back to inversion */
	}

	return OpenInversionScanStream(channel, scan_sql, count_only);
}

/* Unregisters the session's context entries when the connection's scope ends (before
 * the ClientContext is destroyed), on both normal and exception exit -- including any
 * nested-connection aliases pointing at the same channel. */
struct SessionCtxGuard {
	duckdb::ClientContext *key;
	~SessionCtxGuard() {
		std::lock_guard<std::mutex> lock(g_session_ctx_lock);
		auto it = g_session_ctx.find(key);
		if (it == g_session_ctx.end())
			return;
		SessionChannel *channel = it->second.channel;
		for (auto e = g_session_ctx.begin(); e != g_session_ctx.end();) {
			e = (e->second.channel == channel) ? g_session_ctx.erase(e) : std::next(e);
		}
	}
};

/* Run one shipped query on its own DuckDB connection and stream the result. Runs on a
 * session thread: the worker is PG-free here (catalog is fetched from the backend via
 * DescribeRel RPC, heap scans run on the backend via scan inversion), and all channel
 * transport goes through the Serialized* helpers that take the global process lock.
 * Errors are C++ exceptions (no PG longjmp), so no PG_TRY -- which would be unsafe off
 * the main thread anyway. */
void
RunOneSession(SessionChannel &ch) {
	FrameTag tag = FrameTag::Error;
	const char *data = nullptr;
	std::size_t len = 0;
	std::string snapshot_bytes;
	if (ch.SerializedRecvControl(&tag, &data, &len) == FrameResult::Ok && tag == FrameTag::Snapshot) {
		snapshot_bytes.assign(data, len);            /* re-shipped to scan worker tasks */
		ch.SerializedRecvControl(&tag, &data, &len); /* next frame: the SQL */
	}
	std::string sql = (tag == FrameTag::Sql) ? std::string(data, len) : std::string();

	/* Thread-local: read by binding/optimization code (catalog RPC, transaction manager,
	 * order pushdown), which runs on this session thread during con.SendQuery. */
	SetCurrentWorkerSession(&ch);
	try {
		/* Own connection on the shared instance: each session gets its own DuckDB
		 * connection so N sessions run concurrently. The engine + its secrets/extensions
		 * were primed once at worker startup, so a fresh connection needs no per-query
		 * refresh (which would touch PG). */
		duckdb::Connection con(WorkerAccess::Database(g_worker));
		duckdb::ClientContext *ctx_key = con.context.get();
		{
			std::lock_guard<std::mutex> lock(g_session_ctx_lock);
			g_session_ctx[ctx_key] = WorkerSessionContext {&ch, std::move(snapshot_bytes)};
		}
		SessionCtxGuard guard {ctx_key};
		RunWorkerSession(con, ch, sql);
	} catch (const std::exception &e) {
		ch.SerializedSendResult(FrameTag::Error, e.what(), std::strlen(e.what()));
	}
	SetCurrentWorkerSession(nullptr);
}

/* Session thread entry: validate + attach the connection slot (palloc in shm_mq_attach
 * -> under the global process lock), run the query, detach. A stale entry -- its
 * backend released the slot (or another backend re-acquired it) after enqueueing -- is
 * skipped via the generation check. The done flag lets the main loop reap the thread. */
void
SessionThreadMain(PendingSession session, std::shared_ptr<std::atomic<bool>> done) {
	int conn_slot = (int)session.conn_slot;
	std::unique_ptr<SessionChannel> ch;
	if (SessionPool().TryAttachEnd(conn_slot, session.conn_generation)) {
		std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
		ch = SessionChannel::AttachSlot(SessionPool().ChannelBase(conn_slot));
	}
	if (ch) {
		RunOneSession(*ch);
		std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
		ch.reset(); /* shm_mq_detach -> under the lock */
		SessionPool().DetachEnd(conn_slot);
	}
	done->store(true, std::memory_order_release);
}

} // namespace

DuckdbWorker::DuckdbWorker(const Settings &settings) : settings_(settings) {
}

void
DuckdbWorker::Init() {
	g_settings = settings_;
	g_worker = this;

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ShmemRequest;
#else
	ShmemRequest();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ShmemStartup;
}

bool
DuckdbWorker::InWorker() {
	return g_in_duckdb_worker;
}

duckdb::unique_ptr<WorkerResultStream>
DuckdbWorker::OpenSession(const std::string &sql) {
	pid_t worker_pid = 0;
	Latch *worker_latch = EnsureWorkerForMyDatabase(&worker_pid);
	EnsureBackendCleanupRegistered();

	/* Serialize the snapshot before claiming a slot: palloc may error, and the slot is
	 * only tracked for cleanup (g_pending_conn_slot) from the moment it is claimed. */
	Snapshot snap = GetActiveSnapshot();
	Size snap_sz = EstimateSnapshotSpace(snap);
	char *snap_buf = (char *)palloc(snap_sz);
	SerializeSnapshot(snap, snap_buf);

	/* Wait (cancellably) for a free session slot rather than falling back to
	 * in-process execution: spinning up a per-backend engine per overflow query is
	 * exactly the memory blow-up the shared worker exists to prevent. The wait ends
	 * on cancel/statement_timeout like any other statement. */
	int conn_slot;
	for (;;) {
		conn_slot = SessionPool().Acquire();
		if (conn_slot >= 0)
			break;
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
	SessionPool().AttachEnd(conn_slot); /* the backend's hold on the slot */
	g_pending_conn_slot = conn_slot;    /* released by the abort callback until a stream owns it */
	auto ch = SessionChannel::OpenSlot(SessionPool().ChannelBase(conn_slot));
	if (!ch) {
		SessionPool().DetachEnd(conn_slot);
		g_pending_conn_slot = -1;
		pfree(snap_buf);
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("could not open session channel")));
	}

	/* Publish the session to the worker's pending ring, waiting out transient states
	 * (worker died -> respawn it; ring not yet drained -> retry) instead of falling
	 * back. The interrupt check makes the wait cancellable; the abort callback then
	 * releases the claimed slot. */
	uint32 conn_generation = SessionPool().Generation(conn_slot);
	for (;;) {
		bool queued = false;
		bool have_worker = false;
		SpinLockAcquire(&g_duckdb_workers->lock);
		int slot = FindWorkerSlot(MyDatabaseId);
		if (slot >= 0) {
			have_worker = true;
			queued = EnqueuePendingSession(slot, (uint32)conn_slot, conn_generation);
			if (queued) {
				worker_latch = g_duckdb_workers->workers[slot].worker_latch;
				worker_pid = g_duckdb_workers->workers[slot].worker_pid;
			}
		}
		SpinLockRelease(&g_duckdb_workers->lock);
		if (queued)
			break;
		if (!have_worker) {
			worker_latch = EnsureWorkerForMyDatabase(&worker_pid); /* respawns a dead worker */
			continue;
		}
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}

	SetLatch(worker_latch);
	/* Ship the backend's active snapshot, then the SQL, so heap reads produced for the
	 * worker use this backend's MVCC view. */
	ch->SendControl(FrameTag::Snapshot, snap_buf, snap_sz);
	pfree(snap_buf);
	ch->SendControl(FrameTag::Sql, sql.c_str(), sql.size());

	auto stream = duckdb::make_uniq<BackendSession>(std::move(ch), conn_slot, worker_latch, worker_pid);
	g_pending_conn_slot = -1; /* the stream owns the slot now */
	return stream;
}

SessionChannel *
WorkerSessionForContext(duckdb::ClientContext *ctx) {
	std::lock_guard<std::mutex> lock(g_session_ctx_lock);
	auto it = g_session_ctx.find(ctx);
	return it == g_session_ctx.end() ? nullptr : it->second.channel;
}

SessionChannel *
EffectiveWorkerSession(duckdb::ClientContext *ctx) {
	if (auto *session = CurrentWorkerSession())
		return session;
	if (ctx == nullptr)
		return nullptr;
	return WorkerSessionForContext(ctx);
}

void
AliasWorkerSessionContext(duckdb::ClientContext *ctx, duckdb::ClientContext *primary) {
	std::lock_guard<std::mutex> lock(g_session_ctx_lock);
	auto it = g_session_ctx.find(primary);
	if (it == g_session_ctx.end() || ctx == primary)
		return;
	g_session_ctx[ctx] = it->second;
}

void
UnaliasWorkerSessionContext(duckdb::ClientContext *ctx) {
	std::lock_guard<std::mutex> lock(g_session_ctx_lock);
	g_session_ctx.erase(ctx);
}

uint64_t
DuckdbWorker::DispatchCount() const {
	if (g_duckdb_workers == nullptr)
		return 0;
	uint64 n = 0;
	SpinLockAcquire(&g_duckdb_workers->lock);
	int idx = FindWorkerSlot(MyDatabaseId);
	if (idx >= 0)
		n = g_duckdb_workers->workers[idx].dispatched;
	SpinLockRelease(&g_duckdb_workers->lock);
	return n;
}

void
DuckdbWorker::Main(uint32_t db_oid_arg) {
	Oid db_oid = (Oid)db_oid_arg;

	g_in_duckdb_worker = true;
	/* Heap scans run on the requesting backend (scan inversion), so the worker's
	 * execution threads are PG-free; install the remote-scan hook here (worker only,
	 * never in regular backends). */
	pgddb_open_remote_scan_hook = OpenRemoteScan;

	/* Custom SIGTERM: just flag + wake. The worker runs session threads, so it must stop
	 * them before exiting -- die()'s CHECK_FOR_INTERRUPTS path would proc_exit while
	 * threads still touch shmem. The main loop drains threads, then exits cleanly. */
	pqsignal(SIGTERM, WorkerSigterm);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(db_oid, InvalidOid, 0);

	Configure();

	SpinLockAcquire(&g_duckdb_workers->lock);
	int idx = FindWorkerSlot(db_oid);
	if (idx >= 0) {
		g_duckdb_workers->workers[idx].worker_pid = MyProcPid;
		g_duckdb_workers->workers[idx].worker_latch = MyLatch;
	}
	SpinLockRelease(&g_duckdb_workers->lock);

	elog(LOG, "%s duckdb worker started for database %u, pid %d", g_settings.display_name, db_oid, MyProcPid);

	/* Prime the engine once, inside a transaction: lazy engine init reads PG catalog.
	 * After this the per-query path reuses the primed instance and never re-initializes,
	 * so it can run transaction-free. */
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	Prime();
	PopActiveSnapshot();
	CommitTransactionCommand();

	SpawnScanWorkers(db_oid);

	/* Drain session threads before any process exit -- including elog(FATAL), which
	 * proc_exits without returning through the main loop; a thread touching shmem or
	 * DuckDB during teardown (or left attached to its connection slot) must not
	 * survive it. */
	before_shmem_exit(
	    [](int, Datum) {
		    SetWorkerDraining(true);
		    for (auto &s : g_worker_sessions) {
			    if (s.thread.joinable())
				    s.thread.join();
		    }
		    g_worker_sessions.clear();
	    },
	    (Datum)0);

	while (!g_duckdb_worker_got_sigterm) {
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		/* Propagate backend cancellations into running DuckDB queries: a session whose
		 * channel has the cancel flag set gets its ClientContext interrupted, so the
		 * query aborts promptly instead of computing to the next chunk boundary. The
		 * backend sets the flag and pokes this latch (RequestCancel + SetLatch). */
		{
			std::lock_guard<std::mutex> lock(g_session_ctx_lock);
			for (auto &entry : g_session_ctx) {
				if (entry.second.channel->IsCancelRequested())
					entry.first->Interrupt();
			}
		}

		/* Reap finished session threads. */
		for (std::size_t i = 0; i < g_worker_sessions.size();) {
			if (g_worker_sessions[i].done->load(std::memory_order_acquire)) {
				g_worker_sessions[i].thread.join();
				g_worker_sessions.erase(g_worker_sessions.begin() + i);
			} else {
				i++;
			}
		}

		/* Drain pending sessions; spawn a thread for each. */
		for (;;) {
			PendingSession session {};
			bool have = false;
			SpinLockAcquire(&g_duckdb_workers->lock);
			idx = FindWorkerSlot(db_oid);
			if (idx >= 0) {
				DuckdbWorkerSlot &w = g_duckdb_workers->workers[idx];
				if (w.pending_head != w.pending_tail) {
					session = w.pending[w.pending_head % MAX_PENDING_SESSIONS];
					w.pending_head++;
					have = true;
				}
			}
			SpinLockRelease(&g_duckdb_workers->lock);
			if (!have)
				break;

			auto done = std::make_shared<std::atomic<bool>>(false);
			g_worker_sessions.push_back(WorkerSession {std::thread(SessionThreadMain, session, done), done});
		}
	}

	/* Shutdown: signal session-thread polls to bail, then join them before exiting so no
	 * thread touches shmem/DuckDB during process teardown. */
	SetWorkerDraining(true);
	for (auto &s : g_worker_sessions) {
		if (s.thread.joinable())
			s.thread.join();
	}
	g_worker_sessions.clear();

	elog(LOG, "%s duckdb worker for database %u shutting down", g_settings.display_name, db_oid);
}

/* A scan worker: idles on its latch, claims block-range tasks for its database, and
 * runs each one (PostgresTableReader under the shipped snapshot -> Arrow/serialized
 * pages into the scan's ready-ring). PG-only; never runs DuckDB. */
void
DuckdbWorker::ScanWorkerMain(uint32_t db_oid_arg) {
	Oid db_oid = (Oid)db_oid_arg;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(db_oid, InvalidOid, 0);

	/* The producer's own reader stays single-threaded (no nested PG parallel workers). */
	SetConfigOption("duckdb.max_workers_per_postgres_scan", "0", PGC_SUSET, PGC_S_SESSION);

	before_shmem_exit([](int, Datum) { ScanQueue().UnpublishWorker(); }, (Datum)0);
	ScanQueue().PublishWorker((uint32)db_oid);
	elog(LOG, "%s scan worker started for database %u, pid %d", g_settings.display_name, db_oid, MyProcPid);

	while (!ShutdownRequestPending) {
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();

		ScanRange range;
		ScanQueue q;
		while (q.Claim((uint32)db_oid, &range)) {
			ProcessScanRange(range);
		}
	}

	elog(LOG, "%s scan worker for database %u shutting down", g_settings.display_name, db_oid);
}

} // namespace pgddb
