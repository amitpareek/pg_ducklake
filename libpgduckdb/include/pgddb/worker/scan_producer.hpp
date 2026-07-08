#pragma once
// Heap-scan production for the duckdb worker's remote scans (kernel-internal, used by
// duckdb_worker.cpp and the scan-producer worker main). The backend (scan inversion) and
// the scan-pool workers share the same producer: a streaming PostgresTableReader whose
// tuples are encoded as Arrow batches into shared pool pages (COUNT(*) is the one
// serialized BIGINT chunk).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "pgddb/pg/declarations.hpp"
#include "pgddb/worker/transport/arrow_codec.hpp"
#include "pgddb/worker/transport/scan_queue.hpp"
#include "pgddb/worker/worker_dispatch.hpp"

#include <duckdb/common/types/data_chunk.hpp>

namespace pgddb {

class PostgresTableReader;
class SessionChannel;

// A streaming heap-scan producer for one remote scan: runs the inner scan SQL through
// the PG executor and converts pulled tuples one Arrow batch at a time -- no full
// materialization, so producer memory stays bounded and the first batch ships as soon
// as it is filled.
struct BackendScanState {
	BackendScanState();
	duckdb::unique_ptr<PostgresTableReader> reader;
	std::vector<TupleTableSlot *> slots;
	duckdb::DataChunk chunk; // COUNT(*) only: the single BIGINT count chunk
	int ncols = 0;
	bool is_count = false;
	bool done = false;
	bool errored = false; // a produce error occurred; reply ScanError to later fetches
	std::string err_msg;

	// Per-column Arrow encoding (the only data transport; every column must be encodable).
	std::vector<ArrowColCode> arrow_codes;
	std::vector<uint8_t> arrow_prec, arrow_scale;
	TupleTableSlot *carry = nullptr; // a row pulled but deferred to the next page (overflow)
	bool has_carry = false;

	~BackendScanState();
};

void InitBackendScan(BackendScanState &st, const std::string &sql, bool count_only);

// Build one Arrow record batch into `page` from the reader. Returns the descriptor
// length written to `desc_out` (>0), or 0 at end of scan. Throws if the column layout
// cannot fit an empty batch in the page (no serialize fallback by design).
ssize_t ProduceArrowChunk(BackendScanState &st, char *page, std::size_t capacity, int page_index, char *desc_out,
                          std::size_t desc_cap);

// COUNT(*): sum the reader's partial counts into st.chunk as one BIGINT. Returns false
// once already produced.
bool ProduceCountChunk(BackendScanState &st);

// Scan-worker side: run one claimed scan task in its own transaction under the
// requesting backend's shipped snapshot, pushing pages into the scan's ready-ring.
void ProcessScanRange(const ScanRange &range);

// Worker side: the per-fetch scan-inversion stream over the session channel.
duckdb::unique_ptr<RemoteScanStream> OpenInversionScanStream(SessionChannel *ch, const std::string &sql,
                                                             bool count_only);

// Worker side: register the scan with the ready-ring, split it into up to `producers`
// block-range tasks, and return the pool-fed stream; nullptr if registration or
// enqueueing failed (caller falls back to the inversion stream).
duckdb::unique_ptr<RemoteScanStream> OpenPoolScanStream(uint32_t db_oid, const std::string &scan_sql, bool count_only,
                                                        const RemoteScanInfo &info, const std::string &snapshot_bytes,
                                                        int producers);

} // namespace pgddb
