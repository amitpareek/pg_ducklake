# TLA+ models of the shared DuckDB duckdb worker

Formal models of the three concurrency cores of the shared per-database DuckDB
duckdb worker (design: `libpgduckdb/worker/DESIGN.md`, vocabulary:
`libpgduckdb/worker/GLOSSARY.md`). Each TLA+ action is commented with the C++
operation it stands for. Constants are kept small so every check finishes in
seconds.

## Run

```sh
JAR=<path>/tla2tools.jar   # e.g. the VS Code/Cursor TLA+ extension's tools/tla2tools.jar
cd specs/tla
java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -config <cfg> -workers auto <spec>.tla
```

Expected-PASS configs report "No error has been found"; the negative configs
fail exactly as described below.

---

# ControlProtocol.tla: session-pool session lifecycle

Models the v2 lifecycle by which a backend gets a session onto the worker and
the session slot is returned (`DuckdbWorker::OpenSession` / the worker main
loop / `SessionThreadMain` in `libpgduckdb/worker/duckdb_worker.cpp`, refcounts
in `libpgduckdb/worker/transport/session_pool.cpp`): slot Acquire (which
bumps the slot generation) + the backend's AttachEnd, the pending-session ring
enqueue of `{slot, generation}`, the worker draining the ring and a session
thread attaching via `TryAttachEnd(idx, generation)` (stale entries are
skipped), both ends DetachEnd-ing, and the detach that reaches refcount 0
freeing the slot.

Capacity never falls back to in-process execution: `OpenSession` WAITS. A full
pool means the backend retries `Acquire` in a cancellable loop (interrupt
check + 10ms latch wait) until a slot frees; a full pending ring or a missing
worker at enqueue time means the backend retries the enqueue, respawning a
dead worker via `EnsureWorkerForMyDatabase` first. In the model a waiting
backend simply has no enabled transition until capacity appears (weak fairness
on its acquire/enqueue actions is the retry loop); in-process execution exists
only for statements the dispatch gate excludes semantically, outside this
model. Failure surface: backend abort in every phase (the xact-abort /
shmem-exit cleanup callbacks) -- during the pool-full wait (nothing held yet),
during the ring-full wait (the claimed slot is released), and while pending,
which leaves a stale ring entry -- plus worker crash + registry
reclaim/respawn (the pid probe in `EnsureWorkerForMyDatabase`).

The v1 model (single-session slot handshake, dsm handle,
SESSION_IDLE/REQUESTED/PROCESSING, lost-wakeup config) described machinery
that was deleted; it is fully replaced by this one. The data plane is modelled
separately in ScanInversion.tla.

## What it checks

- `NoUnderflow` / `RefcountBounded` (safety): the attach refcount stays in
  0..2 -- no DetachEnd without a matching attach (which would wrap the C++
  uint32), at most the two legitimate ends.
- `FreeImpliesUnattached` (safety): a FREE slot has no attached ends -- no
  double-free, no attach to a slot that was already returned.
- `SingleBackendPerSlot` (safety): two live backends never hold one slot.
- `NoSlotLeak` (liveness): the pool eventually returns (and stays) fully FREE.
- `SessionSettles` (liveness): every backend -- including one waiting out a
  full pool or a full pending ring -- is eventually served to completion or
  settles cleanly (abort, worker-death error). No starved wait, no backend
  blocked forever, no enqueued session silently lost.

## Configs

- `ControlProtocol.cfg` -- the real protocol: 3 backends, 2 slots, pending ring
  of 1, so both capacity WAITS are exercised (one backend waits for a free
  slot, one waits holding its slot for the ring to drain); aborts allowed in
  EVERY backend phase including during either wait and while PENDING (the
  stale ring entry the generation check must reject),
  `ValidateGeneration = TRUE`, no worker crash. **PASS** -- including
  `SessionSettles`/`Liveness`, i.e. the waits cannot starve.
  Last run: 3,796 states generated, 1,339 distinct, no error.
- `ControlProtocol_staleentry_bug.cfg` -- the pre-fix stale-entry race
  (`ValidateGeneration = FALSE`: the old `SessionThreadMain` attached the
  enqueued slot index unconditionally). A backend abort while PENDING frees
  the slot but leaves its ring entry; serving it attaches a FREE or
  re-acquired slot. **FAILS `FreeImpliesUnattached`** (other interleavings
  reach refcount 3 or an underflow). This race was found by this model and is
  now FIXED in the code with slot generations + `TryAttachEnd` -- the passing
  main config checks the fix under the same adversary.
  Last run: violation found at depth 5 (state counts at detection vary with -workers).
- `ControlProtocol_workercrash.cfg` -- counterfactual HARD worker kill: session
  threads vanish without DetachEnd and no shmem reset follows, so a slot whose
  worker end was attached at kill time keeps refcount 1 forever. **FAILS:
  deadlock at the leaked terminal state** (equivalently
  `NoSlotLeak`/`Liveness`). What the real code covers: ERROR/FATAL worker
  exits run a `before_shmem_exit` drain that joins every session thread (each
  DetachEnds) -- those are ordinary `SessionFinish` steps in the passing
  config; SIGKILL/segfault skip the callback but trigger a postmaster
  crash-restart that reinitializes shared memory. The modeled leak is the
  state that would persist if neither mechanism existed -- i.e. why the drain
  matters. Kill *before* the worker attached is handled cleanly either way
  (backend pid-probes and detaches to 0).
  Last run: deadlock trace found (state counts at detection vary with -workers).
- `ControlProtocol_bug.cfg` -- the bug class the implemented design prevents:
  Acquire and the backend's AttachEnd as two steps with an untracked crash
  window between them. **FAILS: deadlock at a leaked slot** (INUSE, refcount
  0, nobody will ever detach it). The real code closes the window by putting
  nothing fallible between Acquire and AttachEnd (the snapshot palloc happens
  before Acquire) and tracking the slot for cleanup from the attach onward
  (`g_pending_conn_slot`, then the open-stream registry).
  Last run: deadlock trace found (state counts at detection vary with -workers).

---

# ScanInversion.tla: scan read-ahead + channel demux

Models the scan-inversion sub-protocol *and the channel demux*:
`InversionScanStream::Next` (worker consumer, `worker/scan_producer.cpp`)
talking to `BackendSession::ServiceScanFetch`/`SendScanReply` (backend
producer, `worker/duckdb_worker.cpp`) over the session channel, with replies
routed by `SessionChannel::RoutedRecv` (`worker/transport/session_channel.cpp`).
Every scan reply carries a uint32 scan_id prefix and is demultiplexed into a
per-scan lane; metadata replies go to the single meta lane. This lifted the
old constraint that two scans on one channel were only safe if DuckDB drove
them sequentially: the model now checks two scans running CONCURRENTLY, plus
an interleaved metadata round-trip (`MetadataRoundTrip` in
`session_protocol.cpp`, serialized per channel by `MetaRequestMutex`).

## What it checks

- `WindowBounded` / `RequestsBounded` (safety): at most W (WINDOW) fetches
  outstanding per scan; the result FIFO holds at most W per scan + 1 meta
  request.
- `FIFOMatching` (safety): each scan consumes exactly the 0,1,2,... prefix of
  its own produced chunks -- no reorder, loss, duplication, or cross-scan
  mis-route. The demux guarantees this for concurrent scans; `RouteByScanId =
  FALSE` shows it breaking without the demux.
- `MetaLaneClean` (safety): routing never puts a scan reply on the meta lane
  or vice versa.
- `PageConservation` + the `Quiescent` terminal (safety): every Arrow page
  handed out is released -- by the import, by `DrainOutstanding`, or by
  `CloseScanLane` freeing routed-but-unconsumed frames.
- `TerminalStableInv` / `TerminalStable` (safety): a finished/errored backend
  scan is kept, not re-opened; extra windowed fetches get the same terminal.
- `Termination` (liveness): the protocol always reaches the clean terminal
  (scans done/errored/torn-down, lanes empty, pages reclaimed, no metadata
  round-trip stuck).

## Configs

- `ScanInversion.cfg` -- one scan, W=2, 2 chunks + ScanDone, Arrow on (1 pool
  page, so fast path and inline fallback both occur), early teardown allowed.
  **PASS.** Last run: 165 states generated, 92 distinct, no error.
- `ScanInversion_twoscan.cfg` -- two scans CONCURRENT on one channel (a join's
  two inputs; no sequencing assumption) plus one metadata round-trip
  interleaving (a DuckLake GetFilesForTable from a scheduler thread), Arrow +
  teardown on. Replaces the old `ScanInversion_twoscan_seq.cfg`, whose
  "sequential drive" assumption the demux made unnecessary. **PASS.**
  Last run: 207,153 states generated, 60,968 distinct, no error (~2 s).
- `ScanInversion_noroute_bug.cfg` -- the pre-demux bug class (`RouteByScanId =
  FALSE`): a scan consumes the wire head no matter which scan it was intended
  for. Replaces the old `ScanInversion_alias.cfg`. **FAILS `FIFOMatching`**
  with a cross-scan mis-routing trace -- exactly what the scan_id prefix +
  demux lanes prevent. Last run: violation found at depth 8 (state counts at detection vary with -workers).

---

# ScanPool.tla: shared scan-producer pool

Models the page lifecycle and the producer/consumer/teardown protocol for one
pool scan: scan workers (`ProcessScanRange` in `worker/scan_producer.cpp`,
spawned by `DuckdbWorker::ScanWorkerMain` in `worker/duckdb_worker.cpp`) produce
Arrow pages into the scan's ready-ring (`worker/transport/scan_ring.cpp`,
pages from `worker/transport/page_pool.cpp`); the worker's consumer
(`PoolScanStream::Next`) drains it via `ScanRing::TryNext` and
`ScanRing::Close` reclaims queued pages on teardown.

## What it checks

- `PageConservation` (safety): the four page locations -- free stack, held by a
  producer, queued in the ready-ring, in-flight in the consumer -- always
  partition the pool. Catches page leaks and double-free.
- `RingBounded` (safety): the ready-ring never exceeds its capacity.
- `Termination` (liveness): the scan always reaches a quiescent terminal --
  consumer finished or aborted, every producer stopped, every page back on the
  free stack.

## Configs

- `ScanPool.cfg` -- fixed protocol: producers may fail mid-task but report the
  error (the PG_TRY in `ProcessScanRange` -> `ScanRing::SetError`), no
  external rescue. **PASS.** Last run: 1,167 states generated, 433 distinct.
- `ScanPool_bug.cfg` -- pre-fix bug: a failing producer dies silently
  (`ReportErrors = FALSE`). **FAILS: deadlock** at the hung state (`done <
  NP`, no error, ring empty, consumer draining): the consumer polls forever.
  This is the hang the PG_TRY fix prevents.
  Last run: deadlock trace found (state counts at detection vary with -workers).
- `ScanPool_teardown.cfg` -- cancel/LIMIT teardown plus failures mid-flight,
  exercising `ScanRing::Close` and producers releasing held pages. **PASS.**
  Last run: 2,282 states generated, 719 distinct.

---

# Model-vs-code findings

Two races surfaced while first writing these models; both have since been
addressed in the code, and the models now check the fixes:

1. **Stale pending-ring entry after an early backend abort** -- FIXED. Pending
   entries now carry `{conn_slot, conn_generation}`; `SessionPool::Acquire`
   bumps the slot generation, and the worker's `SessionThreadMain` attaches via
   `TryAttachEnd(idx, generation)`, which rejects a slot that is FREE or at a
   different generation. `ControlProtocol.cfg` (PASS) includes the
   abort-while-pending adversary; `ControlProtocol_staleentry_bug.cfg` (FAIL)
   preserves the pre-fix behavior as the regression illustration.

2. **Connection-slot refcount stranded by worker death** -- addressed for the
   reachable cases. The duckdb worker now drains its session threads from a
   `before_shmem_exit` callback, so ERROR/FATAL exits DetachEnd cleanly; hard
   kills (SIGKILL, segfault) skip the callback but cause a postmaster
   crash-restart that reinitializes shared memory, so no stranded refcount
   persists. `ControlProtocol_workercrash.cfg` (FAIL) keeps the counterfactual
   "hard kill, no drain, no shmem reset" as the illustration of why the drain
   exists.

The model treats `DetachEnd` as one atomic step; the code matches it:
`DetachEnd`'s decrement and its free decision both run under the pool
spinlock, so they are atomic against `TryAttachEnd`'s check + increment
(a decrement landing between them could otherwise free a slot the worker
just attached).
