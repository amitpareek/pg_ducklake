#pragma once

#include <inttypes.h>

#include "pg_config.h" // macro-only PG header, for PG_VERSION_NUM

// Postgres C declarations for mostly-C++ files; must contain no C++, only PG C decls.

extern "C" {
typedef int16_t AttrNumber;

typedef uint32_t BlockNumber;

typedef int Buffer;

struct BufferAccessStrategyData;
typedef struct BufferAccessStrategyData *BufferAccessStrategy;

typedef double Cardinality;

#if PG_VERSION_NUM >= 190000
typedef uint64_t Datum;
#else
typedef uintptr_t Datum;
#endif

struct MemoryContextData;
typedef MemoryContextData *MemoryContext;

struct FormData_pg_attribute;
typedef FormData_pg_attribute *Form_pg_attribute;

struct FormData_pg_class;
typedef FormData_pg_class *Form_pg_class;

struct HeapTupleData;
typedef HeapTupleData *HeapTuple;

struct List;

struct Node;

typedef uint16_t OffsetNumber;

typedef unsigned int Oid;

struct ParamListInfoData;
typedef struct ParamListInfoData *ParamListInfo;

struct CallStmt;
struct CopyStmt;
struct DropStmt;
struct IndexStmt;
struct PlannedStmt;

typedef char PageData;
typedef PageData *Page;

struct Query;

struct RelationData;
typedef struct RelationData *Relation;

struct SnapshotData;
typedef struct SnapshotData *Snapshot;

struct TupleDescData;
typedef struct TupleDescData *TupleDesc;

struct TupleTableSlot;

struct TableAmRoutine;

typedef uint32_t CommandId;

typedef uint32_t SubTransactionId;

struct QueryDesc;

struct ParallelExecutorInfo;

struct MinimalTupleData;
typedef MinimalTupleData *MinimalTuple;

struct TupleQueueReader;

struct ObjectAddress;

struct PlanState;

struct Plan;

struct FuncExpr;

typedef struct FunctionCallInfoBaseData *FunctionCallInfo;

struct ExplainState;
}
