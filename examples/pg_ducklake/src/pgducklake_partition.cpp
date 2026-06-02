/*
 * pgducklake_partition.cpp -- Partition procs: ducklake.set_partition / reset_partition.
 *
 * @scope extension: ducklake.set_partition, ducklake.reset_partition procs
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"

#include <duckdb/common/error_data.hpp> /* must precede postgres.h (FATAL macro) */

#include "pgducklake/pgducklake_table.hpp"
#include "pgddb/utility/cpp_wrapper.hpp"

#include <string>

extern "C" {
#include "postgres.h"

#include "catalog/pg_type.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"

#include "pgddb/pgddb_ruleutils.h"
}

extern "C" {

DECLARE_PG_FUNCTION(ducklake_set_partition) {
  if (PG_ARGISNULL(0))
    elog(ERROR, "table cannot be NULL");
  if (PG_ARGISNULL(1))
    elog(ERROR, "partition_by cannot be NULL");

  Oid relid = PG_GETARG_OID(0);
  EnsureDuckLakeTable(relid);

  ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
  if (ARR_NDIM(arr) == 0)
    elog(ERROR, "partition_by cannot be empty");

  int nelems;
  Datum *elems;
  bool *nulls;
  deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &elems, &nulls, &nelems);

  if (nelems == 0)
    elog(ERROR, "partition_by cannot be empty");

  std::string spec;
  for (int i = 0; i < nelems; i++) {
    if (nulls[i])
      elog(ERROR, "partition key cannot be NULL");
    if (i > 0)
      spec += ", ";
    spec += text_to_cstring(DatumGetTextPP(elems[i]));
  }

  std::string query =
      std::string("ALTER TABLE ") + pgddb_relation_name(relid) + " SET PARTITIONED BY (" + spec + ")";

  pgducklake::DuckDBQueryOrThrow(query);

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_reset_partition) {
  if (PG_ARGISNULL(0))
    elog(ERROR, "table cannot be NULL");

  Oid relid = PG_GETARG_OID(0);
  EnsureDuckLakeTable(relid);

  std::string query = std::string("ALTER TABLE ") + pgddb_relation_name(relid) + " RESET PARTITIONED BY";

  pgducklake::DuckDBQueryOrThrow(query);

  PG_RETURN_VOID();
}

} // extern "C"
