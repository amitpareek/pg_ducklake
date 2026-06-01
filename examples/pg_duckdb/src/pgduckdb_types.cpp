#include "duckdb.hpp"

#include "pgddb/pgddb_types.hpp"
#include "pgddb/pgddb_types_array.hpp"

#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"

extern "C" {
#include "postgres.h"
#include "utils/array.h"
}

namespace pgduckdb {

// pgduckdb composite types are stored as text Datums on the PG side.
// All three converters (struct, union, map) stringify the duckdb value.

namespace {

struct StructArray {
	static ArrayType *
	ConstructArray(Datum *datums, bool *nulls, int ndims, int *dims, int *lower_bound) {
		return construct_md_array(datums, nulls, ndims, dims, lower_bound, DuckdbStructOid(), -1, false, 'i');
	}
	static Datum
	ConvertToPostgres(const duckdb::Value &val) {
		return pgddb::ConvertToStringDatum(val);
	}
};

struct UnionArray {
	static ArrayType *
	ConstructArray(Datum *datums, bool *nulls, int ndims, int *dims, int *lower_bound) {
		return construct_md_array(datums, nulls, ndims, dims, lower_bound, DuckdbUnionOid(), -1, false, 'i');
	}
	static Datum
	ConvertToPostgres(const duckdb::Value &val) {
		return pgddb::ConvertToStringDatum(val);
	}
};

struct MapArray {
	static ArrayType *
	ConstructArray(Datum *datums, bool *nulls, int ndims, int *dims, int *lower_bound) {
		return construct_md_array(datums, nulls, ndims, dims, lower_bound, DuckdbMapOid(), -1, false, 'i');
	}
	static Datum
	ConvertToPostgres(const duckdb::Value &val) {
		return pgddb::ConvertToStringDatum(val);
	}
};

} // namespace

// pg_duckdb's type hooks: add the duckdb.struct / duckdb.union / duckdb.map composite
// types (and their array forms) on top of libpgddb's built-in Postgres types. Each hook
// handles its own Oids/types and returns false to decline (the kernel then tries the next
// registered hook, or its built-in fallback).

static bool
ConvertPostgresToBaseDuckColumnType(Oid pg_oid, duckdb::LogicalType &out) {
	if (pg_oid == DuckdbStructOid() || pg_oid == DuckdbStructArrayOid()) {
		out = duckdb::LogicalTypeId::STRUCT;
		return true;
	}
	if (pg_oid == DuckdbUnionOid() || pg_oid == DuckdbUnionArrayOid()) {
		out = duckdb::LogicalTypeId::UNION;
		return true;
	}
	if (pg_oid == DuckdbMapOid() || pg_oid == DuckdbMapArrayOid()) {
		out = duckdb::LogicalTypeId::MAP;
		return true;
	}
	return false;
}

static bool
GetPostgresDuckDBType(const duckdb::LogicalType &type, Oid &out) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::STRUCT:
		out = DuckdbStructOid();
		return true;
	case duckdb::LogicalTypeId::UNION:
		out = DuckdbUnionOid();
		return true;
	case duckdb::LogicalTypeId::MAP:
		out = DuckdbMapOid();
		return true;
	default:
		return false;
	}
}

static bool
GetPostgresArrayDuckDBType(const duckdb::LogicalType &type, Oid &out) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::STRUCT:
		out = DuckdbStructArrayOid();
		return true;
	case duckdb::LogicalTypeId::UNION:
		out = DuckdbUnionArrayOid();
		return true;
	case duckdb::LogicalTypeId::MAP:
		out = DuckdbMapArrayOid();
		return true;
	default:
		return false;
	}
}

static bool
ConvertDuckToPostgresValue(Oid pg_oid, duckdb::Value &value, TupleTableSlot *slot, uint64_t col) {
	if (pg_oid == DuckdbStructOid() || pg_oid == DuckdbUnionOid() || pg_oid == DuckdbMapOid()) {
		slot->tts_values[col] = pgddb::ConvertToStringDatum(value);
		return true;
	}
	if (pg_oid == DuckdbStructArrayOid()) {
		pgddb::ConvertDuckToPostgresArray<StructArray>(slot, value, col);
		return true;
	}
	if (pg_oid == DuckdbUnionArrayOid()) {
		pgddb::ConvertDuckToPostgresArray<UnionArray>(slot, value, col);
		return true;
	}
	if (pg_oid == DuckdbMapArrayOid()) {
		pgddb::ConvertDuckToPostgresArray<MapArray>(slot, value, col);
		return true;
	}
	return false;
}

static bool
ConvertUnsupportedNumericToDouble(void) {
	return duckdb_convert_unsupported_numeric_to_double;
}

void
InitTypeHooks() {
	pgddb::Register_ConvertPostgresToBaseDuckColumnType(ConvertPostgresToBaseDuckColumnType);
	pgddb::Register_GetPostgresDuckDBType(GetPostgresDuckDBType);
	pgddb::Register_GetPostgresArrayDuckDBType(GetPostgresArrayDuckDBType);
	pgddb::Register_ConvertDuckToPostgresValue(ConvertDuckToPostgresValue);
	pgddb::Register_ConvertUnsupportedNumericToDouble(ConvertUnsupportedNumericToDouble);
}

} // namespace pgduckdb
