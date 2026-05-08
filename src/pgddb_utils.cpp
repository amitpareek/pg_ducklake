#include "pgddb/pgddb_duckdb.hpp"

namespace pgddb {

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBManager::DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query) {
	auto res = context.Query(query, false);
	if (res->HasError()) {
		res->ThrowError();
	}
	return res;
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBManager::DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query) {
	return DuckDBQueryOrThrow(*connection.context, query);
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBManager::DuckDBQueryOrThrow(const std::string &query) {
	auto connection = GetConnection();
	return DuckDBQueryOrThrow(*connection, query);
}

} // namespace pgddb
