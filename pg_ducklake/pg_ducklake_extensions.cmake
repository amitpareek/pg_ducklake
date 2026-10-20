duckdb_extension_load(json)
duckdb_extension_load(icu)
# DuckDB 2.0: only extensions listed here are linked into libduckdb; postgres_scanner and
# ducklake are built as separate archives that the pg_ducklake .so links itself (was DONT_LINK).
duckdb_extension_statically_link(json icu)
duckdb_extension_load(postgres_scanner
    GIT_URL https://github.com/duckdb/duckdb-postgres
    GIT_TAG c0f5a3c0c4b68b02afbf669d36d7f0e503985c88
    SUBMODULES database-connector
)
duckdb_extension_load(ducklake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/third_party/ducklake
)
