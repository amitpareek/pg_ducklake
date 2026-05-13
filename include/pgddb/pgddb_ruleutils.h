#ifndef PGDDB_RULEUTILS_H
#define PGDDB_RULEUTILS_H

#include "postgres.h"
#include "pgddb/vendor/pg_list.hpp"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct StarReconstructionContext {
	List *target_list;
	int varno_star;
	int varattno_star;
	bool added_current_star;
} StarReconstructionContext;

/*
 * Hook surface that the vendored ruleutils calls into. Standard PG hook
 * pattern: extension installs an impl in _PG_init, chains to prev_hook.
 *
 * Each hook has a thin wrapper (pgddb_<name>) below that null-checks the
 * global so vendored ruleutils call sites stay one-line. A null hook
 * means "no consumer override" — the wrapper returns a sensible default
 * (false / NULL / unchanged input) so the vendored deparser falls through
 * to its built-in PG behaviour.
 */

typedef char *(*pgddb_function_name_hook_t)(Oid funcid, bool *use_variadic_p);
typedef bool (*pgddb_is_fake_type_hook_t)(Oid type_oid);
typedef bool (*pgddb_var_is_row_hook_t)(Var *var);
typedef Var *(*pgddb_subscript_var_hook_t)(Expr *expr);
typedef bool (*pgddb_func_returns_row_hook_t)(RangeTblFunction *rtfunc);
typedef bool (*pgddb_replace_subquery_with_view_hook_t)(Query *query, StringInfo buf);
typedef int (*pgddb_show_type_hook_t)(Const *constval, int original_showtype);
typedef bool (*pgddb_reconstruct_star_step_hook_t)(StarReconstructionContext *ctx, ListCell *tle_cell);
typedef SubscriptingRef *(*pgddb_strip_first_subscript_hook_t)(SubscriptingRef *sbsref, StringInfo buf);
typedef bool (*pgddb_subscript_has_custom_alias_hook_t)(Plan *plan, List *rtable, Var *subscript_var, char *colname);
typedef char *(*pgddb_write_row_refname_hook_t)(StringInfo buf, char *refname, bool is_top_level);
typedef List *(*pgddb_db_and_schema_hook_t)(const char *postgres_schema_name, const char *table_am_name);

extern pgddb_function_name_hook_t pgddb_function_name_hook;
extern pgddb_is_fake_type_hook_t pgddb_is_fake_type_hook;
extern pgddb_var_is_row_hook_t pgddb_var_is_row_hook;
extern pgddb_subscript_var_hook_t pgddb_subscript_var_hook;
extern pgddb_func_returns_row_hook_t pgddb_func_returns_row_hook;
extern pgddb_replace_subquery_with_view_hook_t pgddb_replace_subquery_with_view_hook;
extern pgddb_show_type_hook_t pgddb_show_type_hook;
extern pgddb_reconstruct_star_step_hook_t pgddb_reconstruct_star_step_hook;
extern pgddb_strip_first_subscript_hook_t pgddb_strip_first_subscript_hook;
extern pgddb_subscript_has_custom_alias_hook_t pgddb_subscript_has_custom_alias_hook;
extern pgddb_write_row_refname_hook_t pgddb_write_row_refname_hook;
extern pgddb_db_and_schema_hook_t pgddb_db_and_schema_hook;

/* Null-check wrappers called from vendored ruleutils. */
static inline char *
pgddb_function_name(Oid funcid, bool *use_variadic_p) {
	return pgddb_function_name_hook ? pgddb_function_name_hook(funcid, use_variadic_p) : NULL;
}

static inline bool
pgddb_is_fake_type(Oid type_oid) {
	return pgddb_is_fake_type_hook ? pgddb_is_fake_type_hook(type_oid) : false;
}

static inline bool
pgddb_var_is_row(Var *var) {
	return pgddb_var_is_row_hook ? pgddb_var_is_row_hook(var) : false;
}

static inline Var *
pgddb_subscript_var(Expr *expr) {
	return pgddb_subscript_var_hook ? pgddb_subscript_var_hook(expr) : NULL;
}

static inline bool
pgddb_func_returns_row(RangeTblFunction *rtfunc) {
	return pgddb_func_returns_row_hook ? pgddb_func_returns_row_hook(rtfunc) : false;
}

static inline bool
pgddb_replace_subquery_with_view(Query *query, StringInfo buf) {
	return pgddb_replace_subquery_with_view_hook ? pgddb_replace_subquery_with_view_hook(query, buf) : false;
}

static inline int
pgddb_show_type(Const *constval, int original_showtype) {
	return pgddb_show_type_hook ? pgddb_show_type_hook(constval, original_showtype) : original_showtype;
}

static inline bool
pgddb_reconstruct_star_step(StarReconstructionContext *ctx, ListCell *tle_cell) {
	return pgddb_reconstruct_star_step_hook ? pgddb_reconstruct_star_step_hook(ctx, tle_cell) : false;
}

static inline SubscriptingRef *
pgddb_strip_first_subscript(SubscriptingRef *sbsref, StringInfo buf) {
	return pgddb_strip_first_subscript_hook ? pgddb_strip_first_subscript_hook(sbsref, buf) : sbsref;
}

static inline bool
pgddb_subscript_has_custom_alias(Plan *plan, List *rtable, Var *subscript_var, char *colname) {
	return pgddb_subscript_has_custom_alias_hook
	           ? pgddb_subscript_has_custom_alias_hook(plan, rtable, subscript_var, colname)
	           : false;
}

static inline char *
pgddb_write_row_refname(StringInfo buf, char *refname, bool is_top_level) {
	return pgddb_write_row_refname_hook ? pgddb_write_row_refname_hook(buf, refname, is_top_level) : refname;
}

/*
 * Generic deparser helpers (not hooks; same logic for every consumer).
 */
bool pgddb_is_not_default_expr(Node *node, void *context);
bool is_system_sampling(const char *tsm_name, int num_args);
bool is_bernoulli_sampling(const char *tsm_name, int num_args);
void pgddb_add_tablesample_percent(const char *tsm_name, StringInfo buf, int num_args);

/*
 * Entry points and helpers that still live in libpgddb under the legacy
 * pgduckdb_* names. They will be renamed and the pg_duckdb-only ones will
 * move back to the consumer side in the next slice.
 */
char *pgduckdb_relation_name(Oid relid);
char *pgduckdb_get_querydef(Query *);
char *pgduckdb_get_tabledef(Oid relation_id);
char *pgduckdb_get_alter_tabledef(Oid relation_oid, AlterTableStmt *alter_stmt);
char *pgduckdb_get_rename_relationdef(Oid relation_oid, RenameStmt *rename_stmt);
char *pgduckdb_get_viewdef(const ViewStmt *stmt, const char *postgres_schema_name, const char *view_name,
                           const char *duckdb_query_string);
List *pgduckdb_db_and_schema(const char *postgres_schema_name, const char *duckdb_table_am_name);
const char *pgduckdb_db_and_schema_string(const char *postgres_schema_name, const char *duckdb_table_am_name);

extern bool outermost_query;

#ifdef __cplusplus
}
#endif

#endif /* PGDDB_RULEUTILS_H */
