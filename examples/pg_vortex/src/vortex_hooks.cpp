// pg_vortex's planner hook: routes queries calling pg_vortex.read_vortex to PlanNode, chains otherwise.

#include "duckdb.hpp"

#include "pg_vortex/vortex_hooks.hpp"
#include "pg_vortex/vortex_planner.hpp"

#include "pgddb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "commands/extension.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/syscache.h"
}

namespace pg_vortex {

static planner_hook_type prev_planner_hook = nullptr;

// OID cache for pg_vortex.read_vortex(text); invalidated via syscache
// callback so CREATE/DROP EXTENSION re-resolves on the next query.
struct VortexCache {
	bool valid = false;
	Oid extension_oid = InvalidOid;
	Oid read_vortex_oid = InvalidOid;
};

static VortexCache g_cache;

static void
#if PG_VERSION_NUM >= 190000
InvalidateVortexCache(Datum /*arg*/, SysCacheIdentifier /*cacheid*/, uint32 /*hashvalue*/) {
#else
InvalidateVortexCache(Datum /*arg*/, int /*cacheid*/, uint32 /*hashvalue*/) {
#endif
	g_cache.valid = false;
}

static Oid
FindReadVortexOid(Oid extension_oid) {
	CatCList *catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum("read_vortex"));
	Oid found = InvalidOid;
	for (int i = 0; i < catlist->n_members; i++) {
		HeapTuple proctup = &catlist->members[i]->tuple;
		Oid procoid = ((Form_pg_proc)GETSTRUCT(proctup))->oid;
		if (getExtensionOfObject(ProcedureRelationId, procoid) == extension_oid) {
			found = procoid;
			break;
		}
	}
	ReleaseSysCacheList(catlist);
	return found;
}

static void
RefreshVortexCache() {
	if (g_cache.valid) {
		return;
	}
	g_cache.extension_oid = get_extension_oid("pg_vortex", true /*missing_ok*/);
	if (OidIsValid(g_cache.extension_oid)) {
		g_cache.read_vortex_oid = FindReadVortexOid(g_cache.extension_oid);
	} else {
		g_cache.read_vortex_oid = InvalidOid;
	}
	g_cache.valid = true;
}

static bool
ContainsReadVortex(Node *node, void *context) {
	if (node == nullptr) {
		return false;
	}
	if (IsA(node, Query)) {
		Query *query = (Query *)node;
#if PG_VERSION_NUM >= 160000
		return query_tree_walker(query, ContainsReadVortex, context, 0);
#else
		return query_tree_walker(query, (bool (*)())((void *)ContainsReadVortex), context, 0);
#endif
	}
	if (IsA(node, FuncExpr)) {
		FuncExpr *func = castNode(FuncExpr, node);
		if (func->funcid == g_cache.read_vortex_oid) {
			return true;
		}
	}
	if (IsA(node, RangeTblFunction)) {
		RangeTblFunction *rtf = castNode(RangeTblFunction, node);
		if (rtf->funcexpr && IsA(rtf->funcexpr, FuncExpr)) {
			FuncExpr *func = castNode(FuncExpr, rtf->funcexpr);
			if (func->funcid == g_cache.read_vortex_oid) {
				return true;
			}
		}
	}
#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, ContainsReadVortex, context);
#else
	return expression_tree_walker(node, (bool (*)())((void *)ContainsReadVortex), context);
#endif
}

static PlannedStmt *
#if PG_VERSION_NUM >= 190000
VortexPlannerHook_Cpp(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params,
                      ExplainState *es) {
#else
VortexPlannerHook_Cpp(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
#endif
	RefreshVortexCache();

	if (OidIsValid(g_cache.read_vortex_oid) && ContainsReadVortex((Node *)parse, nullptr)) {
		return pg_vortex::PlanNode(parse, cursor_options, /*throw_error=*/true);
	}

#if PG_VERSION_NUM >= 190000
	if (prev_planner_hook) {
		return prev_planner_hook(parse, query_string, cursor_options, bound_params, es);
	}
	return standard_planner(parse, query_string, cursor_options, bound_params, es);
#else
	if (prev_planner_hook) {
		return prev_planner_hook(parse, query_string, cursor_options, bound_params);
	}
	return standard_planner(parse, query_string, cursor_options, bound_params);
#endif
}

static PlannedStmt *
#if PG_VERSION_NUM >= 190000
VortexPlannerHook(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params,
                  ExplainState *es) {
	return InvokeCPPFunc(VortexPlannerHook_Cpp, parse, query_string, cursor_options, bound_params, es);
}
#else
VortexPlannerHook(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
	return InvokeCPPFunc(VortexPlannerHook_Cpp, parse, query_string, cursor_options, bound_params);
}
#endif

void
InitHooks() {
	prev_planner_hook = planner_hook;
	planner_hook = VortexPlannerHook;

	// CREATE/DROP EXTENSION touches pg_namespace; there is no dedicated
	// pg_extension syscache, so NAMESPACENAME is the cheapest invalidation signal.
	CacheRegisterSyscacheCallback(NAMESPACENAME, InvalidateVortexCache, (Datum)0);
}

} // namespace pg_vortex
