// xll_duckdb.cpp - DuckDB Excel Add-in
#include "xll_duckdb.h"

using namespace fms;
using namespace xll;

AddIn xai_duckdb(
	Function(XLL_HANDLEX, L"xll_duckdb_open", CATEGORY L".OPEN")
	.Arguments({
		Arg(XLL_CSTRING4, L"path", L"is the path to a database."),
	})
	.Category(CATEGORY)
	.FunctionHelp(L"Return the number of ducks.")
);
HANDLEX WINAPI xll_duckdb_open(const char* path)
{
#pragma XLLEXPORT
	double result = INVALID_HANDLEX;

	try {
		handle<duckdb::database> h(new duckdb::database(path));
		ensure(h);
		result = h.get();
	}
	catch (const std::exception& ex) {
		XLL_ERROR(ex.what());
	}

	return result;
}