// xll_duckdb.cpp - DuckDB Excel Add-in
#include "xll_duckdb.h"

using namespace xll;

AddIn xai_duckdb(
	Function(XLL_DOUBLE, L"xll_duckdb_open", CATEGORY L".OPEN")
	.Arguments({
		Arg(XLL_CSTRING4, L"path", L"is the path to a database."),
	})
	.Category(CATEGORY)
	.FunctionHelp(L"Return the number of ducks.")
);
double WINAPI xll_duckdb_open(const char* path)
{
#pragma XLLEXPORT
	double result = std::numeric_limits<double>::quiet_NaN();

	try {
		fms::duckdb::database db(path);
	}
	catch (const std::exception& ex) {
		XLL_ERROR(ex.what());
	}

	return result;
}