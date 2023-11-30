// fms_duckdb.h - DuckDB database
// https://duckdb.org/docs/archive/0.9.2/api/c/overview
#define DUCKDB_VERSION "0.9.2"
#pragma once
#include <stdexcept>
#include "duckdb-0.9.2/duckdb.h"

#ifndef CATEGORY
#define CATEGORY "DDB"
#endif // _DEBUG

namespace fms::duckdb {

	struct string : public duckdb_string {
		string(duckdb_string str)
		{
			*static_cast<duckdb_string*>(this) = str;
		}
		string(const string&) = delete;
		string& operator=(const string&) = delete;
		~string()
		{
			duckdb_free(this);
		}
	};

	// https://duckdb.org/docs/archive/0.9.2/api/c/api#result-functions
	class result {
		duckdb_result res;
	public:
		result()
			: res{ .internal_data = nullptr }
		{ }
		result(const result&) = delete;
		result& operator=(const result&) = delete;
		~result()
		{
			if (res.internal_data) {
				duckdb_destroy_result(&res);
			}
		}

		operator duckdb_result()
		{
			return res;
		}
		duckdb_result* operator&()
		{
			return &res;
		}

		const char* column_name(idx_t col)
		{
			return duckdb_column_name(&res, col);
		}
		idx_t column_count()
		{
			return duckdb_column_count(&res);
		}
		idx_t row_count()
		{
			return duckdb_row_count(&res);
		}

// https://duckdb.org/docs/archive/0.9.2/api/c/api#value-functions
#define DUCKDB_VALUE_IMPL(a,b) a value_##b(idx_t row, idx_t col) { return duckdb_value_##b(&res, row, col); }
#define DUCKDB_VALUE(X) \
	X(bool, boolean) \
	X(int8_t, int8) \
	X(uint8_t, uint8) \
	X(int16_t, int16) \
	X(uint16_t, uint16) \
	X(int32_t, int32) \
	X(uint32_t, uint32) \
	X(int64_t, int64) \
	X(uint64_t, uint64) \
	X(duckdb_hugeint, hugeint) \
	X(duckdb_decimal, decimal) \
	X(float, float) \
	X(double, double) \
	X(char*, varchar) \
	X(duckdb_blob, blob) \
	X(string, string) \

#if 0
	X(string, date) \
	X(string, time) \
	X(string, timestamp) \
	X(string, interval) \
	X(string, hugeint) \
	X(string, pointer) \
	X(string, fixedsizebinary) \
	X(string, list) \
	X(string, map) \
	X(string, struct) \
	X(string, union) \
	X(string, json) \
	X(string, intervaldaytime) \
	X(string, intervalyearmonth) \
	X(string, time32) \
	X(string, time64) \
	X(string, timestamp_seconds) \
	X(string, timestamp_milliseconds) \
	X(string, timestamp_microseconds) \
	X(string, timestamp_nanoseconds) \
	X(string, timestamp_zoned_seconds) \
	X(string, timestamp_zoned_milliseconds) \
	X(string, timestamp_zoned_microseconds) \
	X(string, timestamp_zoned_nanoseconds) \
	X(string, date_zoned) \
	X(string, time_zoned) \
	X(string, interval_zoned) \
	X(string, list_element) \
	X(string, nested) \
	X(string, dictionary) \
	X(string, fixedsizelist) \
	X(string, extension) \
	X(string, chunk_collection) \
	X(string, invalid) \

#endif // 0

DUCKDB_VALUE(DUCKDB_VALUE_IMPL)
#undef DUCKDB_VALUE_IMPL
#undef DUCKDB_VALUE
		
	};

	class database {
		duckdb_database db;
	public:
		database(const char* path = nullptr) 
			: db{ nullptr }
		{
			duckdb_state state = duckdb_open(path, &db);
			if (DuckDBSuccess != state) {
				throw std::runtime_error(__FUNCTION__);
			}
		}
		database(const database&) = delete;
		database& operator=(const database&) = delete;
		~database()
		{
			duckdb_close(&db);
		}

		operator duckdb_database()
		{
			return db;
		}

		class connection {
			duckdb_connection con;
		public:
			connection(duckdb_database db)
			{
				duckdb_state state = duckdb_connect(db, &con);
				if (DuckDBSuccess != state) {
					throw std::runtime_error(__FUNCTION__);
				}
			}
			connection(const connection&) = delete;
			connection& operator=(const connection&) = delete;
			~connection()
			{
				duckdb_disconnect(&con);
			}

			operator duckdb_connection()
			{
				return con;
			}

			class query {
				result res;
			public:
				query(duckdb_connection con, const char* sql)
				{
					duckdb_state state = duckdb_query(con, sql, &res);
					if (DuckDBSuccess != state) {
						throw std::runtime_error(__FUNCTION__);
					}
				}
			};
		};
	};

} // namespace fms::duckdb
