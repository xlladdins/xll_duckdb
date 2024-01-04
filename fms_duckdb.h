// fms_duckdb.h - DuckDB database
// https://duckdb.org/docs/archive/0.9.2/api/c/overview
#pragma once
#include <stdexcept>
#include "duckdb-0.9.2/duckdb.h"

#ifndef CATEGORY
#define CATEGORY "DDB"
#endif // _DEBUG

namespace fms::duckdb {

// DUCKDB_TYPE_XXX, C type, description
#define DUCKDB_TYPE_(X) \
	X(BOOLEAN, bool, "") \
	X(TINYINT, int8_t, "") \
	X(SMALLINT, int16_t, "") \
	X(INTEGER, int32_t, "") \
	X(BIGINT, int64_t, "") \
	X(UTINYINT, uint8_t, "") \
	X(USMALLINT, uint16_t, "") \
	X(UINTEGER, uint32_t, "") \
	X(UBIGINT, uint64_t, "") \
	X(FLOAT, float, "") \
	X(DOUBLE, double, "") \
	X(TIMESTAMP, duckdb_timestamp, " in microseconds") \
	X(DATE, duckdb_date, "") \
	X(TIME, duckdb_time, "") \
	X(INTERVAL, duckdb_interval, "") \
	X(HUGEINT, duckdb_hugeint, "") \
	X(VARCHAR, const char*, "") \
	X(BLOB, duckdb_blob, "") \
	X(DECIMAL, decimal, "") \
	X(TIMESTAMP_S, duckdb_timestamp, " in seconds") \
	X(TIMESTAMP_MS, duckdb_timestamp, " in milliseconds") \
	X(TIMESTAMP_NS, duckdb_timestamp, " in nanoseconds") \
	X(UUID, duckdb_hugeint, "") \
	X(ENUM, enum type, " only useful as logical type") \
	X(BIT, duckdb_bit, "") \

	//X(LIST, list type, " only useful as logical type") \
	//X(STRUCT, struct type, " only useful as logical type") \
	//X(MAP, map type, " only useful as logical type") \
	//X(UNION, union type, " only useful as logical type") \

	// https://duckdb.org/docs/archive/0.9.2/api/c/appender
	class appender {
		duckdb_appender app;
	public:
		appender(duckdb_connection con, const char* schema, const char* table)
		{
			duckdb_state state = duckdb_appender_create(con, schema, table, &app);
			if (DuckDBSuccess != state) {
				throw std::runtime_error(duckdb_appender_error(app));
			}
		}
		// default schema
		appender(duckdb_connection con, const char* table)
			: appender(con, nullptr, table)
		{ }
		appender(const appender&) = delete;
		appender& operator=(const appender&) = delete;
		~appender()
		{
			duckdb_appender_destroy(&app);
		}

		// finalize appends
		duckdb_state end_row() const
		{
			return duckdb_appender_end_row(app);
		}

		duckdb_state append(bool b) const
		{
			return duckdb_append_bool(app, b);
		}
		duckdb_state append(int8_t i) const
		{
			return duckdb_append_int8(app, i);
		}
		duckdb_state append(int16_t i) const
		{
			return duckdb_append_int16(app, i);
		}
		duckdb_state append(int32_t i) const
		{
			return duckdb_append_int32(app, i);
		}
		duckdb_state append(int64_t i) const
		{
			return duckdb_append_int64(app, i);
		}
		duckdb_state append(duckdb_hugeint i) const
		{
			return duckdb_append_hugeint(app, i);
		}
		duckdb_state append(uint8_t i) const
		{
			return duckdb_append_uint8(app, i);
		}
		duckdb_state append(uint16_t i) const
		{
			return duckdb_append_uint16(app, i);
		}
		duckdb_state append(uint32_t i) const
		{
			return duckdb_append_uint32(app, i);
		}
		duckdb_state append(uint64_t i) const
		{
			return duckdb_append_uint64(app, i);
		}
		duckdb_state append(float x) const
		{
			return duckdb_append_float(app, x);
		}
		duckdb_state append(double x) const
		{
			return duckdb_append_double(app, x);
		}
		duckdb_state append(duckdb_date d) const
		{
			return duckdb_append_date(app, d);
		}
		duckdb_state append(duckdb_time t) const
		{
			return duckdb_append_time(app, t);
		}
		duckdb_state append(duckdb_timestamp t) const
		{
			return duckdb_append_timestamp(app, t);
		}
		duckdb_state append(duckdb_interval t) const
		{
			return duckdb_append_interval(app, t);
		}
		duckdb_state append(const char* t) const
		{
			return duckdb_append_varchar(app, t);
		}
		duckdb_state append(const char* t, idx_t n) const
		{
			return duckdb_append_varchar_length(app, t, n);
		}
		duckdb_state append(const void* t, idx_t n) const
		{
			return duckdb_append_blob(app, t, n);
		}
		duckdb_state append() const
		{
			return duckdb_append_null(app);
		}
	};

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

	struct date : public duckdb_date {
		date(duckdb_date d)
		{
			*static_cast<duckdb_date*>(this) = d;
		}
		date(duckdb_date_struct d)
		{
			*static_cast<duckdb_date*>(this) = duckdb_to_date(d);
		}
		duckdb_date_struct from_date() const
		{
			return duckdb_from_date(*this);
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

		duckdb_type column_type(idx_t col)
		{
			return duckdb_column_type(&res, col);
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
	X(string, string_internal) \
	X(date, date) \

		//X(time, time) \

		//X(timestamp, timestamp) \

#if 0
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
#define DUCKDB_VALUE_IMPL(a,b) a value_##b(idx_t row, idx_t col) { return duckdb_value_##b(&res, row, col); }
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
