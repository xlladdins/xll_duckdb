// fms_duckdb.h - DuckDB database
#pragma once
#include <stdexcept>
#include "duckdb-0.9.2/duckdb.h"

// https://duckdb.org/docs/archive/0.9.2/api/c/overview
#ifndef CATEGORY
#define CATEGORY "DDB"
#endif // _DEBUG

namespace fms::duckdb {

	struct string : public duckdb_string {
		~string()
		{
			duckdb_free(this);
		}
	};

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

		idx_t row_count()
		{
			return duckdb_row_count(&res);
		}
		idx_t column_count()
		{
			return duckdb_column_count(&res);
		}
		const char* column_name(idx_t col)
		{
			return duckdb_column_name(&res, col);
		}
		duckdb_string value_string(idx_t row, idx_t col)
		{
			return duckdb_value_string(&res, row, col);
		}
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
			}
		};
	};

} // namespace fms::duckdb
