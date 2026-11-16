//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/delta_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_multi_file_list.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

enum class DeltaFilterPushdownMode : uint8_t {
	NONE = 0,
	ALL = 1,
	CONSTANT_ONLY = 2,
	DYNAMIC_ONLY = 3,
};

static constexpr DeltaFilterPushdownMode DEFAULT_PUSHDOWN_MODE = DeltaFilterPushdownMode::ALL;

struct DeltaEnumUtils {
	static DeltaFilterPushdownMode FromString(const string &str);
	static string ToString(const DeltaFilterPushdownMode &mode);
};

struct DeltaFunctionInfo : public TableFunctionInfo {
	shared_ptr<DeltaMultiFileList> snapshot;
	string expected_path;
	string table_name;
	//! The catalog table this scan reads, which DELETE resolves its target from. Null for a delta_scan('path').
	optional_ptr<TableCatalogEntry> table_entry;
};

} // namespace duckdb
