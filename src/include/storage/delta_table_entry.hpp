//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <duckdb/parser/constraints/not_null_constraint.hpp>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
class DeltaMultiFileList;
class NestedNotNullConstraint;

class DeltaTableEntry : public TableCatalogEntry {
public:
	DeltaTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	~DeltaTableEntry();

public:
	const ColumnList &GetColumns() const override;

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	//! Rows are identified by their data file and their position in that file, which is what a deletion vector records
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

	case_insensitive_map_t<vector<NestedNotNullConstraint>> GetNotNullConstraints() const;
	void ThrowOnUnsupportedFieldForInserting() const;
	//! Refuses a DELETE on a table without deletion vectors, before any row is read. Kernel refuses the table features
	//! a DELETE must respect, such as append-only tables, when the transaction commits.
	void ThrowOnUnsupportedDelete() const;

public:
	shared_ptr<DeltaMultiFileList> snapshot;

protected:
	ColumnList columns;

	TableFunction GetScanFunctionInternal(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                      optional_ptr<const EntryLookupInfo> lookup_info);
};

} // namespace duckdb
