#include "storage/delta_delete.hpp"

#include "functions/delta_scan/delta_multi_file_list.hpp"
#include "storage/delta_catalog.hpp"
#include "storage/delta_deletion_vector.hpp"
#include "storage/delta_table_entry.hpp"
#include "storage/delta_transaction.hpp"

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"

#include <algorithm>

namespace duckdb {

//! The rows a DELETE removes, by the data file the scan read them from
using DeletedRows = unordered_map<string, vector<idx_t>>;

class DeltaDeleteLocalState : public LocalSinkState {
public:
	DeletedRows deleted_rows;
};

class DeltaDeleteGlobalState : public GlobalSinkState {
public:
	mutex lock;
	DeletedRows deleted_rows;
	idx_t deleted_count = 0;
};

DeltaDelete::DeltaDelete(PhysicalPlan &physical_plan, DeltaTableEntry &table, PhysicalOperator &child,
                         idx_t filename_index, idx_t row_number_index)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      filename_index(filename_index), row_number_index(row_number_index) {
	children.push_back(child);
}

unique_ptr<GlobalSinkState> DeltaDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DeltaDeleteGlobalState>();
}

unique_ptr<LocalSinkState> DeltaDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<DeltaDeleteLocalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType DeltaDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &local_state = input.local_state.Cast<DeltaDeleteLocalState>();

	UnifiedVectorFormat file_name_format;
	chunk.data[filename_index].ToUnifiedFormat(file_name_format);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_format);
	UnifiedVectorFormat row_number_format;
	chunk.data[row_number_index].ToUnifiedFormat(row_number_format);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_format);

	for (idx_t i = 0; i < chunk.size(); i++) {
		auto name_idx = file_name_format.sel->get_index(i);
		auto row_idx = row_number_format.sel->get_index(i);
		if (!file_name_format.validity.RowIsValid(name_idx) || !row_number_format.validity.RowIsValid(row_idx)) {
			throw InternalException("Delta DELETE: a row has no data file or position");
		}
		local_state.deleted_rows[file_names[name_idx].GetString()].push_back(NumericCast<idx_t>(row_numbers[row_idx]));
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType DeltaDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<DeltaDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<DeltaDeleteLocalState>();
	lock_guard<mutex> guard(global_state.lock);
	for (auto &entry : local_state.deleted_rows) {
		auto &rows = global_state.deleted_rows[entry.first];
		rows.insert(rows.end(), entry.second.begin(), entry.second.end());
	}
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType DeltaDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DeltaDeleteGlobalState>();
	if (global_state.deleted_rows.empty()) {
		return SinkFinalizeType::READY;
	}

	unordered_set<string> paths;
	for (auto &entry : global_state.deleted_rows) {
		paths.insert(entry.first);
	}
	auto logged_files = table.snapshot->GetLoggedDataFiles(paths);

	// The new deletion vector replaces the file's current one, so it must also contain the rows that one deletes
	DeltaDeletionVectorFile dv_file(table.snapshot->GetPath());
	vector<pair<string, DeltaDeletionVectorDescriptor>> deletion_vectors;
	for (auto &entry : global_state.deleted_rows) {
		auto &rows = entry.second;
		// A DELETE ... USING can match a row more than once
		std::sort(rows.begin(), rows.end());
		rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
		global_state.deleted_count += rows.size();

		auto &logged_file = logged_files[entry.first];
		vector<idx_t> all_deleted_rows;
		all_deleted_rows.reserve(logged_file.deleted_rows.size() + rows.size());
		std::set_union(logged_file.deleted_rows.begin(), logged_file.deleted_rows.end(), rows.begin(), rows.end(),
		               std::back_inserter(all_deleted_rows));
		deletion_vectors.emplace_back(logged_file.add_path, dv_file.Add(all_deleted_rows));
	}

	DeltaTransaction::Get(context, table.catalog).UpdateDeletionVectors(context, dv_file, deletion_vectors);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType DeltaDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<DeltaDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(global_state.deleted_count)));
	return SourceResultType::FINISHED;
}

string DeltaDelete::GetName() const {
	return "DELTA_DELETE";
}

InsertionOrderPreservingMap<string> DeltaDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name.GetIdentifierName();
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
PhysicalOperator &DeltaCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion from a Delta table");
	}
	// As for INSERT: a pinned attach shares the schema's table entry, which no transaction owns
	if (UseCachedSnapshot() && access_mode != AccessMode::READ_ONLY) {
		throw NotImplementedException("DELETE from a Delta table attached with PIN_SNAPSHOT is not supported: "
		                              "attach it without PIN_SNAPSHOT to write to it");
	}
	// A table attached through another catalog, such as Unity Catalog, commits through that catalog's table entry.
	// The DELETE only knows the Delta table entry, because that is the one the scan reports.
	if (parent_commit || child_catalog_mode) {
		throw NotImplementedException(
		    "DELETE from a Delta table attached through another catalog is not supported yet");
	}
	auto &table = op.table.Cast<DeltaTableEntry>();
	table.ThrowOnUnsupportedDelete();

	// The row id columns from GetRowIdColumns, `filename` and `file_row_number`, are the first two expressions
	auto filename_index = op.expressions[0]->Cast<BoundReferenceExpression>().Index();
	auto row_number_index = op.expressions[1]->Cast<BoundReferenceExpression>().Index();
	return planner.Make<DeltaDelete>(table, plan, filename_index, row_number_index);
}

} // namespace duckdb
