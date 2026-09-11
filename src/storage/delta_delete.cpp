#include "storage/delta_delete.hpp"

#include "storage/delta_catalog.hpp"
#include "storage/delta_table_entry.hpp"
#include "storage/delta_transaction.hpp"
#include "storage/delta_insert.hpp"
#include "functions/delta_scan/delta_multi_file_list.hpp"

#include "duckdb/common/path.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

DeltaDelete::DeltaDelete(PhysicalPlan &physical_plan, DeltaTableEntry &table,
                         optional_ptr<DeltaMultiFileList> multi_file_list, PhysicalOperator &child,
                         vector<idx_t> row_id_indexes_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      multi_file_list(multi_file_list), row_id_indexes(std::move(row_id_indexes_p)) {
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

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_row_number_vector = chunk.data[row_id_indexes[1]];

	UnifiedVectorFormat file_name_format;
	file_name_vector.ToUnifiedFormat(file_name_format);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_format);

	UnifiedVectorFormat row_number_format;
	file_row_number_vector.ToUnifiedFormat(row_number_format);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_format);

	for (idx_t i = 0; i < chunk.size(); i++) {
		auto name_idx = file_name_format.sel->get_index(i);
		auto row_idx = row_number_format.sel->get_index(i);
		if (!file_name_format.validity.RowIsValid(name_idx)) {
			throw InternalException("DeltaDelete: filename row-id column cannot be NULL");
		}
		auto file_name = file_names[name_idx].GetString();
		auto row_number = NumericCast<idx_t>(row_numbers[row_idx]);
		local_state.deleted_rows[file_name].insert(row_number);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType DeltaDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<DeltaDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<DeltaDeleteLocalState>();
	global_state.Merge(local_state);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
//! Wrap `input` as a single-quoted SQL string literal (doubling embedded quotes).
static string SQLString(const string &input) {
	string result = "'";
	for (auto c : input) {
		if (c == '\'') {
			result += '\'';
		}
		result += c;
	}
	result += "'";
	return result;
}

SinkFinalizeType DeltaDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DeltaDeleteGlobalState>();
	lock_guard<mutex> guard(global_state.lock);
	if (global_state.deleted_rows.empty()) {
		return SinkFinalizeType::READY;
	}

	auto &transaction = DeltaTransaction::Get(context, table.catalog);
	auto table_path = table.snapshot->GetPath();
	auto &fs = FileSystem::GetFileSystem(context);

	vector<DeltaDataFile> new_files;
	unordered_set<string> files_to_remove;
	idx_t total_deleted = 0;

	// Copy-on-write: for each affected file, rewrite the rows that survive the delete into a new data
	// file (read back through delta_scan so column values match the logical schema) and stage it as an
	// Add; the original file is staged as a Remove. Both are committed together by the DeltaTransaction.
	// Every survivor file we write is tracked so it can be cleaned up if any step fails before commit,
	// rather than leaving orphaned parquet files in the table directory.
	vector<string> written_files;
	Connection con(*context.db);
	try {
		for (auto &entry : global_state.deleted_rows) {
			auto &file_name = entry.first;
			auto &positions = entry.second;
			files_to_remove.insert(file_name);
			total_deleted += positions.size();

			// Build the NOT IN list of physical row numbers to drop from this file.
			string pos_list;
			for (auto pos : positions) {
				if (!pos_list.empty()) {
					pos_list += ",";
				}
				pos_list += to_string(pos);
			}

			auto new_file = Path::FromString(table_path)
			                    .Join("duckdb-" + UUID::ToString(UUID::GenerateRandomUUID()) + ".parquet")
			                    .ToString();
			written_files.push_back(new_file);

			string sql = "COPY (SELECT * FROM delta_scan(" + SQLString(table_path) + ") WHERE filename = " +
			             SQLString(file_name) + " AND file_row_number NOT IN (" + pos_list + ")) TO " +
			             SQLString(new_file) + " (FORMAT PARQUET)";

			auto result = con.Query(sql);
			if (result->HasError()) {
				result->ThrowError();
			}
			auto survivor_count = result->GetValue(0, 0).GetValue<idx_t>();

			if (survivor_count == 0) {
				// Every row of this file was deleted: no survivors to write, just remove the original. COPY
				// still wrote an empty file, so clean it up.
				fs.TryRemoveFile(new_file);
				continue;
			}

			DeltaDataFile data_file;
			data_file.file_name = new_file;
			data_file.row_count = survivor_count;
			data_file.footer_size = 0;
			{
				auto handle = fs.OpenFile(new_file, FileOpenFlags::FILE_FLAGS_READ);
				data_file.file_size_bytes = NumericCast<idx_t>(fs.GetFileSize(*handle));
			}
			new_files.push_back(std::move(data_file));
		}

		if (!new_files.empty()) {
			transaction.Append(context, new_files);
		}
		transaction.RemoveFiles(context, files_to_remove, "DELETE");
	} catch (...) {
		// Roll back any survivor files written this pass so a failed DELETE leaves no orphans behind.
		for (auto &path : written_files) {
			fs.TryRemoveFile(path);
		}
		throw;
	}

	global_state.total_deleted_count = total_deleted;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType DeltaDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<DeltaDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count.load())));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
static bool ScanEmitsRowId(const PhysicalTableScan &scan) {
	if (scan.function.GetName() != "delta_scan") {
		return false;
	}
	bool has_file_name = false;
	bool has_file_row_number = false;
	for (auto &column : scan.column_ids) {
		if (!column.HasPrimaryIndex()) {
			continue;
		}
		auto index = column.GetPrimaryIndex();
		if (index == MultiFileReader::COLUMN_IDENTIFIER_FILENAME) {
			has_file_name = true;
		} else if (index == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			has_file_row_number = true;
		}
	}
	return has_file_name && has_file_row_number;
}

optional_ptr<PhysicalTableScan> DeltaDelete::FindDeltaScan(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &scan = plan.Cast<PhysicalTableScan>();
		return ScanEmitsRowId(scan) ? &scan : nullptr;
	}
	for (auto &child : plan.children) {
		auto result = FindDeltaScan(child.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

PhysicalOperator &DeltaDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, DeltaTableEntry &table,
                                          PhysicalOperator &child_plan, vector<idx_t> &&row_id_indexes) {
	auto scan = FindDeltaScan(child_plan);
	optional_ptr<DeltaMultiFileList> multi_file_list;
	if (scan) {
		auto &bind_data = scan->bind_data->Cast<MultiFileBindData>();
		multi_file_list = bind_data.file_list->Cast<DeltaMultiFileList>();
	}
	return planner.Make<DeltaDelete>(table, multi_file_list, child_plan, std::move(row_id_indexes));
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DeltaDelete::GetName() const {
	return "DELTA_DELETE";
}

InsertionOrderPreservingMap<string> DeltaDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name.GetIdentifierName();
	return result;
}

} // namespace duckdb
