//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class DeltaTableEntry;
class DeltaMultiFileList;
class PhysicalTableScan;

//! Per-thread accumulation of the positions to delete, grouped by the data file they belong to.
class DeltaDeleteLocalState : public LocalSinkState {
public:
	//! filename (as emitted by the scan's `filename` virtual column) -> physical row numbers to delete
	unordered_map<string, set<idx_t>> deleted_rows;
};

class DeltaDeleteGlobalState : public GlobalSinkState {
public:
	DeltaDeleteGlobalState() : total_deleted_count(0) {
	}

	mutex lock;
	//! filename -> the physical row numbers deleted from that file
	unordered_map<string, set<idx_t>> deleted_rows;
	atomic<idx_t> total_deleted_count;

	void Merge(DeltaDeleteLocalState &local_state) {
		lock_guard<mutex> guard(lock);
		for (auto &entry : local_state.deleted_rows) {
			auto &global_entry = deleted_rows[entry.first];
			global_entry.insert(entry.second.begin(), entry.second.end());
		}
		local_state.deleted_rows.clear();
	}
};

//! Copy-on-write DELETE for Delta tables: collects the (file, row) pairs to delete, then rewrites the
//! surviving rows of each affected file into new data files and stages Add + Remove actions on the
//! DeltaTransaction so they commit atomically.
class DeltaDelete : public PhysicalOperator {
public:
	DeltaDelete(PhysicalPlan &physical_plan, DeltaTableEntry &table, optional_ptr<DeltaMultiFileList> multi_file_list,
	            PhysicalOperator &child, vector<idx_t> row_id_indexes);

	//! The table to delete from
	DeltaTableEntry &table;
	//! The file list of the delta scan feeding this delete (may be null if the scan was optimized away)
	optional_ptr<DeltaMultiFileList> multi_file_list;
	//! Indexes, within the incoming chunk, of the {filename, file_row_number} row-id columns
	vector<idx_t> row_id_indexes;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, DeltaTableEntry &table,
	                                    PhysicalOperator &child_plan, vector<idx_t> &&row_id_indexes);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	//! Walk `plan` for the delta_scan that emits the {filename, file_row_number} row-id virtual columns.
	static optional_ptr<PhysicalTableScan> FindDeltaScan(PhysicalOperator &plan);
};

} // namespace duckdb
