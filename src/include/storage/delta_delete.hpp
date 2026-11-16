//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class DeltaTableEntry;

//! DELETE with deletion vectors. The operator collects the position of every deleted row within its data file. It
//! then builds one deletion vector file with a deletion vector for each affected data file, including the rows that
//! file's current deletion vector already deletes, and stages it on the transaction. On commit, kernel writes a
//! remove and an add action for each affected file.
class DeltaDelete : public PhysicalOperator {
public:
	DeltaDelete(PhysicalPlan &physical_plan, DeltaTableEntry &table, PhysicalOperator &child, idx_t filename_index,
	            idx_t row_number_index);

	//! The table to delete from
	DeltaTableEntry &table;
	//! The positions of the `filename` and `file_row_number` row id columns in the child's chunks
	idx_t filename_index;
	idx_t row_number_index;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

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
};

} // namespace duckdb
