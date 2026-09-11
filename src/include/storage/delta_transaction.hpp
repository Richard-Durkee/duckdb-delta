//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_utils.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class DeltaCatalog;
class DeltaSchemaEntry;
class DeltaTableEntry;
class DeltaMultiFileList;
class TableFunctionCatalogEntry;
struct DeltaDataFile;
struct DeltaMultiFileColumnDefinition;

enum class DeltaTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class DeltaTransaction : public Transaction {
public:
	DeltaTransaction(DeltaCatalog &delta_catalog, TransactionManager &manager, ClientContext &context);
	~DeltaTransaction() override;

	void Start();
	void Commit(ClientContext &context);
	void Rollback();

	void Append(ClientContext &context, const vector<DeltaDataFile> &append_files);

	//! Stage Remove actions for the given data files (paths as the kernel records them, i.e. relative to
	//! the table root). Drives the kernel scan to recover the scan-metadata engine data, selects the rows
	//! matching `files_to_remove`, and applies them to the transaction via `ffi::remove_files`. `operation`
	//! is the history operation name recorded in the commit (e.g. "DELETE", "UPDATE", "MERGE").
	void RemoveFiles(ClientContext &context, const unordered_set<string> &files_to_remove, const string &operation);

	void SetTransactionVersion(const string &app_id, idx_t new_version, Value expected_value);

	static DeltaTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const;

	bool HasOutstandingAppends() const;
	//! True when this transaction has staged any uncommitted write (append or remove)
	bool HasOutstandingWrites() const;

	optional_ptr<DeltaTableEntry> GetTableEntry(idx_t version);

	DeltaTableEntry &InitializeTableEntry(ClientContext &context, DeltaSchemaEntry &schema_entry, idx_t version,
	                                      optional_ptr<const DeltaMultiFileList> old_snapshot);
	vector<DeltaMultiFileColumnDefinition> GetWriteSchema(ClientContext &context);

	//! Removes all outstanding appends and removes the files if possible
	void CleanUpFiles();

	//! CGetCommits callback for Unity Catalog managed commits
	//! CCommit callback for Unity Catalog managed commits - returns None on success, Some(error) on failure
	static ffi::OptionalValue<ffi::Handle<ffi::ExclusiveRustString>> CommitCallback(ffi::NullableCvoid context,
	                                                                                ffi::CommitRequest request);

	void SetParentTableEntry(TableCatalogEntry &entry) {
		lock_guard<mutex> guard(lock);
		parent_table_entry = &entry;
	}

protected:
	void InitializeTransaction(ClientContext &context);
	//! Records the operation name (once per transaction) and marks the transaction as changing data.
	//! `ffi::with_operation` consumes and returns the transaction handle, so this must run after
	//! InitializeTransaction and is guarded to run at most once.
	void SetOperationOnce(const string &operation);

private:
	mutable mutex lock;

	//! Cached table entry (without a specified version)
	//! Note: this should be the latest version of the table, pinned at the version of first reading it during this
	//! transaction
	unique_ptr<DeltaTableEntry> table_entry;

	//! Cached table entries at specific versions
	unordered_map<idx_t, unique_ptr<DeltaTableEntry>> versioned_table_entries;

	//	DeltaConnection connection;
	DeltaTransactionState transaction_state;

	const AccessMode access_mode;

	vector<DeltaDataFile> outstanding_appends;

	//! Number of Remove actions staged on the kernel transaction this session. Removes are applied to the
	//! kernel transaction eagerly (like appends), so this only needs to gate commit and the uncommitted-write
	//! check; the file list itself lives in the kernel transaction.
	idx_t outstanding_remove_count = 0;

	//! Whether `ffi::with_operation` has already been called on this transaction (it consumes the handle)
	bool operation_set = false;

	KernelExclusiveTransaction kernel_transaction;

	//! stores a ptr to the table entry that this transaction is writing to
	optional_ptr<DeltaTableEntry> write_entry;

	// Versions registered to this transaction
	struct TransactionVersion {
		idx_t new_version;
		Value expected_version;
	};
	unordered_map<string, TransactionVersion> app_versions;

	//! Whether we should invoke our parent catalog to do the commit or this catalog can do the commit itself
	bool parent_commit = false;
	string parent_catalog_name;
	// string parent_catalog_schema;
	optional_ptr<TableFunctionCatalogEntry> commit_function;
	string unity_table_id;
	weak_ptr<ClientContext> current_context;
	optional_ptr<TableCatalogEntry> parent_table_entry;

	ErrorData active_error;
};

} // namespace duckdb
