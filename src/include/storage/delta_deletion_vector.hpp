//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_deletion_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class FileSystem;

//! The fields of a deletion vector's descriptor in the log, for one deletion vector in a deletion vector file
struct DeltaDeletionVectorDescriptor {
	//! Storage type 'u', `KernelDvStorageType::PersistedRelative`: a file in the table root named after a UUID.
	//! Kernel's FFI takes the storage type as an int and its generated header does not declare the enum.
	static constexpr int STORAGE_TYPE_PERSISTED_RELATIVE = 0;

	//! `<z85 uuid>`: the file's name relative to the table root, storage type 'u'
	string path_or_inline_dv;
	//! Byte offset of the deletion vector's size prefix in the file
	int32_t offset;
	//! Size of the deletion vector: its magic number and bitmap array, without the size prefix and checksum
	int32_t size_in_bytes;
	//! The number of rows it deletes
	int64_t cardinality;
};

//! A deletion vector file as PROTOCOL.md's "Deletion Vector Format" lays it out: a version byte, then per deletion
//! vector its size, a magic number, the deleted row indexes as a RoaringBitmapArray, and a CRC-32 of the last two.
//! The rows are serialized the way the reference writers' `RoaringTreemap` serializes them, without run containers.
class DeltaDeletionVectorFile {
public:
	//! A new file in the root of the table at `table_path`, named after a random UUID
	explicit DeltaDeletionVectorFile(const string &table_path);

	//! Appends a deletion vector of `deleted_rows`, which must be sorted and unique
	DeltaDeletionVectorDescriptor Add(const vector<idx_t> &deleted_rows);
	//! Writes the file. Its name comes from a random UUID, so no other file has it.
	void Write(FileSystem &fs) const;

	const string &GetPath() const {
		return path;
	}

private:
	void WriteBitmapArray(const vector<idx_t> &deleted_rows);
	void WriteBitmap(const vector<idx_t> &deleted_rows, idx_t begin, idx_t end);
	void WriteLittleEndian(uint64_t value, idx_t bytes);
	void WriteBigEndian(uint32_t value);
	static string EncodeZ85(const data_t *input, idx_t size);

private:
	string path;
	//! The file's name as a 'u' descriptor encodes it: the UUID in Z85
	string encoded_uuid;
	vector<data_t> buffer;
};

} // namespace duckdb
