#include "storage/delta_deletion_vector.hpp"

#include "functions/delta_scan/delta_multi_file_list.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "miniz.hpp"

namespace duckdb {

//! The magic number that opens every RoaringBitmapArray in a deletion vector file
static constexpr uint32_t DELETION_VECTOR_MAGIC = 1681511377;
//! The portable roaring format's cookie for a bitmap without run containers, which is all this writer produces
static constexpr uint32_t ROARING_SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
//! A container holding more values than this is stored as a bitmap instead of a sorted array
static constexpr idx_t ROARING_MAX_ARRAY_CARDINALITY = 4096;
static constexpr idx_t ROARING_BITMAP_WORDS = 1024;

DeltaDeletionVectorFile::DeltaDeletionVectorFile(const string &table_path) {
	// Not the client's random engine: setseed() would make two DELETEs pick the same file name
	auto uuid_value = UUID::GenerateRandomUUID();
	auto uuid = UUID::ToString(uuid_value);
	data_t uuid_bytes[16];
	UUID::ToBlob(uuid_value, uuid_bytes);
	encoded_uuid = EncodeZ85(uuid_bytes, sizeof(uuid_bytes));

	auto root = table_path;
	StringUtil::RTrim(root, "/");
	path = DeltaMultiFileList::ToDuckDBPath(root + "/deletion_vector_" + uuid + ".bin");

	// The format version
	buffer.push_back(1);
}

DeltaDeletionVectorDescriptor DeltaDeletionVectorFile::Add(const vector<idx_t> &deleted_rows) {
	D_ASSERT(!deleted_rows.empty());
	auto size_offset = buffer.size();
	// The size prefix, filled in once the bitmap array is written
	WriteBigEndian(0);
	auto data_offset = buffer.size();
	WriteLittleEndian(DELETION_VECTOR_MAGIC, 4);
	WriteBitmapArray(deleted_rows);
	auto data_size = buffer.size() - data_offset;
	auto checksum = duckdb_miniz::mz_crc32(MZ_CRC32_INIT, buffer.data() + data_offset, data_size);
	WriteBigEndian(static_cast<uint32_t>(checksum));
	Store<uint32_t>(BSwapIfLE(NumericCast<uint32_t>(data_size)), buffer.data() + size_offset);

	DeltaDeletionVectorDescriptor result;
	result.path_or_inline_dv = encoded_uuid;
	result.offset = NumericCast<int32_t>(size_offset);
	result.size_in_bytes = NumericCast<int32_t>(data_size);
	result.cardinality = NumericCast<int64_t>(deleted_rows.size());
	return result;
}

void DeltaDeletionVectorFile::Write(FileSystem &fs) const {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                    FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
	handle->Write(const_cast<data_t *>(buffer.data()), buffer.size());
	handle->Sync();
	handle->Close();
}

//! A `RoaringTreemap`: the number of 32-bit bitmaps, then each one after the high half of the rows it holds
void DeltaDeletionVectorFile::WriteBitmapArray(const vector<idx_t> &deleted_rows) {
	vector<idx_t> bitmap_starts;
	for (idx_t i = 0; i < deleted_rows.size(); i++) {
		if (i == 0 || (deleted_rows[i] >> 32) != (deleted_rows[i - 1] >> 32)) {
			bitmap_starts.push_back(i);
		}
	}
	WriteLittleEndian(bitmap_starts.size(), 8);
	for (idx_t b = 0; b < bitmap_starts.size(); b++) {
		auto begin = bitmap_starts[b];
		auto end = b + 1 < bitmap_starts.size() ? bitmap_starts[b + 1] : deleted_rows.size();
		WriteLittleEndian(deleted_rows[begin] >> 32, 4);
		WriteBitmap(deleted_rows, begin, end);
	}
}

//! A 32-bit roaring bitmap of the low halves of `deleted_rows[begin, end)`, in the portable format: a header naming
//! each container's high 16 bits and cardinality, the containers' offsets, then the containers themselves
void DeltaDeletionVectorFile::WriteBitmap(const vector<idx_t> &deleted_rows, idx_t begin, idx_t end) {
	vector<idx_t> container_starts;
	for (idx_t i = begin; i < end; i++) {
		if (i == begin || ((deleted_rows[i] >> 16) & 0xFFFF) != ((deleted_rows[i - 1] >> 16) & 0xFFFF)) {
			container_starts.push_back(i);
		}
	}
	auto container_count = container_starts.size();
	auto container_end = [&](idx_t c) {
		return c + 1 < container_count ? container_starts[c + 1] : end;
	};

	auto bitmap_start = buffer.size();
	WriteLittleEndian(ROARING_SERIAL_COOKIE_NO_RUNCONTAINER, 4);
	WriteLittleEndian(container_count, 4);
	for (idx_t c = 0; c < container_count; c++) {
		WriteLittleEndian((deleted_rows[container_starts[c]] >> 16) & 0xFFFF, 2);
		WriteLittleEndian(container_end(c) - container_starts[c] - 1, 2);
	}
	// Offsets from the start of this bitmap, which readers use to seek to a container
	auto offset = 8 + 8 * container_count;
	for (idx_t c = 0; c < container_count; c++) {
		WriteLittleEndian(offset, 4);
		auto cardinality = container_end(c) - container_starts[c];
		offset += cardinality <= ROARING_MAX_ARRAY_CARDINALITY ? 2 * cardinality : 8 * ROARING_BITMAP_WORDS;
	}
	for (idx_t c = 0; c < container_count; c++) {
		auto cardinality = container_end(c) - container_starts[c];
		if (cardinality <= ROARING_MAX_ARRAY_CARDINALITY) {
			for (idx_t i = container_starts[c]; i < container_end(c); i++) {
				WriteLittleEndian(deleted_rows[i] & 0xFFFF, 2);
			}
			continue;
		}
		uint64_t words[ROARING_BITMAP_WORDS] = {};
		for (idx_t i = container_starts[c]; i < container_end(c); i++) {
			auto low = deleted_rows[i] & 0xFFFF;
			words[low / 64] |= uint64_t(1) << (low % 64);
		}
		for (auto word : words) {
			WriteLittleEndian(word, 8);
		}
	}
	D_ASSERT(buffer.size() - bitmap_start == offset);
	(void)bitmap_start;
}

void DeltaDeletionVectorFile::WriteLittleEndian(uint64_t value, idx_t bytes) {
	for (idx_t i = 0; i < bytes; i++) {
		buffer.push_back(static_cast<data_t>(value >> (8 * i)));
	}
}

void DeltaDeletionVectorFile::WriteBigEndian(uint32_t value) {
	buffer.resize(buffer.size() + 4);
	Store<uint32_t>(BSwapIfLE(value), buffer.data() + buffer.size() - 4);
}

//! ZeroMQ's Z85 (rfc.zeromq.org/spec/32): every four bytes, read big-endian, become five base-85 digits
string DeltaDeletionVectorFile::EncodeZ85(const data_t *input, idx_t size) {
	static constexpr const char *ALPHABET =
	    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
	D_ASSERT(size % 4 == 0);
	string result;
	for (idx_t i = 0; i < size; i += 4) {
		uint32_t value = (uint32_t(input[i]) << 24) | (uint32_t(input[i + 1]) << 16) | (uint32_t(input[i + 2]) << 8) |
		                 uint32_t(input[i + 3]);
		char digits[5];
		for (idx_t d = 5; d > 0; d--) {
			digits[d - 1] = ALPHABET[value % 85];
			value /= 85;
		}
		result.append(digits, 5);
	}
	return result;
}

} // namespace duckdb
