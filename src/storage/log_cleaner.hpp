#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <vector>

#include "common/types.hpp"
#include "storage/key_offset_map.hpp"
#include "storage/record_batch.hpp"

namespace dariyakyu::storage {

// One batch, as the cleaner needs it: the header, and the bytes.
//
// The bytes rather than a FileRange, which is the one place this codebase copies
// record payloads on purpose. Compaction has to look INSIDE records — at their
// keys — and rebuild batches from the ones it keeps, so there is nothing for
// sendfile to do here. That is why the cleaner is a background thread rather than
// anything on a request path.
struct ScannedBatch {
    BatchHeader               header;
    std::vector<std::uint8_t> bytes;
    std::uint64_t             position = 0;
};

// Walks every complete batch in a segment file, oldest first.
//
// Stops at the first batch that does not fit in what remains rather than
// throwing. A sealed segment should never end mid-batch — sealing is what
// promises it does not — but a torn tail is exactly what a crash during a roll
// can leave, and a cleaner that threw there would be permanently unable to clean
// that partition. Everything before the tear is complete and is worth compacting;
// the tear itself is left alone for recovery to find.
void scanSegment(const std::filesystem::path& logFile,
                 const std::function<void(const ScannedBatch&)>& visit);

}  // namespace dariyakyu::storage
