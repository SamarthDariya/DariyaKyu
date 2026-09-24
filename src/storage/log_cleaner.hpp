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

// Everything the retain decision needs that is not in the record itself.
struct RetainRules {
    const KeyOffsetMap* map               = nullptr;
    std::int64_t        nowMs             = 0;
    std::int64_t        deleteRetentionMs = 0;
};

// Whether a rewrite keeps this record.
//
// Four cases, and three of them resolve toward keeping. That asymmetry is
// deliberate: a record wrongly dropped is gone, and a record wrongly kept costs
// disk until the next pass.
//
//   no key         KEEP. It cannot be compacted by key at all, so there is no
//                  sense in which a newer record replaces it. Kafka treats an
//                  unkeyed record on a compacted topic as a producer error and
//                  discards it; this keeps it, because the failure mode there is
//                  a misconfigured producer silently losing everything it writes,
//                  which is worse than a compacted topic that does not fully
//                  compact.
//   not the newest KEEP NOT. The whole point.
//   newest, valued KEEP.
//   newest, null   a TOMBSTONE — keep until the horizon, then drop.
//
// The horizon is measured against the record's own timestamp, which comes from
// the producer. That inherits the skew RetentionPolicy already accepts and for
// the same reason: the alternative is deciding by when the broker happened to
// receive it, which makes the window meaningless after any replay or backfill.
// Kafka instead measures from when the segment was last cleaned, which is more
// robust to a bad clock and needs per-segment state this does not keep — banked.
// `offset` and `timestampMs` are absolute, resolved against the batch header —
// a Record holds both as deltas, and a predicate that took the deltas would be
// answering about the wrong record.
bool retain(const Record& record, Offset offset, std::int64_t timestampMs,
            const RetainRules& rules);

// Whether a record says "this key is gone". A NULL value, which is not the same
// as an empty one: an empty value is an ordinary record whose value happens to be
// zero bytes long, and it means the key is still there.
inline bool isTombstone(const Record& record) { return !record.value.has_value(); }

}  // namespace dariyakyu::storage
