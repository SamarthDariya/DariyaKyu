#include "storage/log_cleaner.hpp"

#include "common/errors.hpp"
#include "common/file_handle.hpp"

using namespace std;

namespace dariyakyu::storage {

void scanSegment(const filesystem::path&                        logFile,
                 const function<void(const ScannedBatch&)>&     visit) {
    FileHandle     handle(logFile, FileHandle::Mode::ReadOnly);
    const uint64_t limit = handle.size();

    uint64_t position = 0;
    while (position + kBatchHeaderSize <= limit) {
        // The header first, because it says how long the batch is. Reading a
        // fixed-size prefix is the only way to find that out without trusting a
        // length we have not validated.
        vector<uint8_t> headerBytes(kBatchHeaderSize);
        if (handle.readAt(position, headerBytes) < headerBytes.size()) return;

        size_t total = 0;
        try {
            total = RecordBatch::totalSizeOf(headerBytes);
        } catch (const CorruptData&) {
            return;   // a torn tail; everything before it was complete
        }
        if (position + total > limit) return;

        ScannedBatch batch;
        batch.position = position;
        batch.bytes.resize(total);
        if (handle.readAt(position, batch.bytes) < total) return;

        try {
            batch.header = RecordBatch::parseHeader(batch.bytes);
        } catch (const CorruptData&) {
            return;
        }

        // Checked EXPLICITLY, because parseHeader does not — it reads the crc
        // field without testing it, which is right for the read path where the
        // payload is never touched. Here it is the whole point: a cleaner that
        // rewrote a corrupt batch would launder the corruption into a freshly
        // sealed segment that recovery has no reason to re-examine, turning
        // damage a scrub could still find into damage nothing will.
        if (!RecordBatch::verifyCrc(batch.bytes)) return;

        visit(batch);
        position += total;
    }
}

}  // namespace dariyakyu::storage
