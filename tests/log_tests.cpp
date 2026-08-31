#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "common/errors.hpp"
#include "storage/offset_index.hpp"
#include "storage/record_batch.hpp"
#include "storage/segment.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::storage;

namespace {

// Same isolation the common suite uses: filesystem tests leak into each other
// more readily than any other kind.
class TempDir {
public:
    explicit TempDir(const string& name)
        : path_(filesystem::temp_directory_path() / ("dariyakyu-test-" + name)) {
        filesystem::remove_all(path_);
        filesystem::create_directories(path_);
    }
    ~TempDir() { filesystem::remove_all(path_); }

    TempDir(const TempDir&)            = delete;
    TempDir& operator=(const TempDir&) = delete;

    filesystem::path file(const string& name) const { return path_ / name; }

private:
    filesystem::path path_;
};

constexpr size_t kIndexBytes = 1024;   // 128 entries, plenty for most cases here

vector<uint8_t> readFile(const filesystem::path& path) {
    ifstream in(path, ios::binary);
    return vector<uint8_t>(istreambuf_iterator<char>(in), istreambuf_iterator<char>());
}

void writeFile(const filesystem::path& path, const vector<uint8_t>& bytes) {
    ofstream out(path, ios::binary | ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<streamsize>(bytes.size()));
}

// Hand-built index bytes, so the recovery cases can describe a file that no
// correct run of the code would produce.
void appendEntryBytes(vector<uint8_t>& out, uint32_t relativeOffset, uint32_t position) {
    for (int shift = 24; shift >= 0; shift -= 8)
        out.push_back(static_cast<uint8_t>(relativeOffset >> shift));
    for (int shift = 24; shift >= 0; shift -= 8)
        out.push_back(static_cast<uint8_t>(position >> shift));
}

// What lookup() must agree with, written the slow obvious way.
OffsetIndex::Entry referenceLookup(const vector<OffsetIndex::Entry>& entries, uint32_t target) {
    OffsetIndex::Entry best{};
    for (const auto& entry : entries)
        if (entry.relativeOffset <= target) best = entry;
    return best;
}

}  // namespace

static_assert(!is_copy_constructible_v<OffsetIndex>,
              "an index owns a mapping; copying one would mean two owners of the same pages");
static_assert(is_move_constructible_v<OffsetIndex>,
              "ActiveSegment holds an OffsetIndex by value and seal() moves the segment");

// ===========================================================================
// Construction
// ===========================================================================

TEST_CASE("A new index is preallocated and holds nothing") {
    TempDir dir("index-create");
    const auto path = dir.file("00000000000000000000.index");

    OffsetIndex index = OffsetIndex::create(path, Offset(0), kIndexBytes);

    CHECK(index.isEmpty());
    CHECK(index.entryCount() == 0);
    CHECK(index.maxEntries() == kIndexBytes / OffsetIndex::kEntrySize);
    CHECK(index.usedBytes() == 0);
    CHECK(index.isWritable());
    CHECK_FALSE(index.isFull());

    // The bytes exist on disk before a single entry is written: an append must
    // never be the thing that extends a live mapping.
    CHECK(filesystem::file_size(path) == kIndexBytes);
}

TEST_CASE("An index with room for no entries is a configuration error") {
    TempDir dir("index-too-small");
    CHECK_THROWS_AS(OffsetIndex::create(dir.file("a.index"), Offset(0), 7), Error);
}

TEST_CASE("maxBytes is rounded down to whole entries") {
    TempDir dir("index-rounding");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), 8 * 10 + 5);
    CHECK(index.maxEntries() == 10);
}

TEST_CASE("Creating an index discards whatever was there before") {
    TempDir dir("index-stale");
    const auto path = dir.file("a.index");

    // A stale index describing some other segment's bytes. Inheriting these
    // would binary search perfectly and point at the wrong records.
    vector<uint8_t> stale;
    appendEntryBytes(stale, 500, 9999);
    appendEntryBytes(stale, 900, 19999);
    writeFile(path, stale);

    OffsetIndex index = OffsetIndex::create(path, Offset(0), kIndexBytes);
    CHECK(index.isEmpty());
    CHECK(index.lookup(Offset(700)).position == 0);
}

// ===========================================================================
// Append and lookup
// ===========================================================================

TEST_CASE("An appended entry is found exactly") {
    TempDir     dir("index-exact");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(100), kIndexBytes);

    index.append(Offset(140), 4096);
    CHECK(index.entryCount() == 1);
    CHECK(index.usedBytes() == 8);

    const auto entry = index.lookup(Offset(140));
    CHECK(entry.relativeOffset == 40);
    CHECK(entry.position == 4096);
    CHECK(entry.offsetFrom(Offset(100)) == Offset(140));
}

TEST_CASE("A lookup between entries lands on the one before it") {
    TempDir     dir("index-between");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);

    index.append(Offset(10), 4096);
    index.append(Offset(20), 8192);
    index.append(Offset(30), 12288);

    // The whole point of a sparse index: 17 is not indexed, so the answer is
    // where to start scanning for it.
    CHECK(index.lookup(Offset(17)).position == 4096);
    CHECK(index.lookup(Offset(20)).position == 8192);
    CHECK(index.lookup(Offset(29)).position == 8192);
    CHECK(index.lookup(Offset(30)).position == 12288);
}

TEST_CASE("A lookup past the last entry returns the last entry") {
    TempDir     dir("index-past-end");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);

    index.append(Offset(10), 4096);
    index.append(Offset(20), 8192);

    CHECK(index.lookup(Offset(1'000'000)).position == 8192);
}

TEST_CASE("An empty index answers with the start of the segment") {
    TempDir     dir("index-empty-lookup");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(500), kIndexBytes);

    // A segment younger than its first index interval. Scanning from byte zero
    // is correct, and the hot read path carries no special case for it.
    const auto entry = index.lookup(Offset(507));
    CHECK(entry.relativeOffset == 0);
    CHECK(entry.position == 0);
}

TEST_CASE("A lookup below the first entry answers with the start of the segment") {
    TempDir     dir("index-before-first");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);

    index.append(Offset(50), 4096);
    CHECK(index.lookup(Offset(7)).position == 0);
}

TEST_CASE("A lookup below the segment's base offset is a routing bug, not a miss") {
    TempDir     dir("index-below-base");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(100), kIndexBytes);
    index.append(Offset(140), 4096);

    CHECK_THROWS_AS(index.lookup(Offset(99)), OffsetInvariantViolated);
}

TEST_CASE("Binary search agrees with a linear scan at every offset") {
    TempDir     dir("index-exhaustive");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), 8 * 200);

    vector<OffsetIndex::Entry> expected;
    for (uint32_t i = 1; i <= 100; ++i) {
        index.append(Offset(i * 7), i * 4096);
        expected.push_back({i * 7, i * 4096});
    }

    // Every offset in and around the indexed range, including the gaps and both
    // edges — the off-by-one in a hand-rolled binary search hides in exactly
    // these.
    for (uint32_t target = 0; target <= 100 * 7 + 10; ++target) {
        const auto actual = index.lookup(Offset(target));
        const auto want   = referenceLookup(expected, target);
        CHECK(actual.relativeOffset == want.relativeOffset);
        CHECK(actual.position == want.position);
    }
}

// ===========================================================================
// Append invariants
// ===========================================================================

TEST_CASE("Offsets must strictly advance") {
    TempDir     dir("index-monotonic-offset");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);

    index.append(Offset(20), 4096);
    CHECK_THROWS_AS(index.append(Offset(20), 8192), OffsetInvariantViolated);
    CHECK_THROWS_AS(index.append(Offset(19), 8192), OffsetInvariantViolated);
    CHECK(index.entryCount() == 1);
}

TEST_CASE("Positions must strictly advance") {
    TempDir     dir("index-monotonic-position");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);

    index.append(Offset(20), 4096);
    CHECK_THROWS_AS(index.append(Offset(30), 4096), OffsetInvariantViolated);
    CHECK_THROWS_AS(index.append(Offset(30), 100), OffsetInvariantViolated);
    CHECK(index.entryCount() == 1);
}

TEST_CASE("Position zero is reserved, because it marks unwritten padding") {
    TempDir     dir("index-position-zero");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);

    CHECK_THROWS_AS(index.append(Offset(0), 0), OffsetInvariantViolated);
    CHECK(index.isEmpty());
}

TEST_CASE("An offset below the base offset cannot be indexed") {
    TempDir     dir("index-append-below-base");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(100), kIndexBytes);

    CHECK_THROWS_AS(index.append(Offset(99), 4096), OffsetInvariantViolated);
}

TEST_CASE("A relative offset too large for 32 bits is rejected, not truncated") {
    TempDir     dir("index-relative-overflow");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);

    // Silently wrapping would produce a small relative offset that binary
    // searches fine and points at the wrong record.
    const int64_t tooFar = static_cast<int64_t>(numeric_limits<uint32_t>::max()) + 1;
    CHECK_THROWS_AS(index.append(Offset(tooFar), 4096), OffsetInvariantViolated);
}

TEST_CASE("A full index drops entries rather than failing the append") {
    TempDir     dir("index-full");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), 8 * 3);

    index.append(Offset(10), 4096);
    index.append(Offset(20), 8192);
    index.append(Offset(30), 12288);
    CHECK(index.isFull());

    // A capacity limit must never turn into a failed produce request: the index
    // just becomes sparser, and lookups stay correct.
    CHECK_NOTHROW(index.append(Offset(40), 16384));
    CHECK(index.entryCount() == 3);
    CHECK(index.lookup(Offset(45)).position == 12288);
}

TEST_CASE("A full index still reports a caller's broken bookkeeping") {
    TempDir     dir("index-full-invariant");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), 8 * 1);

    index.append(Offset(10), 4096);
    CHECK(index.isFull());
    // Dropping the entry is fine; hiding the fact that the caller went
    // backwards is not.
    CHECK_THROWS_AS(index.append(Offset(5), 8192), OffsetInvariantViolated);
}

// ===========================================================================
// On-disk format
// ===========================================================================

TEST_CASE("Entries are stored big-endian, eight bytes each") {
    TempDir    dir("index-endianness");
    const auto path = dir.file("a.index");

    {
        OffsetIndex index = OffsetIndex::create(path, Offset(0), kIndexBytes);
        index.append(Offset(0x01020304), 0x05060708);
        index.flushAndTrim();
    }

    const auto bytes = readFile(path);
    REQUIRE(bytes.size() == 8);
    CHECK(bytes[0] == 0x01);
    CHECK(bytes[1] == 0x02);
    CHECK(bytes[2] == 0x03);
    CHECK(bytes[3] == 0x04);
    CHECK(bytes[4] == 0x05);
    CHECK(bytes[5] == 0x06);
    CHECK(bytes[6] == 0x07);
    CHECK(bytes[7] == 0x08);
}

TEST_CASE("Sealing trims the preallocated tail away") {
    TempDir    dir("index-trim");
    const auto path = dir.file("a.index");

    OffsetIndex index = OffsetIndex::create(path, Offset(0), kIndexBytes);
    index.append(Offset(10), 4096);
    index.append(Offset(20), 8192);
    CHECK(filesystem::file_size(path) == kIndexBytes);

    index.flushAndTrim();
    CHECK(filesystem::file_size(path) == 16);
}

TEST_CASE("A trimmed index is spent, and says so rather than reading unmapped pages") {
    TempDir     dir("index-spent");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), kIndexBytes);
    index.append(Offset(10), 4096);
    index.flushAndTrim();

    CHECK(index.isSpent());
    CHECK_THROWS_AS(index.lookup(Offset(10)), Error);
    CHECK_THROWS_AS(index.append(Offset(20), 8192), Error);
    CHECK_THROWS_AS(index.flushAndTrim(), Error);
}

TEST_CASE("A sealed index is read-only") {
    TempDir    dir("index-sealed-readonly");
    const auto path = dir.file("a.index");

    {
        OffsetIndex index = OffsetIndex::create(path, Offset(0), kIndexBytes);
        index.append(Offset(10), 4096);
        index.flushAndTrim();
    }

    OffsetIndex sealed = OffsetIndex::openSealed(path, Offset(0));
    CHECK_FALSE(sealed.isWritable());
    CHECK_THROWS_AS(sealed.append(Offset(20), 8192), Error);
}

TEST_CASE("A sealed index reads back exactly what was written") {
    TempDir    dir("index-roundtrip");
    const auto path = dir.file("a.index");

    {
        OffsetIndex index = OffsetIndex::create(path, Offset(1000), kIndexBytes);
        for (uint32_t i = 1; i <= 20; ++i) index.append(Offset(1000 + i * 13), i * 4096);
        index.flushAndTrim();
    }

    OffsetIndex sealed = OffsetIndex::openSealed(path, Offset(1000));
    CHECK(sealed.entryCount() == 20);
    for (uint32_t i = 1; i <= 20; ++i) {
        const auto entry = sealed.lookup(Offset(1000 + i * 13));
        CHECK(entry.relativeOffset == i * 13);
        CHECK(entry.position == i * 4096);
    }
}

// ===========================================================================
// Recovery
// ===========================================================================

TEST_CASE("A zero-length index opens as an empty one") {
    TempDir    dir("index-zero-length");
    const auto path = dir.file("a.index");
    writeFile(path, {});

    // A segment that rolled on age before its first index interval. Refusing
    // this would fail to open a perfectly good low-traffic partition.
    OffsetIndex sealed = OffsetIndex::openSealed(path, Offset(0));
    CHECK(sealed.isEmpty());
    CHECK(sealed.lookup(Offset(5)).position == 0);
}

TEST_CASE("An untrimmed index recovers its real entry count, not its file size") {
    TempDir    dir("index-untrimmed");
    const auto path = dir.file("a.index");

    // The crash case: the broker died with this segment still active, so the
    // file is at its full preallocated length with a zero tail.
    {
        OffsetIndex index = OffsetIndex::create(path, Offset(0), kIndexBytes);
        index.append(Offset(10), 4096);
        index.append(Offset(20), 8192);
        index.append(Offset(30), 12288);
    }   // no flushAndTrim — this is what a crash leaves behind
    REQUIRE(filesystem::file_size(path) == kIndexBytes);

    OffsetIndex recovered = OffsetIndex::openSealed(path, Offset(0));
    CHECK(recovered.entryCount() == 3);
    CHECK(recovered.lookup(Offset(25)).position == 8192);
    // Trusting the file size would have handed out a {0, 0} from the padding.
    CHECK(recovered.lookup(Offset(1'000'000)).position == 12288);
}

TEST_CASE("An index whose size is not a whole number of entries drops the partial one") {
    TempDir    dir("index-partial-entry");
    const auto path = dir.file("a.index");

    vector<uint8_t> bytes;
    appendEntryBytes(bytes, 10, 4096);
    appendEntryBytes(bytes, 20, 8192);
    bytes.insert(bytes.end(), {0xDE, 0xAD, 0xBE});   // died mid-write

    writeFile(path, bytes);

    OffsetIndex recovered = OffsetIndex::openSealed(path, Offset(0));
    CHECK(recovered.entryCount() == 2);
    CHECK(recovered.lookup(Offset(25)).position == 8192);
}

TEST_CASE("A completely full untrimmed index recovers every entry") {
    TempDir    dir("index-full-untrimmed");
    const auto path = dir.file("a.index");

    // No zero tail to find, so the boundary search must report the whole file.
    {
        OffsetIndex index = OffsetIndex::create(path, Offset(0), 8 * 4);
        for (uint32_t i = 1; i <= 4; ++i) index.append(Offset(i * 10), i * 4096);
        CHECK(index.isFull());
    }

    OffsetIndex recovered = OffsetIndex::openSealed(path, Offset(0));
    CHECK(recovered.entryCount() == 4);
    CHECK(recovered.lookup(Offset(45)).position == 4 * 4096);
}

// ===========================================================================
// Ownership and concurrency
// ===========================================================================

TEST_CASE("An index can be moved, and the source is left inert") {
    TempDir    dir("index-move");
    const auto path = dir.file("a.index");

    OffsetIndex source = OffsetIndex::create(path, Offset(50), kIndexBytes);
    source.append(Offset(60), 4096);

    OffsetIndex moved = std::move(source);
    CHECK(moved.entryCount() == 1);
    CHECK(moved.baseOffset() == Offset(50));
    CHECK(moved.lookup(Offset(65)).position == 4096);

    // ActiveSegment is moved into seal(); a moved-from index must not look like
    // a working one holding a mapping it no longer owns.
    CHECK(source.isSpent());
}

TEST_CASE("A reader never observes half an entry while the appender runs") {
    TempDir     dir("index-concurrent");
    OffsetIndex index = OffsetIndex::create(dir.file("a.index"), Offset(0), 8 * 4096);

    constexpr uint32_t kEntries = 4000;
    atomic<bool>       done{false};
    atomic<int>        torn{0};
    atomic<int>        observations{0};

    // Positions are ten times the relative offset, so any entry that is a mix
    // of two writes fails the relation.
    //
    // The loop also runs until at least one observation has landed. Without
    // that, the appender can finish all 4000 entries before this thread is ever
    // scheduled, `done` is already true, the body runs zero times, and the
    // assertion that the reader saw something fails — a test that loses a race
    // with itself rather than a real defect.
    thread reader([&] {
        while (!done.load(memory_order_acquire) || observations.load(memory_order_relaxed) == 0) {
            const auto entry = index.lookup(Offset(kEntries * 10));
            observations.fetch_add(1, memory_order_relaxed);
            if (entry.position != entry.relativeOffset * 10)
                torn.fetch_add(1, memory_order_relaxed);
        }
    });

    for (uint32_t i = 1; i <= kEntries; ++i) index.append(Offset(i * 10), i * 100);
    done.store(true, memory_order_release);
    reader.join();

    CHECK(torn.load() == 0);
    CHECK(observations.load() > 0);
    CHECK(index.entryCount() == kEntries);
}

// ===========================================================================
// Segment file naming
// ===========================================================================

TEST_CASE("A segment name is its base offset, zero padded to twenty digits") {
    CHECK(segmentBaseName(Offset(0)) == "00000000000000000000");
    CHECK(segmentBaseName(Offset(1073741)) == "00000000000001073741");
    CHECK(segmentBaseName(Offset(9)).size() == 20);
}

TEST_CASE("Padded names sort in the same order as the offsets they encode") {
    // The whole reason for the padding. Without it "9" > "10" as a string, and a
    // sorted directory listing would no longer be a sorted segment table.
    CHECK(segmentBaseName(Offset(0)) < segmentBaseName(Offset(1)));
    CHECK(segmentBaseName(Offset(9)) < segmentBaseName(Offset(10)));
    CHECK(segmentBaseName(Offset(99)) < segmentBaseName(Offset(100)));
}

TEST_CASE("A segment's two files sit in the partition directory") {
    const filesystem::path dir = "/data/orders-0";
    CHECK(segmentLogPath(dir, Offset(1073741)) == "/data/orders-0/00000000000001073741.log");
    CHECK(segmentIndexPath(dir, Offset(1073741)) == "/data/orders-0/00000000000001073741.index");
}

TEST_CASE("A base offset round-trips through a log file name") {
    const auto path = segmentLogPath("/data/orders-0", Offset(1073741));
    CHECK(baseOffsetFromLogPath(path) == Offset(1073741));
    CHECK(baseOffsetFromLogPath(segmentLogPath("/d", Offset(0))) == Offset(0));
}

TEST_CASE("A name that is not a segment log is rejected, not read as offset zero") {
    // Each of these is a real thing that turns up in a partition directory. A
    // phantom segment claiming offset 0 would shadow the real first segment.
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/partition.meta"), CorruptData);
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/leader-epoch-checkpoint"), CorruptData);

    // Nineteen digits, then twenty-one.
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/0000000000000000000.log"), CorruptData);
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/000000000000000000000.log"), CorruptData);

    // Right length, non-digit inside.
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/0000000000000000000x.log"), CorruptData);
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/-0000000000000000001.log"), CorruptData);

    // Right digits, wrong extension — the index file must not parse as a log.
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/00000000000000000000.index"), CorruptData);
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/00000000000000000000.log.swp"), CorruptData);
}

TEST_CASE("Twenty digits that overflow int64 are corruption, not an out_of_range escape") {
    // int64 max is 19 digits, so twenty digits can describe a number it cannot
    // hold. stoll signals that with out_of_range; callers only ever catch
    // CorruptData, so it has to be translated rather than escaping as a
    // different type from every other bad name.
    CHECK_THROWS_AS(baseOffsetFromLogPath("/d/99999999999999999999.log"), CorruptData);
}

// ===========================================================================
// Roll policy
// ===========================================================================

TEST_CASE("The default index can describe a full-size segment") {
    // Asserted at compile time in segment.hpp too; spelled out here because the
    // relationship between the four knobs is the whole content of the struct.
    const RollPolicy policy;
    const size_t entries = policy.maxIndexBytes / OffsetIndex::kEntrySize;

    CHECK(entries == 1310720);
    CHECK(entries * policy.indexIntervalBytes > policy.maxSegmentBytes);

    // If this ever fails, segments roll on a full index instead of on size, and
    // maxSegmentBytes quietly stops meaning anything.
    CHECK(entries * policy.indexIntervalBytes / policy.maxSegmentBytes == 5);
}

// ===========================================================================
// ActiveSegment: creation
// ===========================================================================

namespace {

RollPolicy testPolicy() {
    // Deliberately small, so tests reach an index interval and a size limit
    // without writing megabytes.
    RollPolicy policy;
    policy.maxSegmentBytes    = 1 << 16;
    policy.indexIntervalBytes = 256;
    policy.maxIndexBytes      = 1024;
    return policy;
}

}  // namespace

TEST_CASE("A new segment creates both files and starts empty at its base offset") {
    TempDir dir("seg-create");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(100), testPolicy());

    CHECK(segment->baseOffset() == Offset(100));
    CHECK(segment->nextOffset() == Offset(100));   // one past the last it holds
    CHECK(segment->isEmpty());
    CHECK(segment->sizeBytes() == 0);
    CHECK(segment->largestTimestamp() == -1);

    // Nothing is in it, so it contains nothing — not even its own base offset.
    CHECK_FALSE(segment->contains(Offset(100)));

    CHECK(filesystem::exists(segmentLogPath(dir.file(""), Offset(100))));
    CHECK(filesystem::exists(segmentIndexPath(dir.file(""), Offset(100))));

    // The index is preallocated up front, because growing a live mmap and then
    // touching the new pages raises SIGBUS.
    CHECK(filesystem::file_size(segmentIndexPath(dir.file(""), Offset(100))) ==
          testPolicy().maxIndexBytes);
    CHECK(segment->index().isEmpty());
    CHECK(segment->policy().indexIntervalBytes == 256);
}

TEST_CASE("Creating a segment that already exists is refused") {
    TempDir dir("seg-create-twice");
    auto    first = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    // Appending into a file that already holds a segment would interleave two
    // segments' records — corruption no checksum could catch, since every
    // individual batch would still verify.
    CHECK_THROWS(ActiveSegment::create(dir.file(""), Offset(0), testPolicy()));
}

TEST_CASE("A refused create leaves the existing index untouched") {
    TempDir dir("seg-create-preserves-index");
    const auto indexFile = segmentIndexPath(dir.file(""), Offset(0));

    auto first = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    first->index().isEmpty();
    const auto sizeBefore = filesystem::file_size(indexFile);

    // OffsetIndex::create truncates an existing index to zero. If argument
    // evaluation order let it run before the .log was claimed, this second
    // create would wipe the first segment's index on its way to failing.
    CHECK_THROWS(ActiveSegment::create(dir.file(""), Offset(0), testPolicy()));
    CHECK(filesystem::file_size(indexFile) == sizeBefore);
}

// ===========================================================================
// ActiveSegment: append
// ===========================================================================

namespace {

// One batch of `records` records, each carrying `payloadBytes` of value, stamped
// so the header says it begins at `baseOffset`.
vector<uint8_t> makeBatch(Offset baseOffset, int64_t timestamp, size_t payloadBytes = 32,
                          int records = 1) {
    RecordBatchBuilder    builder;
    const vector<uint8_t> value(payloadBytes, 0xAB);
    for (int i = 0; i < records; ++i)
        builder.append(timestamp + i, nullopt, span<const uint8_t>(value));

    auto bytes = builder.build();
    // What Log will do on the write path: stamp the assigned offset into the
    // header, in place, without recomputing the checksum.
    RecordBatch::stampBaseOffset(bytes, baseOffset);
    return bytes;
}

// Appends `count` single-record batches, returning total bytes written.
uint64_t fillSegment(ActiveSegment& segment, int count, size_t payloadBytes = 32,
                     int64_t startTimestamp = 1000) {
    uint64_t written = 0;
    for (int i = 0; i < count; ++i) {
        const Offset base  = segment.nextOffset();
        auto         bytes = makeBatch(base, startTimestamp + i, payloadBytes);
        segment.append(base, bytes);
        written += bytes.size();
    }
    return written;
}

}  // namespace

TEST_CASE("Appending advances the next offset by the batch's record count") {
    TempDir dir("seg-append");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    auto batch = makeBatch(Offset(0), 5000, 16, 3);   // three records
    segment->append(Offset(0), batch);

    CHECK(segment->nextOffset() == Offset(3));
    CHECK(segment->sizeBytes() == batch.size());
    CHECK_FALSE(segment->isEmpty());

    CHECK(segment->contains(Offset(0)));
    CHECK(segment->contains(Offset(2)));
    CHECK_FALSE(segment->contains(Offset(3)));
}

TEST_CASE("The largest timestamp is the maximum seen, not the latest appended") {
    TempDir dir("seg-timestamps");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    auto first = makeBatch(Offset(0), 9000, 16);
    segment->append(Offset(0), first);
    CHECK(segment->largestTimestamp() == 9000);

    // A producer may send an older timestamp than one already stored — clock
    // skew, or a backfill. The segment tracks the maximum, because M3's
    // retention asks "how old is the newest record here".
    auto older = makeBatch(Offset(1), 500, 16);
    segment->append(Offset(1), older);
    CHECK(segment->largestTimestamp() == 9000);
}

TEST_CASE("Successive appends stay contiguous") {
    TempDir dir("seg-contiguous");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(200), testPolicy());
    const uint64_t written = fillSegment(*segment, 10);

    CHECK(segment->nextOffset() == Offset(210));
    CHECK(segment->sizeBytes() == written);
}

TEST_CASE("A batch that does not start where the segment ends is refused") {
    TempDir dir("seg-gap");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 2);
    REQUIRE(segment->nextOffset() == Offset(2));

    // A gap would make the offsets in this file stop being a sequence, and every
    // later lookup wrong.
    auto gap = makeBatch(Offset(5), 1000);
    CHECK_THROWS_AS(segment->append(Offset(5), gap), OffsetInvariantViolated);

    // An overlap would write two records claiming the same offset.
    auto overlap = makeBatch(Offset(1), 1000);
    CHECK_THROWS_AS(segment->append(Offset(1), overlap), OffsetInvariantViolated);

    // Neither wrote anything.
    CHECK(segment->nextOffset() == Offset(2));
}

TEST_CASE("An unstamped batch is caught rather than stored claiming offset zero") {
    TempDir dir("seg-unstamped");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(10), testPolicy());

    RecordBatchBuilder    builder;
    const vector<uint8_t> value(8, 0x01);
    builder.append(1000, nullopt, span<const uint8_t>(value));
    auto unstamped = builder.build();   // header still says base offset 0

    CHECK_THROWS_AS(segment->append(Offset(10), unstamped), OffsetInvariantViolated);
    CHECK(segment->isEmpty());
}
