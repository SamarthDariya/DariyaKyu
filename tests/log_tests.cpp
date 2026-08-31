#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
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

constexpr size_t kIndexBytes = 1024;

// Larger than anything these tests write, so reads are not capped unless a test
// is specifically about capping.
constexpr size_t kBigFetch = 1 << 20;   // 128 entries, plenty for most cases here

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

// ===========================================================================
// ActiveSegment: sparse indexing
// ===========================================================================

TEST_CASE("The index stays empty until an interval of log has been written") {
    TempDir dir("seg-index-start");
    auto    policy = testPolicy();   // 256-byte interval
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), policy);

    CHECK(segment->index().isEmpty());

    // One batch is nowhere near an interval, so nothing is indexed yet.
    fillSegment(*segment, 1, 32);
    CHECK(segment->index().isEmpty());
}

TEST_CASE("Entries accumulate one per interval, not one per batch") {
    TempDir dir("seg-index-sparse");
    auto    policy  = testPolicy();
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), policy);

    constexpr int kBatches = 60;
    const uint64_t written = fillSegment(*segment, kBatches, 64);

    const size_t entries = segment->index().entryCount();

    // Sparse: far fewer entries than batches. That ratio is the whole point —
    // it is what keeps a 1 GiB segment's index in the megabytes.
    CHECK(entries > 0);
    CHECK(entries < static_cast<size_t>(kBatches));

    // Never DENSER than one entry per interval. This is the direction that
    // matters: the interval is a promise about how big the index can get.
    CHECK(entries <= written / policy.indexIntervalBytes);

    // And the precise invariant, which a ratio cannot express: consecutive
    // entries are at least one interval apart in the file.
    //
    // They are also usually MORE than one interval apart, because the counter
    // resets after an entry is written and then accumulates the current batch
    // too — so the real spacing is interval + one batch, not interval. Asserting
    // a ratio against `written / interval` gets this wrong; asserting the gap
    // directly does not.
    vector<uint32_t> positions;
    for (int64_t offset = 0; offset < segment->nextOffset() - Offset(0); ++offset) {
        const uint32_t position = segment->index().lookup(Offset(offset)).position;
        if (position != 0 && (positions.empty() || positions.back() != position))
            positions.push_back(position);
    }
    REQUIRE(positions.size() == entries);

    for (size_t i = 1; i < positions.size(); ++i)
        CHECK(positions[i] - positions[i - 1] >= policy.indexIntervalBytes);
}

TEST_CASE("The segment's first batch is never indexed") {
    TempDir dir("seg-index-first");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 60, 64);

    REQUIRE(segment->index().entryCount() > 0);

    // Position 0 is OffsetIndex's marker for unwritten padding, so an entry
    // there would be indistinguishable from padding on the next recovery. It is
    // also information-free: lookup() already falls back to the front of the
    // file. OffsetIndex::append throws on position 0, so an entry ever being
    // attempted there would have failed the appends above.
    CHECK(segment->index().lookup(Offset(0)).position == 0);      // the fallback
    CHECK(segment->index().lookup(segment->nextOffset() - 1).position > 0);
}

TEST_CASE("Every index entry points at a real batch boundary") {
    TempDir dir("seg-index-boundaries");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    // Record where each batch actually starts, then check the index only ever
    // names one of those positions. An entry pointing into the middle of a batch
    // would make a read start on garbage.
    vector<uint64_t> starts;
    for (int i = 0; i < 60; ++i) {
        starts.push_back(segment->sizeBytes());
        const Offset base = segment->nextOffset();
        auto bytes = makeBatch(base, 1000 + i, 64);
        segment->append(base, bytes);
    }

    for (int64_t offset = 0; offset < segment->nextOffset() - Offset(0); ++offset) {
        const auto entry = segment->index().lookup(Offset(offset));
        const bool isBoundary =
            entry.position == 0 ||
            find(starts.begin(), starts.end(), entry.position) != starts.end();
        CHECK(isBoundary);
        // And the entry never overshoots the offset asked for.
        CHECK(entry.offsetFrom(Offset(0)) <= Offset(offset));
    }
}

// ===========================================================================
// SegmentBase: batchAt
// ===========================================================================

TEST_CASE("batchAt reads the batch at a position and says how far the next one is") {
    TempDir dir("seg-batchat");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(500), testPolicy());

    auto first = makeBatch(Offset(500), 7000, 32, 3);   // offsets 500..502
    segment->append(Offset(500), first);
    auto second = makeBatch(Offset(503), 8000, 32, 2);  // offsets 503..504
    segment->append(Offset(503), second);

    const auto at = segment->batchAt(0, segment->sizeBytes());
    REQUIRE(at.has_value());
    CHECK(at->position == 0);
    CHECK(at->totalSize == first.size());
    CHECK(at->header.baseOffset == Offset(500));
    CHECK(at->header.recordCount == 3);
    CHECK(at->header.lastOffset() == Offset(502));
    CHECK(at->header.maxTimestamp == 7002);
}

TEST_CASE("Adding totalSize walks from one batch to the next") {
    TempDir dir("seg-batchat-walk");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 12, 48);

    // This loop is the shape of every forward scan in the codebase: read a
    // header, use it, step by totalSize, repeat until nothing comes back.
    const uint64_t limit = segment->sizeBytes();
    uint64_t       position = 0;
    int            seen     = 0;

    while (auto at = segment->batchAt(position, limit)) {
        CHECK(at->position == position);
        CHECK(at->header.baseOffset == Offset(seen));
        position += at->totalSize;
        ++seen;
    }

    CHECK(seen == 12);
    CHECK(position == limit);   // the walk lands exactly on the end
}

TEST_CASE("batchAt finds nothing at or past the end") {
    TempDir dir("seg-batchat-end");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 3);
    const uint64_t limit = segment->sizeBytes();

    CHECK_FALSE(segment->batchAt(limit, limit).has_value());
    CHECK_FALSE(segment->batchAt(limit + 100, limit).has_value());

    // An empty segment has nothing anywhere.
    auto empty = ActiveSegment::create(dir.file(""), Offset(9), testPolicy());
    CHECK_FALSE(empty->batchAt(0, 0).has_value());
}

TEST_CASE("A batch extending past the limit is absent, not corrupt") {
    TempDir dir("seg-batchat-inflight");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    auto batch = makeBatch(Offset(0), 1000, 64);
    segment->append(Offset(0), batch);

    // Exactly what a reader sees mid-append: the header is on disk and parses,
    // but the batch it describes is longer than the bytes the reader was told
    // about. It must read as "not there yet", never as damage — otherwise every
    // concurrent read during a write would look like corruption.
    const uint64_t shortLimit = batch.size() - 1;
    CHECK_FALSE(segment->batchAt(0, shortLimit).has_value());

    // With the full extent it is there.
    CHECK(segment->batchAt(0, batch.size()).has_value());
}

TEST_CASE("Fewer bytes than a header is absent too") {
    TempDir dir("seg-batchat-tiny");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 1);

    // 60 bytes cannot hold a 61-byte prefix, so there is nothing to interpret.
    CHECK_FALSE(segment->batchAt(0, kBatchHeaderSize - 1).has_value());
    CHECK(segment->batchAt(0, segment->sizeBytes()).has_value());
}

TEST_CASE("batchAt called off a batch boundary is corruption") {
    TempDir dir("seg-batchat-misaligned");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 4, 64);

    // The precondition made visible: position must be a real boundary. Five
    // bytes in, the magic byte lands on part of another field and does not read
    // as 2. This is why a forward scan must step by totalSize and never guess.
    CHECK_THROWS_AS(segment->batchAt(5, segment->sizeBytes()), CorruptData);
}

// ===========================================================================
// SegmentBase: read — resolving an offset to a location
// ===========================================================================

TEST_CASE("Every offset resolves to the exact byte its batch begins at") {
    TempDir dir("seg-read-exact");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    // Record the true start of every batch, then check read() finds each one.
    vector<uint64_t> starts;
    for (int i = 0; i < 50; ++i) {
        starts.push_back(segment->sizeBytes());
        const Offset base = segment->nextOffset();
        auto bytes = makeBatch(base, 1000 + i, 48);
        segment->append(base, bytes);
    }

    // Spans offsets whose index entry is right beside them and offsets whose
    // nearest entry is most of an interval away — the case the forward scan
    // exists for.
    for (int i = 0; i < 50; ++i) {
        const auto range = segment->read(Offset(i), kBigFetch);
        CHECK(range.position == starts[static_cast<size_t>(i)]);
        CHECK(range.length > 0);
        CHECK(range.fd == segment->fd());
    }
}

TEST_CASE("An offset inside a multi-record batch resolves to the batch's start") {
    TempDir dir("seg-read-mid-batch");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(500), testPolicy());

    auto batch = makeBatch(Offset(500), 1000, 16, 5);   // offsets 500..504
    segment->append(Offset(500), batch);

    // A batch is the unit of transfer, so all five offsets get the same range.
    // The consumer skips the records ahead of the one it asked for.
    for (int64_t offset = 500; offset <= 504; ++offset) {
        const auto range = segment->read(Offset(offset), kBigFetch);
        CHECK(range.position == 0);
        CHECK(range.length == batch.size());
    }
}

TEST_CASE("A read works with no index entries at all") {
    TempDir dir("seg-read-no-index");
    auto    policy = testPolicy();
    policy.indexIntervalBytes = 1 << 20;   // nothing here will reach an interval
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), policy);
    fillSegment(*segment, 6, 32);

    // The low-traffic case. lookup() falls back to the front of the file and the
    // scan does all the work — slower, but correct, and it is why lookup() never
    // returns an optional.
    REQUIRE(segment->index().isEmpty());
    CHECK(segment->read(Offset(0), kBigFetch).position == 0);
    CHECK(segment->read(Offset(5), kBigFetch).position > 0);
}

TEST_CASE("A caught-up read succeeds with zero bytes") {
    TempDir dir("seg-read-caught-up");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 4);

    // The most common read in the system: a consumer polling for an offset the
    // producer has not written yet. Not an error, and it must not cost one.
    const auto range = segment->read(Offset(4), kBigFetch);
    CHECK(range.empty());
    CHECK(range.length == 0);
    CHECK(range.position == segment->sizeBytes());

    // Well past the end behaves the same way.
    CHECK(segment->read(Offset(9999), kBigFetch).empty());

    // And an empty segment is caught up at its own base offset.
    auto fresh = ActiveSegment::create(dir.file(""), Offset(77), testPolicy());
    CHECK(fresh->read(Offset(77), kBigFetch).empty());
}

TEST_CASE("A read below the segment's base offset is a routing bug") {
    TempDir dir("seg-read-below-base");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(100), testPolicy());
    fillSegment(*segment, 3);

    // Log picked this segment; asking it for an offset it cannot hold means the
    // picking was wrong. That is a bug in Log, not a routine miss, so unlike the
    // caught-up case it throws.
    CHECK_THROWS_AS(segment->read(Offset(99), kBigFetch), OffsetInvariantViolated);
    CHECK_THROWS_AS(segment->read(Offset(0), kBigFetch), OffsetInvariantViolated);
}

TEST_CASE("A read never describes bytes beyond what was written") {
    TempDir dir("seg-read-in-bounds");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 30, 64);

    for (int64_t offset = 0; offset < 30; ++offset) {
        const auto range = segment->read(Offset(offset), kBigFetch);
        CHECK(range.position + range.length <= segment->sizeBytes());
    }
}

// ===========================================================================
// SegmentBase: read — fetch size
// ===========================================================================

TEST_CASE("A read is capped at the fetch size") {
    TempDir dir("seg-read-cap");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    const uint64_t total = fillSegment(*segment, 40, 64);

    const auto range = segment->read(Offset(0), 500);
    CHECK(range.position == 0);
    CHECK(range.length == 500);
    CHECK(range.length < total);
}

TEST_CASE("A read always carries at least one whole batch") {
    TempDir dir("seg-read-min-batch");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    auto batch = makeBatch(Offset(0), 1000, 512);
    segment->append(Offset(0), batch);
    REQUIRE(batch.size() > 1);

    // Without this rule a consumer whose fetch size is smaller than the next
    // batch polls forever and never advances — a permanent stall caused by a
    // merely conservative setting.
    CHECK(segment->read(Offset(0), 1).length == batch.size());
    CHECK(segment->read(Offset(0), 0).length == batch.size());
    CHECK(segment->read(Offset(0), batch.size() - 1).length == batch.size());

    // At exactly the batch size, and above it, nothing changes: there is only
    // one batch to give.
    CHECK(segment->read(Offset(0), batch.size()).length == batch.size());
    CHECK(segment->read(Offset(0), batch.size() * 10).length == batch.size());
}

TEST_CASE("A read never promises bytes that have not been written") {
    TempDir dir("seg-read-available");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    const uint64_t total = fillSegment(*segment, 5, 32);

    // A fetch size far larger than the segment must be clamped to what exists,
    // or sendfile would be handed a range running past the end of the file.
    const auto range = segment->read(Offset(0), 1 << 30);
    CHECK(range.position == 0);
    CHECK(range.length == total);

    // Same from a later offset: the cap is bytes-from-here, not bytes-in-total.
    const auto later = segment->read(Offset(3), 1 << 30);
    CHECK(later.position + later.length == total);
}

TEST_CASE("A capped read starts on a boundary even though it may end mid-batch") {
    TempDir dir("seg-read-mid-end");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    vector<uint64_t> starts;
    for (int i = 0; i < 20; ++i) {
        starts.push_back(segment->sizeBytes());
        const Offset base = segment->nextOffset();
        auto bytes = makeBatch(base, 1000 + i, 48);
        segment->append(base, bytes);
    }

    // The contract: the START is always a batch boundary, so a consumer can
    // always begin parsing. The END may not be, and the consumer discards an
    // incomplete trailing batch — the same thing Kafka's clients do.
    for (int i = 0; i < 20; ++i) {
        const auto range = segment->read(Offset(i), 300);
        CHECK(range.position == starts[static_cast<size_t>(i)]);
        CHECK(range.length >= 1);
        CHECK(range.position + range.length <= segment->sizeBytes());
    }
}

// ===========================================================================
// ActiveSegment: rolling
// ===========================================================================

namespace {

int64_t testNowMs() {
    return chrono::duration_cast<chrono::milliseconds>(
               chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

TEST_CASE("An empty segment never rolls, however old the clock claims it is") {
    TempDir dir("seg-roll-empty");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());

    // Rolling an empty segment would seal an empty file and create another one,
    // so an idle partition would grow a file per maintenance sweep, forever.
    CHECK_FALSE(segment->shouldRoll(0));
    CHECK_FALSE(segment->shouldRoll(testNowMs()));
    CHECK_FALSE(segment->shouldRoll(testNowMs() + segment->policy().maxSegmentAgeMs * 100));
}

TEST_CASE("A segment with room left does not roll") {
    TempDir dir("seg-roll-not-yet");
    auto    segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
    fillSegment(*segment, 3, 32);

    CHECK_FALSE(segment->shouldRoll(testNowMs()));
}

TEST_CASE("A segment rolls once it reaches its size limit") {
    TempDir dir("seg-roll-size");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 800;
    auto    segment        = ActiveSegment::create(dir.file(""), Offset(0), policy);

    fillSegment(*segment, 2, 32);
    REQUIRE(segment->sizeBytes() < 800);
    CHECK_FALSE(segment->shouldRoll(testNowMs()));

    fillSegment(*segment, 20, 64);
    CHECK(segment->sizeBytes() >= 800);
    CHECK(segment->shouldRoll(testNowMs()));
}

TEST_CASE("A segment rolls once it is older than the age limit") {
    TempDir dir("seg-roll-age");
    auto    policy         = testPolicy();
    policy.maxSegmentAgeMs = 5000;
    auto    segment        = ActiveSegment::create(dir.file(""), Offset(0), policy);
    fillSegment(*segment, 1);

    const int64_t now = testNowMs();
    CHECK_FALSE(segment->shouldRoll(now));

    // Time is a parameter, so this needs no sleeping. Without age-based rolling
    // a partition receiving one record a day would keep the same active segment
    // for years, and retention only ever deletes sealed segments.
    CHECK(segment->shouldRoll(now + 5000));
    CHECK(segment->shouldRoll(now + 100000));
}

TEST_CASE("Age is measured from the first append, not from creation") {
    TempDir dir("seg-roll-age-origin");
    auto    policy         = testPolicy();
    policy.maxSegmentAgeMs = 5000;
    auto    segment        = ActiveSegment::create(dir.file(""), Offset(0), policy);

    // Pretend the file was created long ago and has only just received data. It
    // must not be instantly stale — otherwise a segment that sat empty for a
    // week would roll on its very first record.
    const int64_t now = testNowMs();
    CHECK_FALSE(segment->shouldRoll(now + 1000000));   // still empty

    fillSegment(*segment, 1);
    CHECK_FALSE(segment->shouldRoll(now));
}

TEST_CASE("A full index forces a roll even with size and age to spare") {
    TempDir dir("seg-roll-index-full");
    auto    policy            = testPolicy();
    policy.maxIndexBytes      = OffsetIndex::kEntrySize * 2;   // two entries
    policy.indexIntervalBytes = 128;
    policy.maxSegmentBytes    = 1ull << 30;
    policy.maxSegmentAgeMs    = 1ll << 40;
    auto    segment           = ActiveSegment::create(dir.file(""), Offset(0), policy);

    fillSegment(*segment, 2, 32);
    CHECK_FALSE(segment->index().isFull());
    CHECK_FALSE(segment->shouldRoll(testNowMs()));

    fillSegment(*segment, 20, 64);

    // Once entries start being dropped, read()'s forward scan and the walk that
    // reopens a sealed segment both stop being bounded by one index interval.
    CHECK(segment->index().isFull());
    CHECK(segment->shouldRoll(testNowMs()));
}

// ===========================================================================
// SealedSegment: opening from disk
// ===========================================================================

TEST_CASE("A segment reopened from disk agrees with the one that wrote it") {
    TempDir dir("seg-open");
    const auto logFile = segmentLogPath(dir.file(""), Offset(700));

    Offset   next{0};
    uint64_t size = 0;
    int64_t  largest = 0;
    {
        auto segment = ActiveSegment::create(dir.file(""), Offset(700), testPolicy());
        fillSegment(*segment, 25, 64);
        next    = segment->nextOffset();
        size    = segment->sizeBytes();
        largest = segment->largestTimestamp();
    }   // closed

    auto sealed = SealedSegment::open(logFile);

    // None of these were written down anywhere — all three are derived by
    // reading the log, because the .log file is the only authority on what a
    // segment contains.
    CHECK(sealed->baseOffset() == Offset(700));
    CHECK(sealed->nextOffset() == next);
    CHECK(sealed->sizeBytes() == size);
    CHECK(sealed->largestTimestamp() == largest);
    CHECK_FALSE(sealed->isEmpty());
}

TEST_CASE("A reopened segment resolves every offset it holds") {
    TempDir dir("seg-open-reads");
    const auto logFile = segmentLogPath(dir.file(""), Offset(0));

    vector<uint64_t> starts;
    {
        auto segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
        for (int i = 0; i < 30; ++i) {
            starts.push_back(segment->sizeBytes());
            const Offset base = segment->nextOffset();
            auto bytes = makeBatch(base, 2000 + i, 48);
            segment->append(base, bytes);
        }
    }

    auto sealed = SealedSegment::open(logFile);
    for (int i = 0; i < 30; ++i)
        CHECK(sealed->read(Offset(i), kBigFetch).position == starts[static_cast<size_t>(i)]);

    CHECK(sealed->read(Offset(30), kBigFetch).empty());
}

TEST_CASE("A reopened segment with no index entries still derives its end state") {
    TempDir dir("seg-open-no-index");
    const auto logFile = segmentLogPath(dir.file(""), Offset(0));
    auto policy = testPolicy();
    policy.indexIntervalBytes = 1 << 20;   // never reaches an interval

    {
        auto segment = ActiveSegment::create(dir.file(""), Offset(0), policy);
        fillSegment(*segment, 5, 32);
    }

    // The walk starts at the front of the file, which is still bounded: an index
    // only stays empty while the segment holds less than one interval of data.
    auto sealed = SealedSegment::open(logFile);
    CHECK(sealed->index().isEmpty());
    CHECK(sealed->nextOffset() == Offset(5));
}

TEST_CASE("A reopened empty segment reports itself empty") {
    TempDir dir("seg-open-empty");
    const auto logFile = segmentLogPath(dir.file(""), Offset(42));
    { auto segment = ActiveSegment::create(dir.file(""), Offset(42), testPolicy()); (void)segment; }

    auto sealed = SealedSegment::open(logFile);
    CHECK(sealed->isEmpty());
    CHECK(sealed->nextOffset() == Offset(42));
    CHECK(sealed->sizeBytes() == 0);
    CHECK(sealed->largestTimestamp() == -1);
}

TEST_CASE("Opening a segment whose index is missing is refused") {
    TempDir dir("seg-open-no-index-file");
    const auto logFile   = segmentLogPath(dir.file(""), Offset(0));
    const auto indexFile = segmentIndexPath(dir.file(""), Offset(0));

    {
        auto segment = ActiveSegment::create(dir.file(""), Offset(0), testPolicy());
        fillSegment(*segment, 3);
    }
    filesystem::remove(indexFile);

    // Nothing in normal operation deletes an index, so its absence means the
    // directory was tampered with or a delete was interrupted. Rebuilding it
    // would mean a full scan of a segment we are otherwise trusting.
    CHECK_THROWS_AS(SealedSegment::open(logFile), IoError);
}

TEST_CASE("Opening something that is not a segment file is refused") {
    TempDir dir("seg-open-bad-name");
    CHECK_THROWS_AS(SealedSegment::open(dir.file("partition.meta")), CorruptData);
}
