#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "common/errors.hpp"
#include "common/file_handle.hpp"
#include "common/mapped_file.hpp"
#include "common/types.hpp"

using namespace std;
using namespace dariyakyu;

namespace {

// Each test gets its own directory, removed when it goes out of scope. Tests
// that touch the filesystem are the ones most likely to leak state into each
// other, so isolation is worth the few lines.
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

vector<uint8_t> asBytes(const string& s) {
    return vector<uint8_t>(s.begin(), s.end());
}

}  // namespace

// The point of a strong Offset is what it makes impossible, so the guarantees
// are asserted at compile time rather than tested at runtime.
static_assert(!is_convertible_v<int64_t, Offset>,
              "a raw integer must not silently become an Offset — that is the bug this type "
              "exists to prevent");
static_assert(is_trivially_copyable_v<Offset>,
              "Log stores the high watermark in an atomic<Offset> (DESIGN.md decision 20)");
static_assert(atomic<Offset>::is_always_lock_free,
              "an atomic high watermark must not fall back to a mutex");

TEST_CASE("Offset compares and orders") {
    CHECK(Offset{5} == Offset{5});
    CHECK(Offset{4} < Offset{5});
    CHECK(Offset{6} >= Offset{5});
    CHECK(Offset{}.value() == 0);
    CHECK(kUnknownOffset.value() == -1);
}

TEST_CASE("Offset plus a record count is an Offset") {
    Offset base{100};
    CHECK((base + 5) == Offset{105});
    CHECK((base - 5) == Offset{95});

    base += 10;
    CHECK(base == Offset{110});
    ++base;
    CHECK(base == Offset{111});
}

TEST_CASE("Offset minus Offset is a distance, not a position") {
    // This is most of the reason the type exists: the gap between two positions
    // is a count, and counts are what get stored as relative offsets inside an
    // index entry and a record batch.
    const int64_t distance = Offset{4'500'180} - Offset{4'499'712};
    CHECK(distance == 468);
}

TEST_CASE("Offset works as an ordered and hashed key") {
    map<Offset, string> segments;   // Log keeps sealed segments exactly like this
    segments[Offset{0}]       = "first";
    segments[Offset{1048576}] = "second";
    segments[Offset{2097152}] = "third";

    // "greatest base offset <= target", the segment lookup from decision 11.
    auto it = segments.upper_bound(Offset{1500000});
    REQUIRE(it != segments.begin());
    --it;
    CHECK(it->second == "second");

    unordered_map<Offset, int> byOffset;
    byOffset[Offset{42}] = 1;
    CHECK(byOffset.at(Offset{42}) == 1);
}

TEST_CASE("IoError keeps its facts as data, not only as prose") {
    // LogManager branches on these: ENOENT on the data directory means "first
    // boot, create it", EACCES means "refuse to start".
    const IoError e("open", "/data/orders-3/00000000000000000000.log", ENOENT);
    CHECK(e.operation() == "open");
    CHECK(e.path().filename() == "00000000000000000000.log");
    CHECK(e.errnoValue() == ENOENT);

    const string message = e.what();
    CHECK(message.find("open(") != string::npos);
    CHECK(message.find("errno 2") != string::npos);   // the number, not just the prose
}

TEST_CASE("Every dariyakyu error is catchable through one project base") {
    // A request handler catches Error and knows the failure is ours — catching
    // std::exception there would also swallow bad_alloc.
    auto throwsCorrupt = [] { throw CorruptData("bad CRC at position 4096"); };
    CHECK_THROWS_AS(throwsCorrupt(), Error);

    auto throwsIo = [] { throw IoError("fsync", "/data/x.log", EIO); };
    CHECK_THROWS_AS(throwsIo(), Error);
    CHECK_THROWS_AS(throwsIo(), std::runtime_error);
}

TEST_CASE("An empty FileRange is a valid caught-up result") {
    // A consumer polling for an offset the producer has not written yet is the
    // most common read in the system. It is a success with zero bytes, never an
    // error and never an exception.
    const FileRange caughtUp{};
    CHECK(caughtUp.empty());
    CHECK(caughtUp.fd == -1);

    const FileRange found{7, 2'847'232, 4096};
    CHECK_FALSE(found.empty());
    CHECK(found.position == 2'847'232u);
    CHECK(found.length == 4096u);
}

TEST_CASE("TopicPartition renders as its on-disk directory name") {
    const TopicPartition tp{"orders", 3};
    CHECK(tp.toString() == "orders-3");
}

TEST_CASE("TopicPartition is usable as a hash key") {
    unordered_map<TopicPartition, int> logs;
    logs[TopicPartition{"orders", 0}] = 10;
    logs[TopicPartition{"orders", 1}] = 11;
    logs[TopicPartition{"payments", 0}] = 20;

    CHECK(logs.size() == 3);
    CHECK(logs.at(TopicPartition{"orders", 1}) == 11);
    CHECK(TopicPartition{"orders", 1} == TopicPartition{"orders", 1});
    CHECK_FALSE(TopicPartition{"orders", 1} == TopicPartition{"orders", 2});
}

TEST_CASE("FileHandle::append reports the position it wrote at") {
    TempDir dir("append-position");
    FileHandle f(dir.file("segment.log"), FileHandle::Mode::ReadWrite);

    const auto first  = asBytes("hello");
    const auto second = asBytes("world!");

    // The returned position is what the offset index stores, so it must be the
    // start of the write and not the end (DESIGN.md decision 10).
    CHECK(f.append(first) == 0);
    CHECK(f.append(second) == 5);
    CHECK(f.size() == 11);
}

TEST_CASE("FileHandle::readAt reads a byte range without moving a cursor") {
    TempDir dir("read-at");
    FileHandle f(dir.file("segment.log"), FileHandle::Mode::ReadWrite);
    f.append(asBytes("0123456789"));

    vector<uint8_t> out(4);
    CHECK(f.readAt(3, out) == 4);
    CHECK(string(out.begin(), out.end()) == "3456");

    // Reading the same range again gives the same bytes — pread keeps no state,
    // which is what lets many threads share one descriptor.
    CHECK(f.readAt(3, out) == 4);
    CHECK(string(out.begin(), out.end()) == "3456");
}

TEST_CASE("FileHandle::readAt returns short at end of file") {
    TempDir dir("short-read");
    FileHandle f(dir.file("segment.log"), FileHandle::Mode::ReadWrite);
    f.append(asBytes("abc"));

    vector<uint8_t> out(10);
    CHECK(f.readAt(1, out) == 2);
}

TEST_CASE("FileHandle reopens at the existing end of file") {
    TempDir dir("reopen");
    const auto path = dir.file("segment.log");

    {
        FileHandle f(path, FileHandle::Mode::ReadWrite);
        f.append(asBytes("first"));
    }

    // A restarting broker must continue the active segment rather than
    // overwrite it (DESIGN.md recovery).
    FileHandle reopened(path, FileHandle::Mode::ReadWrite);
    CHECK(reopened.size() == 5);
    CHECK(reopened.append(asBytes("second")) == 5);
    CHECK(reopened.size() == 11);
}

TEST_CASE("FileHandle::truncate drops the tail") {
    TempDir dir("truncate");
    FileHandle f(dir.file("segment.log"), FileHandle::Mode::ReadWrite);
    f.append(asBytes("keep-this-DROP-THIS"));

    // Recovery truncates the active segment at the first bad CRC, and failover
    // truncates to the new leader's log end (decisions 13 and 16).
    f.truncate(9);
    CHECK(f.size() == 9);

    vector<uint8_t> out(20);
    CHECK(f.readAt(0, out) == 9);
    CHECK(string(out.begin(), out.begin() + 9) == "keep-this");

    // Appends resume from the truncation point.
    CHECK(f.append(asBytes("!")) == 9);
}

TEST_CASE("FileHandle is move-only and closes exactly once") {
    TempDir dir("move");
    FileHandle a(dir.file("segment.log"), FileHandle::Mode::ReadWrite);
    a.append(asBytes("data"));
    const int fd = a.fd();

    FileHandle b(std::move(a));
    CHECK_FALSE(a.isOpen());
    CHECK(b.isOpen());
    CHECK(b.fd() == fd);
    CHECK(b.size() == 4);
}

TEST_CASE("FileHandle::CreateNew refuses to clobber an existing file") {
    TempDir dir("create-new");
    const auto path = dir.file("00000000000000000000.log");

    FileHandle first(path, FileHandle::Mode::CreateNew);
    // Rolling a segment must never reopen one that already exists — that would
    // silently append to old data under a new base offset.
    CHECK_THROWS_AS(FileHandle(path, FileHandle::Mode::CreateNew), IoError);
}

TEST_CASE("FileHandle reports a failed open with the path and errno") {
    CHECK_THROWS_AS(FileHandle("/definitely/not/a/real/path.log", FileHandle::Mode::ReadOnly),
                    IoError);
}

TEST_CASE("MappedFile writes are visible through the file") {
    TempDir dir("mmap-write");
    const auto path = dir.file("00000000000000000000.index");

    {
        MappedFile map(path, 4096);
        CHECK(map.size() == 4096);
        auto bytes = map.mutableBytes();
        bytes[0]   = 0xDE;
        bytes[1]   = 0xAD;
        map.flush();
    }

    // The mapping is gone; the bytes are not.
    FileHandle f(path, FileHandle::Mode::ReadOnly);
    vector<uint8_t> out(2);
    CHECK(f.readAt(0, out) == 2);
    CHECK(out[0] == 0xDE);
    CHECK(out[1] == 0xAD);
}

TEST_CASE("MappedFile preallocates to the full mapping length") {
    TempDir dir("mmap-prealloc");
    const auto path = dir.file("00000000000000000000.index");

    MappedFile map(path, 8192);

    // Index files are preallocated so appends never extend a live mapping;
    // touching a page beyond the file's end would raise SIGBUS.
    CHECK(filesystem::file_size(path) == 8192);
}

TEST_CASE("MappedFile::unmapAndTrim shrinks the file to the bytes used") {
    TempDir dir("mmap-trim");
    const auto path = dir.file("00000000000000000000.index");

    MappedFile map(path, 4096);
    auto bytes = map.mutableBytes();
    for (int i = 0; i < 24; ++i) bytes[i] = static_cast<uint8_t>(i);

    // Sealing a segment trims its preallocated index down to the three 8-byte
    // entries actually written.
    map.unmapAndTrim(24);
    CHECK_FALSE(map.isMapped());
    CHECK(filesystem::file_size(path) == 24);
}

TEST_CASE("MappedFile is move-only") {
    TempDir dir("mmap-move");
    MappedFile a(dir.file("idx"), 4096);
    a.mutableBytes()[0] = 0x42;

    MappedFile b(std::move(a));
    CHECK_FALSE(a.isMapped());
    CHECK(b.isMapped());
    CHECK(b.bytes()[0] == 0x42);
}

TEST_CASE("A read-only mapping refuses to be written or trimmed") {
    TempDir dir("mmap-readonly");
    const auto path = dir.file("00000000000000000000.index");

    {
        MappedFile writable(path, 64);
        writable.mutableBytes()[0] = 0x99;
        writable.unmapAndTrim(8);
    }

    // A sealed segment's index is immutable. Mapping it writable would let a
    // stray write corrupt it silently and would dirty pages for no reason.
    MappedFile sealed = MappedFile::openReadOnly(path);
    CHECK(sealed.isMapped());
    CHECK_FALSE(sealed.isWritable());
    CHECK(sealed.size() == 8);          // mapped in its entirety, post-trim
    CHECK(sealed.bytes()[0] == 0x99);

    CHECK_THROWS_AS(sealed.mutableBytes(), Error);
    CHECK_THROWS_AS(sealed.unmapAndTrim(4), Error);
}

TEST_CASE("An empty index file opens as an empty mapping, not an error") {
    TempDir dir("mmap-empty");
    const auto path = dir.file("00000000000000000000.index");

    // A segment that rolled on age with less than one index interval of data
    // has no index entries, so its trimmed index file is zero bytes. Refusing
    // that would fail to open a valid partition on any low-traffic topic.
    { MappedFile writable(path, 4096); writable.unmapAndTrim(0); }
    REQUIRE(filesystem::file_size(path) == 0);

    MappedFile empty = MappedFile::openReadOnly(path);
    CHECK_FALSE(empty.isMapped());       // no pages: there is nothing in the file
    CHECK(empty.size() == 0);
    CHECK(empty.bytes().empty());        // still safe to call
}

TEST_CASE("A zero-length writable mapping is a bug, not a state") {
    TempDir dir("mmap-zero-writable");
    // Asking to preallocate nothing is a programming error, unlike opening an
    // index that legitimately has no entries.
    CHECK_THROWS_AS(MappedFile(dir.file("idx"), 0), Error);
}

TEST_CASE("Trimming twice reports being unmapped, not being read-only") {
    TempDir dir("mmap-double-trim");
    MappedFile map(dir.file("idx"), 4096);
    map.unmapAndTrim(16);

    // unmap() clears writable_, so without an isMapped() check first this would
    // blame the wrong thing.
    REQUIRE_THROWS_AS(map.unmapAndTrim(8), Error);
    try {
        map.unmapAndTrim(8);
    } catch (const Error& e) {
        CHECK(string(e.what()).find("unmapped") != string::npos);
    }
}

TEST_CASE("Appending publishes size and bytes together") {
    // The single-writer / many-reader pattern the storage design rests on: one
    // appender releases writePosition_, readers acquire it. The release/acquire
    // pairing must guarantee that a reader observing the new size also sees the
    // bytes behind it — otherwise it reads zeroes out of a file it just
    // measured. Run this under -DDARIYAKYU_TSAN=ON for it to have teeth.
    TempDir dir("concurrent-append");
    FileHandle f(dir.file("segment.log"), FileHandle::Mode::ReadWrite);

    constexpr int kRecords    = 4000;
    constexpr size_t kRecordSize = 64;
    const vector<uint8_t> record(kRecordSize, 0xAB);

    atomic<bool> done{false};
    atomic<int>  tornReads{0};

    auto reader = [&] {
        vector<uint8_t> buf(kRecordSize);
        while (!done.load(memory_order_acquire)) {
            const uint64_t visible = f.size();
            if (visible < kRecordSize) continue;
            if (f.readAt(visible - kRecordSize, buf) != kRecordSize) { ++tornReads; continue; }
            for (uint8_t b : buf)
                if (b != 0xAB) { ++tornReads; break; }
        }
    };

    thread r1(reader), r2(reader);
    for (int i = 0; i < kRecords; ++i) f.append(record);
    done.store(true, memory_order_release);
    r1.join();
    r2.join();

    CHECK(tornReads.load() == 0);
    CHECK(f.size() == static_cast<uint64_t>(kRecords) * kRecordSize);
}
