#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <set>
#include <string>
#include <type_traits>

#include "protocol/error_codes.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::protocol;
using namespace dariyakyu::test;

// ===========================================================================
// Error codes
// ===========================================================================

TEST_CASE("Success is zero") {
    // Deliberate and worth pinning: it means a zero-filled buffer reads as
    // "fine", which is the wrong default — so every encoder writes this field
    // explicitly rather than trusting an initialiser.
    CHECK(static_cast<int16_t>(ErrorCode::None) == 0);
    CHECK(static_cast<int16_t>(ErrorCode::Unknown) != 0);
}

TEST_CASE("Every code has a distinct number") {
    // The numbers are the contract. Two enumerators sharing one would make a
    // client's branch silently wrong, and nothing else would notice.
    set<int16_t> seen;
    for (const ErrorCode code : kAllErrorCodes) seen.insert(static_cast<int16_t>(code));
    CHECK(seen.size() == kAllErrorCodes.size());
}

TEST_CASE("Every code has a description") {
    // kAllErrorCodes exists so this cannot silently skip a newly added code.
    for (const ErrorCode code : kAllErrorCodes) {
        const string text = describe(code);
        CHECK_FALSE(text.empty());
        CHECK(text != "unrecognised error code");
    }
}

TEST_CASE("A code from a newer broker still describes itself") {
    // A client of a newer broker can genuinely receive a code this build has
    // never heard of. Better a placeholder than a crash or an empty string.
    const auto fromTheFuture = static_cast<ErrorCode>(9999);
    CHECK(string(describe(fromTheFuture)) == "unrecognised error code");
}

TEST_CASE("A read's outcome translates to a wire code") {
    using storage::ReadError;

    CHECK(errorCodeFor(ReadError::None) == ErrorCode::None);
    CHECK(errorCodeFor(ReadError::BelowLogStart) == ErrorCode::OffsetOutOfRange);
    CHECK(errorCodeFor(ReadError::AboveLogEnd) == ErrorCode::OffsetOutOfRange);
}

TEST_CASE("The two out-of-range cases collapse into one code") {
    using storage::ReadError;

    // Storage keeps them apart because a client's reset policy differs — "you
    // were too slow" versus "the log was truncated under you". The wire has one
    // code, Kafka-shaped, and a client tells them apart by comparing against the
    // log start and end offsets the response also carries.
    CHECK(errorCodeFor(ReadError::BelowLogStart) == errorCodeFor(ReadError::AboveLogEnd));

    // Which is exactly why the storage-level distinction must not be lost.
    CHECK(ReadError::BelowLogStart != ReadError::AboveLogEnd);
}

TEST_CASE("A real read's outcome maps end to end") {
    TempDir dir("proto-error-mapping");
    auto    log = storage::Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"),
                                       testConfig());
    auto bytes = makeUnstampedBatch(1000, 48);
    log->append(bytes);

    // Not a hand-built enum value — the outcomes a Log actually produces.
    CHECK(errorCodeFor(log->read(Offset(0), kBigFetch).error) == ErrorCode::None);
    CHECK(errorCodeFor(log->read(Offset(1), kBigFetch).error) == ErrorCode::None);  // caught up
    CHECK(errorCodeFor(log->read(Offset(2), kBigFetch).error) == ErrorCode::OffsetOutOfRange);
}
