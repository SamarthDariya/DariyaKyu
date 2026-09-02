#pragma once

#include <array>
#include <cstdint>
#include <type_traits>

#include "storage/log.hpp"

namespace dariyakyu::protocol {

// What a response tells a client went wrong.
//
// Numeric, because a client has to branch on it and a string is not something to
// branch on. int16 and Kafka-shaped, so a compatibility shim stays possible
// later (decision 19).
//
// Attached PER PARTITION, not per request. A fetch across twelve partitions where
// one has been reassigned returns eleven partitions' worth of data plus one
// error — failing wholesale would let a single moved partition stall every
// consumer that happened to batch it with others.
//
// Zero is success, and that is worth being deliberate about: it means a
// zero-filled response buffer reads as "fine", which is the wrong default. Every
// encoder writes this field explicitly rather than relying on a struct's
// initialiser.
enum class ErrorCode : std::int16_t {
    None                    = 0,
    Unknown                 = 1,
    OffsetOutOfRange        = 2,
    CorruptMessage          = 3,
    NotLeaderForPartition   = 4,
    UnknownTopicOrPartition = 5,
    TopicAlreadyExists      = 6,
    InvalidTopic            = 7,
    RequestTimedOut         = 8,
    UnsupportedVersion      = 9,
};

static_assert(std::is_same_v<std::underlying_type_t<ErrorCode>, std::int16_t>,
              "error codes go on the wire as int16; widening one silently changes the protocol");

// Every code, for exhaustive switches and for tests that must not silently skip
// a newly added one.
inline constexpr std::array<ErrorCode, 10> kAllErrorCodes{
    ErrorCode::None,          ErrorCode::Unknown,                 ErrorCode::OffsetOutOfRange,
    ErrorCode::CorruptMessage, ErrorCode::NotLeaderForPartition,  ErrorCode::UnknownTopicOrPartition,
    ErrorCode::TopicAlreadyExists, ErrorCode::InvalidTopic,       ErrorCode::RequestTimedOut,
    ErrorCode::UnsupportedVersion,
};

// The one mapping that is unambiguous, so the one that gets a function.
//
// Storage reports a read's outcome as a ReadError and never learns what a wire
// code is (decision 12). This is where that vocabulary is translated, and the
// only place it is.
//
// Note that BelowLogStart and AboveLogEnd collapse into one code. Storage keeps
// them apart because a client's reset policy genuinely differs — "you were too
// slow" versus "the log was truncated under you" — but Kafka's shape has one code
// here, and a client distinguishes them by comparing against the log start and
// end offsets the response also carries. Splitting them is banked in DESIGN.md.
ErrorCode errorCodeFor(storage::ReadError error);

// A short description, for the CLI and for test failure messages. Never sent on
// the wire: the code is the contract, and a client that needed the text would be
// parsing prose.
const char* describe(ErrorCode code);

// There is deliberately NO general errorCodeFor(const Error&).
//
// The same exception type means different things depending on which direction
// the bytes came from, so a single global mapping would be wrong:
//
//   CorruptData from a Produce  -> CorruptMessage. The client sent a batch that
//                                  fails its own checksum; that is its fault and
//                                  it should be told precisely.
//   CorruptData from a Fetch    -> Unknown. Our files are damaged. Telling the
//                                  client its message was corrupt would send it
//                                  chasing a bug it does not have.
//
// So each handler decides at its catch site, where the direction is known.
// OffsetInvariantViolated and IoError map to Unknown wherever they appear: the
// first is a broker bug and the second is a broker problem, and inventing a
// client-facing story for either is worse than admitting neither is actionable.

}  // namespace dariyakyu::protocol
