#include "protocol/error_codes.hpp"

namespace dariyakyu::protocol {

ErrorCode errorCodeFor(storage::ReadError error) {
    switch (error) {
        case storage::ReadError::None: return ErrorCode::None;
        case storage::ReadError::BelowLogStart: return ErrorCode::OffsetOutOfRange;
        case storage::ReadError::AboveLogEnd: return ErrorCode::OffsetOutOfRange;
    }
    // No default label above, so adding a ReadError is a compiler warning here
    // rather than a silent fall-through to this line.
    return ErrorCode::Unknown;
}

const char* describe(ErrorCode code) {
    switch (code) {
        case ErrorCode::None: return "none";
        case ErrorCode::Unknown: return "unknown broker error";
        case ErrorCode::OffsetOutOfRange: return "offset out of range";
        case ErrorCode::CorruptMessage: return "corrupt message";
        case ErrorCode::NotLeaderForPartition: return "not leader for partition";
        case ErrorCode::UnknownTopicOrPartition: return "unknown topic or partition";
        case ErrorCode::TopicAlreadyExists: return "topic already exists";
        case ErrorCode::InvalidTopic: return "invalid topic name";
        case ErrorCode::RequestTimedOut: return "request timed out";
        case ErrorCode::UnsupportedVersion: return "unsupported api version";
    }
    // Reached only for a code off the wire that this build does not know, which a
    // client of a newer broker can genuinely produce.
    return "unrecognised error code";
}

}  // namespace dariyakyu::protocol
