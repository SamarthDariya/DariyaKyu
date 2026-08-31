#pragma once

#include <type_traits>

#include "common/types.hpp"

namespace dariyakyu::storage {

// What a read of a partition can go wrong in.
//
// A read has three outcomes and they are one integer apart from each other:
//
//   offset <  logStartOffset   aged out by retention   -> BelowLogStart
//   offset == logEndOffset     caught up, nothing new  -> None, zero bytes
//   offset >  logEndOffset     asking for the future   -> AboveLogEnd
//
// The middle row is why this is a returned value and not an exception. It is the
// most common read in the entire system: every caught-up consumer polls for an
// offset that does not exist yet, several times a second, forever. Throwing
// there would mean allocating and unwinding on the hot path for the normal case.
//
// The two error rows have to stay distinguishable, because a client's reset
// policy treats them differently: below the log start means "you are too slow,
// jump to the earliest available offset", while above the log end means the
// client is confused or the log was truncated under it by a failover — a
// situation that needs a different recovery, not a silent skip forward.
//
// Deliberately NOT a wire error code. Storage never learns what a protocol is;
// M4's request handler maps these to whatever the response format calls them
// (DESIGN.md decision 12).
enum class ReadError {
    None,
    BelowLogStart,
    AboveLogEnd,
};

// Where a read's bytes are, or why there are none.
//
// Note that success and "there are bytes" are different questions. A caught-up
// read is a SUCCESS carrying a zero-length range, so callers ask ok() to find
// out whether anything went wrong and range.empty() to find out whether there is
// anything to send.
struct ReadResult {
    ReadError error = ReadError::None;
    FileRange range{};

    bool ok() const { return error == ReadError::None; }
};

// Returned by value from the hot read path, so it must stay cheap to copy: no
// allocation, no destructor, small enough to travel in registers.
static_assert(std::is_trivially_copyable_v<ReadResult>,
              "ReadResult is returned by value on every fetch; it must not acquire anything "
              "that needs copying or destroying");

}  // namespace dariyakyu::storage
