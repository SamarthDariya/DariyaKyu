#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>

namespace dariyakyu {

// Base for every error dariyakyu raises deliberately.
class Error : public std::runtime_error {
public:
    explicit Error(const std::string& what) : std::runtime_error(what) {}
};

// A syscall failed.
//
// The three facts about such a failure — what was attempted, on what, and why —
// are kept as data as well as being formatted into what(). Callers branch on
// them: LogManager treats ENOENT on the data directory as "first boot, create
// it" and EACCES as "refuse to start", and structured logging wants the fields
// rather than a sentence it would have to re-parse.
//
// The errno must be captured at the failure site: almost anything, including
// allocating the message string, can clobber it.
class IoError : public Error {
public:
    IoError(std::string operation, std::filesystem::path path, int errnoValue);

    const std::string&           operation() const { return operation_; }
    const std::filesystem::path& path() const { return path_; }
    int                          errnoValue() const { return errno_; }

private:
    std::string           operation_;
    std::filesystem::path path_;
    int                   errno_;
};

// An internal invariant about offsets was violated — a segment asked for an
// offset it claims to hold but does not, or an index entry pointing outside its
// own file.
//
// Deliberately NOT used for a consumer reading an aged-out or not-yet-written
// offset. That is routine, not exceptional: a caught-up consumer polls for an
// offset beyond the log end every few hundred milliseconds, forever, and it is
// the most common read in the system. Those outcomes come back as a
// storage::ReadResult carrying a ReadError, which the request handler maps to a
// wire error code (DESIGN.md decision 12).
class OffsetInvariantViolated : public Error {
public:
    using Error::Error;
};

// A stored file failed its checksum, or its header did not parse.
class CorruptData : public Error {
public:
    using Error::Error;
};

}  // namespace dariyakyu
