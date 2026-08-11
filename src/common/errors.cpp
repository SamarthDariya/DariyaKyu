#include "common/errors.hpp"

#include <system_error>

using namespace std;

namespace dariyakyu {

namespace {

string describe(const string& operation, const filesystem::path& path, int errnoValue) {
    // generic_category().message() rather than strerror(): strerror may return a
    // pointer into a shared static buffer, so two threads formatting an error at
    // the same time can corrupt each other's message. This broker has an I/O
    // thread pool, so that is a real race and not a theoretical one.
    return operation + "(" + path.string() + ") failed: " +
           generic_category().message(errnoValue) + " (errno " + to_string(errnoValue) + ")";
}

}  // namespace

// Initialisation order is load-bearing: the base class is constructed first, so
// describe() reads the parameters while they are still intact, and only then are
// they moved into the members.
IoError::IoError(string operation, filesystem::path path, int errnoValue)
    : Error(describe(operation, path, errnoValue)),
      operation_(std::move(operation)),
      path_(std::move(path)),
      errno_(errnoValue) {}

}  // namespace dariyakyu
