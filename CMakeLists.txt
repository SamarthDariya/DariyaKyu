cmake_minimum_required(VERSION 3.20)
project(dariyakyu LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

# For clangd / IDE navigation.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(DARIYAKYU_ASAN "Build with AddressSanitizer"        OFF)
option(DARIYAKYU_TSAN "Build with ThreadSanitizer"         OFF)

# ASan and TSan are mutually exclusive; TSan is the one that matters most here,
# since the whole storage design rests on claims about who may touch what
# concurrently (DESIGN.md decision 20).
if(DARIYAKYU_ASAN AND DARIYAKYU_TSAN)
  message(FATAL_ERROR "DARIYAKYU_ASAN and DARIYAKYU_TSAN cannot both be enabled")
endif()
if(DARIYAKYU_ASAN)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
endif()
if(DARIYAKYU_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=thread)
endif()

# ---------------------------------------------------------------------------
# Library: common
# ---------------------------------------------------------------------------

add_library(dariyakyu_common
  src/common/types.cpp
  src/common/errors.cpp
  src/common/file_handle.cpp
  src/common/mapped_file.cpp
  src/common/varint.cpp
  src/common/crc32c.cpp
  src/common/buffer.cpp
)

# Includes are rooted at src/, so every include reads "common/file_handle.hpp" —
# the layer a header belongs to is visible at the include site.
target_include_directories(dariyakyu_common PUBLIC src)
target_compile_options(dariyakyu_common PRIVATE -Wall -Wextra -Wpedantic)

# ---------------------------------------------------------------------------
# Library: storage
# ---------------------------------------------------------------------------
#
# Depends on common and nothing else. The layering is enforced by the build: if
# common ever tries to include from storage, it will not link.

add_library(dariyakyu_storage
  src/storage/record_batch.cpp
  src/storage/offset_index.cpp
  src/storage/segment.cpp
  src/storage/log.cpp
  src/storage/partition_meta.cpp
  src/storage/log_manager.cpp
)

target_link_libraries(dariyakyu_storage PUBLIC dariyakyu_common)
target_compile_options(dariyakyu_storage PRIVATE -Wall -Wextra -Wpedantic)

# ---------------------------------------------------------------------------
# Library: protocol
# ---------------------------------------------------------------------------
#
# The wire format and the request/response types. Depends on storage, because a
# response carries a FileRange and an error code is translated from a ReadError.
# Linked by both the broker and the CLI, so a field the encoder writes and the
# decoder ignores shows up the first time they talk.

add_library(dariyakyu_protocol
  src/protocol/error_codes.cpp
)

target_link_libraries(dariyakyu_protocol PUBLIC dariyakyu_storage)
target_compile_options(dariyakyu_protocol PRIVATE -Wall -Wextra -Wpedantic)

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
#
# dariyakyu-dump inspects a partition directory read-only. It exists so the
# storage layer can be looked at rather than only asserted about, and it becomes
# a dariyakyu-cli subcommand at M4.

add_executable(dariyakyu-dump apps/dariyakyu_dump.cpp)
target_link_libraries(dariyakyu-dump PRIVATE dariyakyu_storage)
target_compile_options(dariyakyu-dump PRIVATE -Wall -Wextra -Wpedantic)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

include(FetchContent)

# doctest is header-only, so we fetch the source and use the header directly
# rather than building doctest's own CMake project. SOURCE_SUBDIR names a
# directory with no CMakeLists.txt, which tells FetchContent_MakeAvailable to
# download without calling add_subdirectory().
#
# This is not only tidier — it is necessary. doctest v2.4.11 declares
# cmake_minimum_required(VERSION 3.0), and CMake 4 removed compatibility with
# anything below 3.5, so configuring its project fails outright. Skipping a
# build system we never needed is a better fix than forcing an old policy
# version across the whole build.
FetchContent_Declare(
  doctest
  GIT_REPOSITORY https://github.com/doctest/doctest.git
  GIT_TAG        v2.4.11
  SOURCE_SUBDIR  header-only-no-cmake-project
)
FetchContent_MakeAvailable(doctest)

add_library(doctest_headers INTERFACE)
target_include_directories(doctest_headers INTERFACE ${doctest_SOURCE_DIR})
add_library(doctest::doctest ALIAS doctest_headers)

enable_testing()

add_executable(common_tests tests/common_tests.cpp)
target_link_libraries(common_tests PRIVATE dariyakyu_common doctest::doctest)
target_compile_options(common_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME common_tests COMMAND common_tests)

add_executable(codec_tests tests/codec_tests.cpp)
target_link_libraries(codec_tests PRIVATE dariyakyu_storage doctest::doctest)
target_compile_options(codec_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME codec_tests COMMAND codec_tests)

# The storage suites share tests/test_support.hpp, so each needs tests/ on its
# include path. Split by layer rather than kept as one file: a single 4,000-line
# suite meant every change rebuilt and reran all of it.
foreach(suite index_tests segment_tests log_tests manager_tests protocol_tests)
  add_executable(${suite} tests/${suite}.cpp)
  target_link_libraries(${suite} PRIVATE dariyakyu_protocol doctest::doctest)
  target_include_directories(${suite} PRIVATE tests)
  target_compile_options(${suite} PRIVATE -Wall -Wextra -Wpedantic)
  add_test(NAME ${suite} COMMAND ${suite})
endforeach()
