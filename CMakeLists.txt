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
)

# Includes are rooted at src/, so every include reads "common/file_handle.hpp" —
# the layer a header belongs to is visible at the include site.
target_include_directories(dariyakyu_common PUBLIC src)
target_compile_options(dariyakyu_common PRIVATE -Wall -Wextra -Wpedantic)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

include(FetchContent)
FetchContent_Declare(
  doctest
  GIT_REPOSITORY https://github.com/doctest/doctest.git
  GIT_TAG        v2.4.11
)
FetchContent_MakeAvailable(doctest)

enable_testing()

add_executable(common_tests tests/common_tests.cpp)
target_link_libraries(common_tests PRIVATE dariyakyu_common doctest::doctest)
target_compile_options(common_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME common_tests COMMAND common_tests)
