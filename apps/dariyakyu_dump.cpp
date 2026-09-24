// dariyakyu-dump — look inside a partition directory.
//
//   dariyakyu-dump <partition-dir>
//   dariyakyu-dump --generate <dir> [records] [segBytes] [--compact]
//
// The same thing as `dariyakyu-cli dump-segment`, kept as its own binary because
// it is wanted exactly when the broker will not start — and a tool that needs the
// broker's library to load is one more thing that can fail at that moment.

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "cli/dump.hpp"
#include "common/errors.hpp"

using namespace std;
using namespace dariyakyu;

int main(int argc, char** argv) {
    const vector<string> args(argv + 1, argv + argc);

    try {
        if (!args.empty() && args[0] == "--generate") {
            if (args.size() < 2) {
                fprintf(stderr, "usage: dariyakyu-dump --generate <dir> [records] [segBytes] [--compact]\n");
                return 2;
            }
            const int      records = (args.size() > 2) ? stoi(args[2]) : 24;
            const uint64_t bytes   = (args.size() > 3) ? stoull(args[3]) : 700;
            const bool compact =
                find(args.begin(), args.end(), "--compact") != args.end();
            cli::generatePartition(args[1], records, bytes, compact);
            cli::inspectPartition(args[1]);
            return 0;
        }

        if (args.size() != 1) {
            fprintf(stderr,
                    "usage: dariyakyu-dump <partition-dir>\n"
                    "       dariyakyu-dump --generate <dir> [records] [segBytes] [--compact]\n");
            return 2;
        }

        cli::inspectPartition(args[0]);
        return 0;
    } catch (const Error& error) {
        fprintf(stderr, "dariyakyu-dump: %s\n", error.what());
        return 1;
    }
}
