// dariyakyu-broker — the broker.
//
//   dariyakyu-broker [--data DIR] [--host HOST] [--port N] [--advertise HOST]
//
// Runs until interrupted. SIGINT and SIGTERM stop it cleanly, which matters more
// than it sounds: an unclean stop leaves the active segment unflushed, so the
// next start pays for a crash-recovery scan it did not need.

#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/errors.hpp"
#include "server/broker.hpp"

using namespace std;
using namespace dariyakyu;

namespace {

// Written by a signal handler, so it must be a type the standard says is safe to
// touch there — nothing else is, not even a bool.
volatile sig_atomic_t stopRequested = 0;

void onSignal(int) {
    stopRequested = 1;
}

string optionValue(const vector<string>& args, const string& name, const string& fallback) {
    for (size_t i = 0; i + 1 < args.size(); ++i)
        if (args[i] == name) return args[i + 1];
    return fallback;
}

}  // namespace

int main(int argc, char** argv) {
    const vector<string> args(argv + 1, argv + argc);

    server::Broker::Options options;
    options.dataDir        = optionValue(args, "--data", "./data");
    options.host           = optionValue(args, "--host", "127.0.0.1");
    options.port           = stoi(optionValue(args, "--port", "9092"));
    options.advertisedHost = optionValue(args, "--advertise", "");

    try {
        server::Broker broker(options);
        broker.start();

        printf("dariyakyu listening on %s:%d, data in %s\n", options.host.c_str(),
               broker.port(), options.dataDir.c_str());
        fflush(stdout);

        ::signal(SIGINT, onSignal);
        ::signal(SIGTERM, onSignal);

        // Polled rather than waited on, because a condition variable cannot be
        // signalled from a signal handler — almost nothing can. A second of
        // latency on shutdown is not worth the machinery to avoid.
        while (stopRequested == 0) ::sleep(1);

        printf("\nstopping\n");
        broker.stop();
        return 0;
    } catch (const Error& error) {
        fprintf(stderr, "dariyakyu-broker: %s\n", error.what());
        return 1;
    }
}
