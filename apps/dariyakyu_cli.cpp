// dariyakyu-cli — talk to a broker.
//
//   dariyakyu-cli create   <topic> [--partitions N] [--broker host:port]
//   dariyakyu-cli describe [topic]
//   dariyakyu-cli produce  <topic> <partition>            records from stdin, one per line
//   dariyakyu-cli consume  <topic> <partition> [--from earliest|latest|N] [--follow]
//   dariyakyu-cli dump-segment <partition-dir>            no broker needed
//
// A real client, not test scaffolding: it links the same protocol library the
// broker does, so a field the encoder writes and the decoder ignores shows up the
// first time these two talk.

#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "cli/client.hpp"
#include "cli/dump.hpp"
#include "common/errors.hpp"
#include "protocol/create_topic.hpp"
#include "protocol/fetch.hpp"
#include "protocol/list_offsets.hpp"
#include "protocol/metadata.hpp"
#include "protocol/produce.hpp"
#include "storage/record_batch.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::protocol;

namespace {

struct Options {
    string host       = "127.0.0.1";
    int32_t port      = 9092;
    int32_t partitions = 1;
    string  from      = "earliest";
    bool    follow    = false;
};

string valueOf(const vector<string>& args, const string& name, const string& fallback) {
    for (size_t i = 0; i + 1 < args.size(); ++i)
        if (args[i] == name) return args[i + 1];
    return fallback;
}

bool hasFlag(const vector<string>& args, const string& name) {
    for (const auto& arg : args)
        if (arg == name) return true;
    return false;
}

Options parseOptions(const vector<string>& args) {
    Options options;

    const string broker = valueOf(args, "--broker", "127.0.0.1:9092");
    const size_t colon  = broker.rfind(':');
    if (colon == string::npos) throw Error("--broker wants host:port, got '" + broker + "'");
    options.host = broker.substr(0, colon);
    options.port = stoi(broker.substr(colon + 1));

    options.partitions = stoi(valueOf(args, "--partitions", "1"));
    options.from       = valueOf(args, "--from", "earliest");
    options.follow     = hasFlag(args, "--follow");
    return options;
}

vector<uint8_t> encodedBody(const auto& request, auto encoder) {
    BufferWriter out;
    encoder(out, request);
    return out.take();
}

// Reports a partition-level error and says whether the caller should stop.
bool reportError(ErrorCode error, const string& what) {
    if (error == ErrorCode::None) return false;
    fprintf(stderr, "%s: %s\n", what.c_str(), describe(error));
    return true;
}

int doCreate(cli::Client& client, const string& topic, const Options& options) {
    CreateTopicRequest request;
    request.topics.push_back({topic, options.partitions, {}, {}, {}});

    const auto   body = client.call(ApiKey::CreateTopic, encodedBody(request, encodeCreateTopicRequest));
    BufferReader in(body);
    const auto   response = decodeCreateTopicResponse(in);

    for (const auto& answer : response.topics) {
        if (reportError(answer.error, "create " + answer.name)) return 1;
        printf("created %s with %d partition(s)\n", answer.name.c_str(), options.partitions);
    }
    return 0;
}

int doDescribe(cli::Client& client, const string& topic) {
    MetadataRequest request;
    if (topic.empty()) request.allTopics = true;
    else request.topics.push_back(topic);

    const auto   body = client.call(ApiKey::Metadata, encodedBody(request, encodeMetadataRequest));
    BufferReader in(body);
    const auto   metadata = decodeMetadataResponse(in);

    printf("brokers:\n");
    for (const auto& broker : metadata.brokers)
        printf("  %d  %s:%d%s\n", broker.nodeId, broker.host.c_str(), broker.port,
               broker.nodeId == metadata.controllerId ? "  (controller)" : "");

    if (metadata.topics.empty()) {
        printf("\nno topics\n");
        return 0;
    }

    // The offsets are a second request. Worth it: "which partitions exist" and
    // "what is in them" are the two things anyone describing a topic wants, and a
    // client asking one almost always wants the other.
    ListOffsetsRequest offsets;
    for (const auto& answer : metadata.topics) {
        if (answer.error != ErrorCode::None) continue;
        ListOffsetsRequest::Topic asked;
        asked.name = answer.name;
        for (const auto& partition : answer.partitions) {
            asked.partitions.push_back({partition.partition, kEarliestTimestamp});
            asked.partitions.push_back({partition.partition, kLatestTimestamp});
        }
        offsets.topics.push_back(asked);
    }

    ListOffsetsResponse bounds;
    if (!offsets.topics.empty()) {
        const auto   offsetBody = client.call(ApiKey::ListOffsets,
                                              encodedBody(offsets, encodeListOffsetsRequest));
        BufferReader offsetIn(offsetBody);
        bounds = decodeListOffsetsResponse(offsetIn);
    }

    for (size_t t = 0; t < metadata.topics.size(); ++t) {
        const auto& answer = metadata.topics[t];
        printf("\n%s", answer.name.c_str());
        if (answer.error != ErrorCode::None) {
            printf("  [%s]\n", describe(answer.error));
            continue;
        }
        printf("\n");

        for (size_t p = 0; p < answer.partitions.size(); ++p) {
            printf("  partition %-4d leader %d", answer.partitions[p].partition,
                   answer.partitions[p].leader);
            if (t < bounds.topics.size() && 2 * p + 1 < bounds.topics[t].partitions.size()) {
                const auto& earliest = bounds.topics[t].partitions[2 * p];
                const auto& latest   = bounds.topics[t].partitions[2 * p + 1];
                printf("   offsets %lld..%lld  (%lld record(s))",
                       (long long)earliest.offset.value(), (long long)latest.offset.value(),
                       (long long)(latest.offset - earliest.offset));
            }
            printf("\n");
        }
    }
    return 0;
}

int doProduce(cli::Client& client, const string& topic, PartitionId partition) {
    string line;
    int    sent = 0;

    // One batch per line, sent immediately. A real producer would accumulate for
    // linger.ms and send bigger batches — that is client policy, and it is banked
    // rather than built because a terminal wants to see each line land.
    while (getline(cin, line)) {
        storage::RecordBatchBuilder builder;
        builder.append(storage::wallClockMillis(), nullopt,
                       span<const uint8_t>(reinterpret_cast<const uint8_t*>(line.data()),
                                           line.size()));
        auto batch = builder.build();

        ProduceRequest request;
        request.topics.push_back({topic, {{partition, span<const uint8_t>(batch), 0}}});

        const auto   body = client.call(ApiKey::Produce, encodedBody(request, encodeProduceRequest));
        BufferReader in(body);
        const auto   response = decodeProduceResponse(in);

        const auto& answer = response.topics.at(0).partitions.at(0);
        if (reportError(answer.error, topic + "-" + to_string(partition))) return 1;

        printf("%lld\n", (long long)answer.baseOffset.value());
        fflush(stdout);
        ++sent;
    }

    fprintf(stderr, "produced %d record(s)\n", sent);
    return 0;
}

Offset resolveStart(cli::Client& client, const string& topic, PartitionId partition,
                    const string& from) {
    if (from != "earliest" && from != "latest") return Offset{stoll(from)};

    ListOffsetsRequest request;
    request.topics.push_back(
        {topic, {{partition, from == "earliest" ? kEarliestTimestamp : kLatestTimestamp}}});

    const auto   body = client.call(ApiKey::ListOffsets,
                                    encodedBody(request, encodeListOffsetsRequest));
    BufferReader in(body);
    const auto   response = decodeListOffsetsResponse(in);

    const auto& answer = response.topics.at(0).partitions.at(0);
    if (answer.error != ErrorCode::None)
        throw Error(string("cannot resolve a starting offset: ") + describe(answer.error));
    return answer.offset;
}

int doConsume(cli::Client& client, const string& topic, PartitionId partition,
              const Options& options) {
    Offset offset = resolveStart(client, topic, partition, options.from);

    while (true) {
        FetchRequest request;
        request.topics.push_back({topic, {{partition, offset, 1 << 20}}});

        const auto   body = client.call(ApiKey::Fetch, encodedBody(request, encodeFetchRequest));
        BufferReader in(body);
        const auto   response = decodeFetchResponse(in);
        const auto&  answer   = response.topics.at(0).partitions.at(0);

        if (answer.error == ErrorCode::OffsetOutOfRange) {
            // The two offsets the response carries are how a client tells "you
            // were too slow" from "you asked for the future" — the distinction
            // the single wire code collapses.
            if (offset < answer.logStartOffset) {
                fprintf(stderr, "offset %lld has been deleted; resuming at %lld\n",
                        (long long)offset.value(), (long long)answer.logStartOffset.value());
                offset = answer.logStartOffset;
                continue;
            }
            fprintf(stderr, "offset %lld is past the end of the log (%lld)\n",
                    (long long)offset.value(), (long long)answer.highWatermark.value());
            return 1;
        }
        if (reportError(answer.error, topic + "-" + to_string(partition))) return 1;

        if (answer.records.empty()) {
            if (!options.follow) return 0;   // caught up, and not waiting
            ::usleep(200 * 1000);
            continue;
        }

        // A response may end mid-batch — the fetch contract — so an incomplete
        // trailing batch is discarded rather than treated as damage.
        size_t position = 0;
        while (position < answer.records.size()) {
            const auto remaining = answer.records.subspan(position);
            size_t     total     = 0;
            try {
                total = storage::RecordBatch::totalSizeOf(remaining);
            } catch (const CorruptData&) {
                break;
            }
            if (total > remaining.size()) break;

            const auto batch  = remaining.subspan(0, total);
            const auto header = storage::RecordBatch::parseHeader(batch);
            for (const auto& record : storage::RecordBatch::decodeRecords(batch)) {
                printf("%lld\t%.*s\n", (long long)record.offsetFrom(header.baseOffset).value(),
                       record.value ? static_cast<int>(record.value->size()) : 0,
                       record.value ? reinterpret_cast<const char*>(record.value->data()) : "");
            }
            offset   = header.lastOffset() + 1;
            position += total;
        }
        fflush(stdout);
    }
}

void usage() {
    fprintf(stderr,
            "usage:\n"
            "  dariyakyu-cli create   <topic> [--partitions N] [--broker host:port]\n"
            "  dariyakyu-cli describe [topic] [--broker host:port]\n"
            "  dariyakyu-cli produce  <topic> <partition> [--broker host:port]\n"
            "  dariyakyu-cli consume  <topic> <partition> [--from earliest|latest|N] "
            "[--follow] [--broker host:port]\n"
            "  dariyakyu-cli dump-segment <partition-dir>\n");
}

}  // namespace

int main(int argc, char** argv) {
    const vector<string> args(argv + 1, argv + argc);
    if (args.empty()) {
        usage();
        return 2;
    }

    const string& command = args[0];

    try {
        // The one command that needs no broker, which is exactly when it is
        // wanted: when the broker will not start.
        if (command == "dump-segment") {
            if (args.size() < 2) { usage(); return 2; }
            cli::inspectPartition(args[1]);
            return 0;
        }

        const Options options = parseOptions(args);
        cli::Client   client(options.host, options.port);

        if (command == "create") {
            if (args.size() < 2) { usage(); return 2; }
            return doCreate(client, args[1], options);
        }
        if (command == "describe") {
            const string topic = (args.size() > 1 && args[1].rfind("--", 0) != 0) ? args[1] : "";
            return doDescribe(client, topic);
        }
        if (command == "produce") {
            if (args.size() < 3) { usage(); return 2; }
            return doProduce(client, args[1], stoi(args[2]));
        }
        if (command == "consume") {
            if (args.size() < 3) { usage(); return 2; }
            return doConsume(client, args[1], stoi(args[2]), options);
        }

        usage();
        return 2;
    } catch (const Error& error) {
        fprintf(stderr, "dariyakyu-cli: %s\n", error.what());
        return 1;
    }
}
