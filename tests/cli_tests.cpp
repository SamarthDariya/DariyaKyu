#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <optional>
#include <thread>
#include <string>
#include <vector>

#include "cli/client.hpp"
#include "protocol/create_topic.hpp"
#include "protocol/fetch.hpp"
#include "protocol/list_offsets.hpp"
#include "protocol/metadata.hpp"
#include "protocol/produce.hpp"
#include "server/broker.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::protocol;
using namespace dariyakyu::test;

// ===========================================================================
// M4 end to end: a real client against a real broker over a real socket
// ===========================================================================

namespace {

server::Broker::Options optionsFor(const filesystem::path& dataDir) {
    server::Broker::Options options;
    options.dataDir               = dataDir;
    options.defaults              = testConfig();
    options.host                  = "127.0.0.1";
    options.port                  = 0;   // the OS picks
    options.maintenanceIntervalMs = 1000;
    return options;
}

template <typename Request, typename Encoder>
vector<uint8_t> body(const Request& request, Encoder encoder) {
    BufferWriter out;
    encoder(out, request);
    return out.take();
}

// Everything the CLI does to send one line, minus the terminal.
Offset produceLine(cli::Client& client, const string& topic, PartitionId partition,
                   const string& line) {
    storage::RecordBatchBuilder builder;
    builder.append(storage::wallClockMillis(), nullopt,
                   span<const uint8_t>(reinterpret_cast<const uint8_t*>(line.data()),
                                       line.size()));
    auto batch = builder.build();

    ProduceRequest request;
    request.topics.push_back({topic, {{partition, span<const uint8_t>(batch), 0}}});

    const auto   response = client.call(ApiKey::Produce, body(request, encodeProduceRequest));
    BufferReader in(response);
    const auto   answer = decodeProduceResponse(in).topics.at(0).partitions.at(0);
    REQUIRE(answer.error == ErrorCode::None);
    return answer.baseOffset;
}

// And everything it does to print them back.
vector<string> consumeFrom(cli::Client& client, const string& topic, PartitionId partition,
                           Offset from) {
    FetchRequest request;
    request.topics.push_back({topic, {{partition, from, 1 << 20}}});

    const auto   response = client.call(ApiKey::Fetch, body(request, encodeFetchRequest));
    BufferReader in(response);
    const auto   answer = decodeFetchResponse(in).topics.at(0).partitions.at(0);
    REQUIRE(answer.error == ErrorCode::None);

    vector<string> values;
    size_t         position = 0;
    while (position < answer.records.size()) {
        const auto remaining = answer.records.subspan(position);
        const auto total     = storage::RecordBatch::totalSizeOf(remaining);
        if (total > remaining.size()) break;   // an incomplete trailing batch

        const auto batch = remaining.subspan(0, total);
        for (const auto& record : storage::RecordBatch::decodeRecords(batch))
            values.push_back(record.value ? string(reinterpret_cast<const char*>(
                                                       record.value->data()),
                                                   record.value->size())
                                          : string{});
        position += total;
    }
    return values;
}

}  // namespace

TEST_CASE("A client creates a topic, produces to it, and reads it back") {
    TempDir        dir("cli-end-to-end");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();

    cli::Client client("127.0.0.1", broker.port());

    CreateTopicRequest create;
    create.topics.push_back({"orders", 2, {}, {}, {}});
    {
        const auto   response = client.call(ApiKey::CreateTopic,
                                            body(create, encodeCreateTopicRequest));
        BufferReader in(response);
        CHECK(decodeCreateTopicResponse(in).topics.at(0).error == ErrorCode::None);
    }

    CHECK(produceLine(client, "orders", 0, "first order") == Offset(0));
    CHECK(produceLine(client, "orders", 0, "second order") == Offset(1));
    CHECK(produceLine(client, "orders", 0, "third order") == Offset(2));

    // Everything, then from the middle.
    CHECK(consumeFrom(client, "orders", 0, Offset(0)) ==
          vector<string>{"first order", "second order", "third order"});
    CHECK(consumeFrom(client, "orders", 0, Offset(1)) ==
          vector<string>{"second order", "third order"});

    // Partitions are separate logs, which is the whole reason they exist.
    CHECK(consumeFrom(client, "orders", 1, Offset(0)).empty());

    broker.stop();
}

TEST_CASE("A caught-up consumer gets an answer with nothing in it") {
    TempDir        dir("cli-caught-up");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    broker.logs().createPartition(TopicPartition{"orders", 0});

    cli::Client client("127.0.0.1", broker.port());
    produceLine(client, "orders", 0, "one");

    // The most common fetch in the system, over a real socket: success, no bytes,
    // poll again.
    CHECK(consumeFrom(client, "orders", 0, Offset(1)).empty());

    broker.stop();
}

TEST_CASE("Records survive the broker being restarted") {
    TempDir                 dir("cli-restart");
    const auto              options = optionsFor(dir.file("data"));
    vector<string>          expected;

    {
        server::Broker broker(options);
        broker.start();
        cli::Client client("127.0.0.1", broker.port());

        CreateTopicRequest create;
        create.topics.push_back({"orders", 1, {}, {}, {}});
        client.call(ApiKey::CreateTopic, body(create, encodeCreateTopicRequest));

        for (int i = 0; i < 25; ++i) {
            const string line = "line " + to_string(i);
            produceLine(client, "orders", 0, line);
            expected.push_back(line);
        }
        broker.stop();
    }

    // A second broker over the same directory. Everything below this line is
    // M0 through M3 being load-bearing: the startup scan finds the partition,
    // partition.meta supplies its config, and recovery trusts the sealed
    // segments while scanning only the newest.
    server::Broker restarted(options);
    restarted.start();
    cli::Client client("127.0.0.1", restarted.port());

    CHECK(consumeFrom(client, "orders", 0, Offset(0)) == expected);

    // And it is still writable, not just readable — a broker has to restart into
    // service.
    CHECK(produceLine(client, "orders", 0, "after the restart") == Offset(25));
    restarted.stop();
}

TEST_CASE("A client is told where the log begins and ends") {
    TempDir        dir("cli-list-offsets");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    broker.logs().createPartition(TopicPartition{"orders", 0});

    cli::Client client("127.0.0.1", broker.port());
    for (int i = 0; i < 4; ++i) produceLine(client, "orders", 0, "record " + to_string(i));

    ListOffsetsRequest request;
    request.topics.push_back(
        {"orders", {{0, kEarliestTimestamp}, {0, kLatestTimestamp}}});

    const auto   response = client.call(ApiKey::ListOffsets,
                                        body(request, encodeListOffsetsRequest));
    BufferReader in(response);
    const auto   answer = decodeListOffsetsResponse(in).topics.at(0);

    CHECK(answer.partitions.at(0).offset == Offset(0));
    CHECK(answer.partitions.at(1).offset == Offset(4));
    broker.stop();
}

TEST_CASE("Metadata names every partition, and a topic that is not there") {
    TempDir        dir("cli-metadata");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    for (PartitionId p = 0; p < 3; ++p)
        broker.logs().createPartition(TopicPartition{"orders", p});

    cli::Client     client("127.0.0.1", broker.port());
    MetadataRequest request;
    request.topics = {"orders", "missing"};

    const auto   response = client.call(ApiKey::Metadata, body(request, encodeMetadataRequest));
    BufferReader in(response);
    const auto   metadata = decodeMetadataResponse(in);

    REQUIRE(metadata.brokers.size() == 1);
    CHECK(metadata.brokers.at(0).port == broker.port());
    CHECK(metadata.topics.at(0).partitions.size() == 3);
    CHECK(metadata.topics.at(1).error == ErrorCode::UnknownTopicOrPartition);
    broker.stop();
}

TEST_CASE("One client's requests stay matched to its own responses") {
    TempDir        dir("cli-correlation");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    broker.logs().createPartition(TopicPartition{"orders", 0});

    cli::Client client("127.0.0.1", broker.port());

    // Fifty requests down one connection. Client::call checks the correlation id
    // rather than assuming — a mismatch means the stream desynchronised, and
    // finding out here beats decoding the next three answers against the wrong
    // questions.
    for (int i = 0; i < 50; ++i)
        CHECK(produceLine(client, "orders", 0, "record " + to_string(i)) == Offset(i));

    CHECK(consumeFrom(client, "orders", 0, Offset(0)).size() == 50);
    broker.stop();
}

TEST_CASE("Several clients share one broker") {
    TempDir        dir("cli-many-clients");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    for (PartitionId p = 0; p < 4; ++p)
        broker.logs().createPartition(TopicPartition{"orders", p});

    vector<thread> clients;
    for (PartitionId p = 0; p < 4; ++p) {
        clients.emplace_back([&, p] {
            cli::Client client("127.0.0.1", broker.port());
            for (int i = 0; i < 20; ++i)
                produceLine(client, "orders", p, "p" + to_string(p) + " r" + to_string(i));
        });
    }
    for (auto& client : clients) client.join();

    cli::Client reader("127.0.0.1", broker.port());
    for (PartitionId p = 0; p < 4; ++p) {
        const auto values = consumeFrom(reader, "orders", p, Offset(0));
        REQUIRE(values.size() == 20);
        CHECK(values.front() == "p" + to_string(p) + " r0");
        CHECK(values.back() == "p" + to_string(p) + " r19");
    }
    broker.stop();
}
