#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <optional>
#include <thread>
#include <string>
#include <vector>

#include <set>

#include "cli/assignor.hpp"
#include "cli/client.hpp"
#include "cli/group_consumer.hpp"
#include "protocol/create_topic.hpp"
#include "protocol/fetch.hpp"
#include "protocol/group_apis.hpp"
#include "protocol/list_offsets.hpp"
#include "protocol/metadata.hpp"
#include "protocol/produce.hpp"
#include "server/broker.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::protocol;
using namespace dariyakyu::cli;
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

// ===========================================================================
// Assignors
// ===========================================================================

namespace {

AssignmentInput inputFor(const vector<string>& members, const vector<string>& topics,
                         const map<string, int32_t>& counts) {
    AssignmentInput input;
    for (const auto& member : members) input.members[member] = topics;
    input.partitionCounts = counts;
    return input;
}

size_t totalAssigned(const map<string, vector<TopicPartition>>& assignment) {
    size_t total = 0;
    for (const auto& [member, partitions] : assignment) total += partitions.size();
    return total;
}

}  // namespace

TEST_CASE("An assignment round-trips through its opaque bytes") {
    const vector<TopicPartition> assigned{{"orders", 0}, {"orders", 2}, {"payments", 1}};
    const auto                   decoded = decodeAssignment(encodeAssignment(assigned));

    CHECK(decoded.size() == 3);
    CHECK(decoded[0] == TopicPartition{"orders", 0});
    CHECK(decoded[1] == TopicPartition{"orders", 2});
    CHECK(decoded[2] == TopicPartition{"payments", 1});
}

TEST_CASE("An empty assignment is legal, not an error") {
    // A group with more members than partitions leaves some holding nothing.
    // They heartbeat on, waiting for the next rebalance.
    CHECK(decodeAssignment({}).empty());
    CHECK(decodeAssignment(encodeAssignment({})).empty());
}

TEST_CASE("Range gives contiguous blocks, remainder to the earliest members") {
    const auto assignment = assignRange(inputFor({"a", "b"}, {"orders"}, {{"orders", 3}}));

    // Three partitions between two members: two and one. The extra goes to the
    // earlier member, so the result depends only on the sorted member list — not
    // on who joined first, and not on which leader computed it.
    CHECK(assignment.at("a") == vector<TopicPartition>{{"orders", 0}, {"orders", 1}});
    CHECK(assignment.at("b") == vector<TopicPartition>{{"orders", 2}});
    CHECK(totalAssigned(assignment) == 3);
}

TEST_CASE("Range gives the same member partition zero of every topic") {
    const auto assignment = assignRange(
        inputFor({"a", "b"}, {"orders", "payments"}, {{"orders", 4}, {"payments", 4}}));

    // The co-partitioning property, and the reason range exists: a join across
    // two topics keyed the same way needs both halves of a key on one consumer.
    CHECK(assignment.at("a")[0] == TopicPartition{"orders", 0});
    CHECK(assignment.at("a")[2] == TopicPartition{"payments", 0});
    CHECK(totalAssigned(assignment) == 8);
}

TEST_CASE("Round robin deals one partition at a time") {
    const auto assignment = assignRoundRobin(inputFor({"a", "b"}, {"orders"}, {{"orders", 5}}));

    CHECK(assignment.at("a") ==
          vector<TopicPartition>{{"orders", 0}, {"orders", 2}, {"orders", 4}});
    CHECK(assignment.at("b") == vector<TopicPartition>{{"orders", 1}, {"orders", 3}});
    CHECK(totalAssigned(assignment) == 5);
}

TEST_CASE("Round robin spreads more evenly across topics than range") {
    const auto counts = map<string, int32_t>{{"a-topic", 3}, {"b-topic", 3}};
    const auto range  = assignRange(inputFor({"x", "y"}, {"a-topic", "b-topic"}, counts));
    const auto robin  = assignRoundRobin(inputFor({"x", "y"}, {"a-topic", "b-topic"}, counts));

    // Range hands the remainder of EVERY topic to the same earliest member, so
    // its imbalance compounds; round robin carries the deal across topics.
    CHECK(range.at("x").size() == 4);
    CHECK(range.at("y").size() == 2);
    CHECK(robin.at("x").size() == 3);
    CHECK(robin.at("y").size() == 3);
}

TEST_CASE("Every partition is assigned exactly once") {
    for (const int members : {1, 2, 3, 5, 7}) {
        vector<string> ids;
        for (int i = 0; i < members; ++i) ids.push_back("member-" + to_string(i));

        for (const auto& assignment :
             {assignRange(inputFor(ids, {"orders"}, {{"orders", 12}})),
              assignRoundRobin(inputFor(ids, {"orders"}, {{"orders", 12}}))}) {
            set<TopicPartition> seen;
            size_t              count = 0;
            for (const auto& [member, partitions] : assignment)
                for (const auto& tp : partitions) {
                    seen.insert(tp);
                    ++count;
                }
            // Twice would mean two consumers reading the same partition, which
            // breaks the one guarantee a group makes. Never would mean records
            // nobody reads.
            CHECK(seen.size() == 12);
            CHECK(count == 12);
        }
    }
}

TEST_CASE("More members than partitions leaves some holding nothing") {
    const auto assignment =
        assignRange(inputFor({"a", "b", "c", "d"}, {"orders"}, {{"orders", 2}}));

    CHECK(totalAssigned(assignment) == 2);
    CHECK(assignment.size() == 4);   // everyone is still in the group

    int idle = 0;
    for (const auto& [member, partitions] : assignment)
        if (partitions.empty()) ++idle;
    CHECK(idle == 2);
}

TEST_CASE("A member gets nothing from a topic it did not subscribe to") {
    AssignmentInput input;
    input.members["a"]      = {"orders"};
    input.members["b"]      = {"payments"};
    input.partitionCounts   = {{"orders", 2}, {"payments", 2}};

    for (const auto& assignment : {assignRange(input), assignRoundRobin(input)}) {
        for (const auto& tp : assignment.at("a")) CHECK(tp.topic == "orders");
        for (const auto& tp : assignment.at("b")) CHECK(tp.topic == "payments");
        CHECK(totalAssigned(assignment) == 4);
    }
}

TEST_CASE("Two leaders computing the same group agree") {
    const auto input = inputFor({"c", "a", "b"}, {"orders"}, {{"orders", 7}});

    // Ordered by member id rather than by arrival, so which member happened to be
    // elected leader cannot change the answer.
    for (int i = 0; i < 5; ++i) {
        CHECK(assignRange(input) == assignRange(input));
        CHECK(assignRoundRobin(input) == assignRoundRobin(input));
    }
}

TEST_CASE("An assignment from a newer client is refused, not half-read") {
    auto bytes = encodeAssignment({{"orders", 0}});
    bytes[1]   = 99;
    CHECK_THROWS_AS(decodeAssignment(bytes), CorruptData);
}

// ===========================================================================
// M5 end to end: a consumer group over a real socket
// ===========================================================================

namespace {

// Total partitions across every member, and whether any is held twice.
struct Coverage {
    set<TopicPartition> distinct;
    size_t              total = 0;

    void add(const vector<TopicPartition>& assigned) {
        for (const auto& tp : assigned) {
            distinct.insert(tp);
            ++total;
        }
    }
    bool exclusive() const { return distinct.size() == total; }
};

}  // namespace

TEST_CASE("One consumer in a group gets every partition") {
    TempDir        dir("group-single");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    for (PartitionId p = 0; p < 4; ++p)
        broker.logs().createPartition(TopicPartition{"orders", p});

    cli::Client   client("127.0.0.1", broker.port());
    GroupConsumer consumer(client, "g", {"orders"});

    const auto assigned = consumer.join();
    CHECK(assigned.size() == 4);
    CHECK(consumer.isLeader());
    CHECK(consumer.generation() > 0);
    CHECK(consumer.heartbeat());

    broker.stop();
}

TEST_CASE("Two consumers split a topic between them, exclusively") {
    TempDir        dir("group-two");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    for (PartitionId p = 0; p < 4; ++p)
        broker.logs().createPartition(TopicPartition{"orders", p});

    cli::Client clientA("127.0.0.1", broker.port());
    cli::Client clientB("127.0.0.1", broker.port());

    GroupConsumer a(clientA, "g", {"orders"});
    GroupConsumer b(clientB, "g", {"orders"});

    // Both join; each retries through RebalanceInProgress until the group
    // settles. Run on threads because each is blocked on the other.
    vector<TopicPartition> assignedA;
    vector<TopicPartition> assignedB;
    thread joinA([&] { assignedA = a.join(); });
    thread joinB([&] { assignedB = b.join(); });
    joinA.join();
    joinB.join();

    Coverage coverage;
    coverage.add(assignedA);
    coverage.add(assignedB);

    // The only guarantee a group makes: every partition to exactly one member.
    // Twice would mean two consumers reading the same records; never would mean
    // records nobody reads.
    CHECK(coverage.distinct.size() == 4);
    CHECK(coverage.exclusive());
    CHECK(assignedA.size() == 2);
    CHECK(assignedB.size() == 2);
    CHECK(a.generation() == b.generation());

    broker.stop();
}

TEST_CASE("When one consumer leaves, the other takes over its partitions") {
    TempDir        dir("group-takeover");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    for (PartitionId p = 0; p < 4; ++p)
        broker.logs().createPartition(TopicPartition{"orders", p});

    cli::Client clientA("127.0.0.1", broker.port());
    cli::Client clientB("127.0.0.1", broker.port());

    GroupConsumer a(clientA, "g", {"orders"});
    GroupConsumer b(clientB, "g", {"orders"});

    vector<TopicPartition> assignedA;
    thread joinA([&] { assignedA = a.join(); });
    thread joinB([&] { b.join(); });
    joinA.join();
    joinB.join();
    REQUIRE(assignedA.size() == 2);

    const int32_t generationBefore = a.generation();

    // b leaves politely, which lets the rest rebalance immediately rather than
    // waiting out a session timeout.
    b.leave();

    // a finds out on the heartbeat it was making anyway — that is why heartbeats
    // exist rather than the coordinator simply timing members out.
    CHECK_FALSE(a.heartbeat());

    const auto afterwards = a.join();
    CHECK(afterwards.size() == 4);
    CHECK(a.generation() > generationBefore);

    broker.stop();
}

TEST_CASE("A group's position survives a restart") {
    TempDir    dir("group-commit-restart");
    const auto options = optionsFor(dir.file("data"));

    {
        server::Broker broker(options);
        broker.start();
        broker.logs().createPartition(TopicPartition{"orders", 0});

        cli::Client   client("127.0.0.1", broker.port());
        GroupConsumer consumer(client, "g", {"orders"});
        consumer.join();

        // Nothing committed yet — which is NOT offset zero, and a consumer that
        // confused the two would replay an entire topic.
        CHECK_FALSE(consumer.committed({"orders", 0}).has_value());

        consumer.commit({"orders", 0}, Offset(510));
        CHECK(consumer.committed({"orders", 0}) == Offset(510));
        broker.stop();
    }

    // A whole new broker over the same directory. __offsets is an ordinary topic,
    // so this is the log doing the durability — and the coordinator's map is
    // rebuilt by replaying the partition, which is AOF replay by another name.
    server::Broker restarted(options);
    restarted.start();

    cli::Client   client("127.0.0.1", restarted.port());
    GroupConsumer consumer(client, "g", {"orders"});
    consumer.join();

    CHECK(consumer.committed({"orders", 0}) == Offset(510));
    restarted.stop();
}

TEST_CASE("Two groups reading one topic keep separate positions") {
    TempDir        dir("group-independent");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    broker.logs().createPartition(TopicPartition{"orders", 0});

    cli::Client clientA("127.0.0.1", broker.port());
    cli::Client clientB("127.0.0.1", broker.port());

    GroupConsumer a(clientA, "analytics", {"orders"});
    GroupConsumer b(clientB, "billing", {"orders"});
    a.join();
    b.join();

    a.commit({"orders", 0}, Offset(100));
    b.commit({"orders", 0}, Offset(200));

    // Data is stored once and read by as many consumers as want it. Reading
    // consumes nothing, so two groups are two positions over the same records.
    CHECK(a.committed({"orders", 0}) == Offset(100));
    CHECK(b.committed({"orders", 0}) == Offset(200));

    broker.stop();
}

TEST_CASE("A stale member cannot commit over a live one") {
    TempDir        dir("group-fencing");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    broker.logs().createPartition(TopicPartition{"orders", 0});

    cli::Client clientA("127.0.0.1", broker.port());
    cli::Client clientB("127.0.0.1", broker.port());

    GroupConsumer a(clientA, "g", {"orders"});
    a.join();
    a.commit({"orders", 0}, Offset(100));
    const int32_t staleGeneration = a.generation();

    // b joins, which bumps the generation and leaves a stale.
    GroupConsumer b(clientB, "g", {"orders"});
    thread joinB([&] { b.join(); });
    thread rejoinA([&] { a.join(); });
    joinB.join();
    rejoinA.join();

    REQUIRE(a.generation() > staleGeneration);

    // A zombie on the old generation. Without fencing it would move a position
    // another member now owns — backwards, and silently.
    OffsetCommitRequest zombie;
    zombie.groupId    = "g";
    zombie.generation = staleGeneration;
    zombie.memberId   = a.memberId();
    zombie.topics.push_back({"orders", {{0, Offset(5), ""}}});

    const auto   responseBody = clientA.call(ApiKey::OffsetCommit,
                                             body(zombie, encodeOffsetCommitRequest));
    BufferReader in(responseBody);
    const auto   response = decodeOffsetCommitResponse(in);

    CHECK(response.topics.at(0).partitions.at(0).error == ErrorCode::IllegalGeneration);
    CHECK(a.committed({"orders", 0}) == Offset(100));   // unmoved

    broker.stop();
}

TEST_CASE("A group produces and consumes what it was assigned") {
    TempDir        dir("group-consume");
    server::Broker broker(optionsFor(dir.file("data")));
    broker.start();
    for (PartitionId p = 0; p < 2; ++p)
        broker.logs().createPartition(TopicPartition{"orders", p});

    cli::Client producer("127.0.0.1", broker.port());
    for (int i = 0; i < 6; ++i) {
        produceLine(producer, "orders", 0, "p0 line " + to_string(i));
        produceLine(producer, "orders", 1, "p1 line " + to_string(i));
    }

    cli::Client   client("127.0.0.1", broker.port());
    GroupConsumer consumer(client, "g", {"orders"});
    const auto    assigned = consumer.join();
    REQUIRE(assigned.size() == 2);

    // Read each assigned partition from where the group left off — nowhere, so
    // from the beginning — then commit where it got to.
    size_t read = 0;
    for (const auto& tp : assigned) {
        const Offset from = consumer.committed(tp).value_or(Offset(0));
        const auto   values = consumeFrom(client, tp.topic, tp.partition, from);
        read += values.size();
        consumer.commit(tp, from + static_cast<int64_t>(values.size()));
    }

    CHECK(read == 12);
    CHECK(consumer.committed({"orders", 0}) == Offset(6));
    CHECK(consumer.committed({"orders", 1}) == Offset(6));

    broker.stop();
}
