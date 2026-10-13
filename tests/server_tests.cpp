#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "protocol/error_codes.hpp"
#include <cerrno>

#include "protocol/client.hpp"
#include "protocol/create_replicas.hpp"
#include "protocol/create_topic.hpp"
#include "protocol/list_offsets.hpp"
#include "protocol/fetch.hpp"
#include "protocol/metadata.hpp"
#include "protocol/produce.hpp"
#include <atomic>
#include <chrono>
#include <thread>

#include "protocol/frame.hpp"
#include "group/offsets_topic.hpp"
#include "server/api_registry.hpp"
#include "server/broker.hpp"
#include "server/connection.hpp"
#include "protocol/socket.hpp"
#include "server/handlers.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::protocol;
using namespace dariyakyu::server;
using namespace dariyakyu::test;

// ===========================================================================
// Dispatch
// ===========================================================================

namespace {

// A broker with nothing in it, for tests that only exercise dispatch.
struct BareBroker {
    TempDir                 dir;
    storage::LogManager     logs;
    group::OffsetStore      offsets;
    group::GroupCoordinator groups;
    replication::ReplicaManager replicas{1};
    BrokerContext           context;

    explicit BareBroker(const string& name)
        : dir(name),
          logs(dir.file("data"), testConfig()),
          offsets(logs, group::kDefaultOffsetsPartitions),
          context{logs, groups, offsets, group::kDefaultOffsetsPartitions, 1, "127.0.0.1",
                  9092, {}, replicas} {}
};

// Builds a frame for `header` plus `body`, then runs it through the registry and
// hands back the response and the frame it borrowed from — the frame has to
// outlive the response, because a Produce handler stamps into it.
struct Dispatched {
    vector<uint8_t> frame;
    Response        response;
};

Dispatched dispatch(const ApiRegistry& registry, BrokerContext& broker,
                    const RequestHeader& header, const vector<uint8_t>& body = {}) {
    BufferWriter out;
    encodeRequestHeader(out, header);
    out.writeBytes(body);

    Dispatched result;
    result.frame = out.take();

    BufferReader   reader(result.frame);
    const auto     decoded = decodeRequestHeader(reader);
    RequestContext request{decoded, reader, result.frame, broker};
    result.response = registry.dispatch(request);
    return result;
}

RequestHeader headerFor(ApiKey key, int32_t correlationId = 1, int16_t version = 0) {
    RequestHeader header;
    header.apiKey        = key;
    header.apiVersion    = version;
    header.correlationId = correlationId;
    header.clientId      = "test";
    return header;
}

}  // namespace

TEST_CASE("Every response opens with the correlation id it was asked with") {
    BareBroker  broker("dispatch-correlation");
    ApiRegistry registry;
    registry.registerHandler(ApiKey::Metadata, 0, [](RequestContext&, Response& out) {
        BufferWriter body;
        body.writeInt32(0xCAFE);
        out.append(body.take());
    });

    const auto result = dispatch(registry, broker.context, headerFor(ApiKey::Metadata, 7777));

    // Written by dispatch, not by the handler: every response carries one, and a
    // handler that forgot would produce a reply no client could match.
    const auto   frame = result.response.materialise();
    BufferReader in(frame);
    CHECK(decodeResponseHeader(in) == 7777);
    CHECK(in.readInt32() == static_cast<int32_t>(0xCAFE));
    CHECK(in.empty());
}

TEST_CASE("An unknown api is answered rather than disconnected") {
    BareBroker        broker("dispatch-unknown-api");
    const ApiRegistry registry;   // nothing registered

    const auto result =
        dispatch(registry, broker.context, headerFor(static_cast<ApiKey>(999), 42));

    // A client probing what a broker supports is doing something reasonable, and
    // the correlation id is already in hand — which is exactly why
    // decodeRequestHeader refuses to validate the key it just read.
    const auto   frame = result.response.materialise();
    BufferReader in(frame);
    CHECK(decodeResponseHeader(in) == 42);
    CHECK(static_cast<ErrorCode>(in.readInt16()) == ErrorCode::UnsupportedVersion);
}

TEST_CASE("A known api at an unknown version is answered the same way") {
    BareBroker  broker("dispatch-unknown-version");
    ApiRegistry registry;
    registry.registerHandler(ApiKey::Metadata, 0, [](RequestContext&, Response& out) {
        out.append(vector<uint8_t>{1});
    });

    CHECK(registry.has(ApiKey::Metadata, 0));
    CHECK_FALSE(registry.has(ApiKey::Metadata, 1));

    const auto result =
        dispatch(registry, broker.context, headerFor(ApiKey::Metadata, 9, 1));
    const auto   frame = result.response.materialise();
    BufferReader in(frame);
    CHECK(decodeResponseHeader(in) == 9);
    CHECK(static_cast<ErrorCode>(in.readInt16()) == ErrorCode::UnsupportedVersion);
}

TEST_CASE("Two versions of one api are two handlers") {
    BareBroker  broker("dispatch-versions");
    ApiRegistry registry;
    registry.registerHandler(ApiKey::Fetch, 0, [](RequestContext&, Response& out) {
        BufferWriter body;
        body.writeInt16(0);
        out.append(body.take());
    });
    registry.registerHandler(ApiKey::Fetch, 1, [](RequestContext&, Response& out) {
        BufferWriter body;
        body.writeInt16(1);
        out.append(body.take());
    });

    // The reason this is a registry and not a switch: a switch on key containing
    // a switch on version means every new version edits a function every other
    // version lives in.
    for (int16_t version = 0; version <= 1; ++version) {
        const auto   result = dispatch(registry, broker.context,
                                       headerFor(ApiKey::Fetch, 1, version));
        const auto   frame  = result.response.materialise();
        BufferReader in(frame);
        decodeResponseHeader(in);
        CHECK(in.readInt16() == version);
    }
}

TEST_CASE("A handler reads its body from where the header left off") {
    BareBroker  broker("dispatch-body");
    ApiRegistry registry;
    registry.registerHandler(ApiKey::Produce, 0, [](RequestContext& request, Response& out) {
        // No offset arithmetic: decodeRequestHeader already positioned it.
        BufferWriter body;
        body.writeInt64(request.body.readInt64());
        out.append(body.take());
    });

    BufferWriter bodyOut;
    bodyOut.writeInt64(123456789);
    const auto result =
        dispatch(registry, broker.context, headerFor(ApiKey::Produce), bodyOut.take());

    const auto   frame = result.response.materialise();
    BufferReader in(frame);
    decodeResponseHeader(in);
    CHECK(in.readInt64() == 123456789);
}

TEST_CASE("A handler can see the broker it is running in") {
    BareBroker  broker("dispatch-context");
    ApiRegistry registry;
    registry.registerHandler(ApiKey::Metadata, 0, [](RequestContext& request, Response& out) {
        BufferWriter body;
        body.writeInt32(request.broker.nodeId);
        body.writeInt32(request.broker.advertisedPort);
        body.writeInt32(static_cast<int32_t>(request.broker.logs.partitionCount()));
        out.append(body.take());
    });

    broker.logs.createPartition(TopicPartition{"orders", 0});

    const auto   result = dispatch(registry, broker.context, headerFor(ApiKey::Metadata));
    const auto   frame  = result.response.materialise();
    BufferReader in(frame);
    decodeResponseHeader(in);
    CHECK(in.readInt32() == 1);       // nodeId
    CHECK(in.readInt32() == 9092);    // advertisedPort
    CHECK(in.readInt32() == 1);       // one partition, seen through LogManager
}

TEST_CASE("Registering twice replaces the handler") {
    BareBroker  broker("dispatch-replace");
    ApiRegistry registry;
    registry.registerHandler(ApiKey::Metadata, 0,
                             [](RequestContext&, Response& out) { out.append(vector<uint8_t>{1}); });
    registry.registerHandler(ApiKey::Metadata, 0,
                             [](RequestContext&, Response& out) { out.append(vector<uint8_t>{2}); });

    const auto frame = dispatch(registry, broker.context, headerFor(ApiKey::Metadata))
                           .response.materialise();
    CHECK(frame.back() == 2);
}

// ===========================================================================
// Handlers: ListOffsets, Metadata, CreateTopic
// ===========================================================================

namespace {

// A broker with the real handlers registered.
struct ServingBroker : BareBroker {
    ApiRegistry registry;
    explicit ServingBroker(const string& name) : BareBroker(name) {
        registerAllHandlers(registry);
    }

    // Sends one request and hands back the response body, past the correlation id.
    vector<uint8_t> call(ApiKey key, const vector<uint8_t>& body) {
        const auto   result = dispatch(registry, context, headerFor(key, 1), body);
        const auto   frame  = result.response.materialise();
        BufferReader in(frame);
        CHECK(decodeResponseHeader(in) == 1);
        const auto rest = in.readBytes(in.remaining());
        return vector<uint8_t>(rest.begin(), rest.end());
    }
};

vector<uint8_t> encoded(const ListOffsetsRequest& request) {
    BufferWriter out;
    encodeListOffsetsRequest(out, request);
    return out.take();
}
vector<uint8_t> encoded(const MetadataRequest& request) {
    BufferWriter out;
    encodeMetadataRequest(out, request);
    return out.take();
}
vector<uint8_t> encoded(const CreateTopicRequest& request) {
    BufferWriter out;
    encodeCreateTopicRequest(out, request);
    return out.take();
}
vector<uint8_t> encoded(const CreateReplicasRequest& request) {
    BufferWriter out;
    encodeCreateReplicasRequest(out, request);
    return out.take();
}

// What broker 1 sends after creating "orders" with three partitions, two copies
// each, on a three-node cluster: every partition, with its replica list.
CreateReplicasRequest ordersFromBroker1() {
    CreateReplicasRequest::Topic topic;
    topic.settings.name              = "orders";
    topic.settings.partitionCount    = 3;
    topic.settings.retentionMs       = 60'000;
    topic.settings.replicationFactor = 2;
    topic.partitions                 = {{0, {1, 2}}, {1, {2, 3}}, {2, {3, 1}}};

    CreateReplicasRequest request;
    request.topics.push_back(topic);
    return request;
}

ErrorCode firstError(const vector<uint8_t>& body) {
    BufferReader in(body);
    return decodeCreateTopicResponse(in).topics.at(0).error;
}

}  // namespace

TEST_CASE("ListOffsets reports where a partition starts and ends") {
    ServingBroker broker("handler-listoffsets");
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0});
    for (int i = 0; i < 7; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log.append(bytes);
    }

    ListOffsetsRequest request;
    request.topics.push_back(
        {"orders", {{0, kEarliestTimestamp}, {0, kLatestTimestamp}}});

    const auto   body = broker.call(ApiKey::ListOffsets, encoded(request));
    BufferReader in(body);
    const auto   response = decodeListOffsetsResponse(in);

    REQUIRE(response.topics[0].partitions.size() == 2);
    CHECK(response.topics[0].partitions[0].offset == Offset(0));   // earliest
    CHECK(response.topics[0].partitions[1].offset == Offset(7));   // latest
    CHECK(response.topics[0].partitions[0].error == ErrorCode::None);
}

TEST_CASE("ListOffsets on a partition this broker does not host says so") {
    ServingBroker broker("handler-listoffsets-missing");

    ListOffsetsRequest request;
    request.topics.push_back({"orders", {{3, kLatestTimestamp}}});

    const auto   body = broker.call(ApiKey::ListOffsets, encoded(request));
    BufferReader in(body);
    const auto   response = decodeListOffsetsResponse(in);

    // Routine, not exceptional: a client with stale metadata. It re-fetches
    // Metadata and retries.
    CHECK(response.topics[0].partitions[0].error == ErrorCode::NotLeaderForPartition);
}

TEST_CASE("ListOffsets answers the partitions it can and errors the ones it cannot") {
    ServingBroker broker("handler-listoffsets-mixed");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    ListOffsetsRequest request;
    request.topics.push_back(
        {"orders", {{0, kLatestTimestamp}, {1, kLatestTimestamp}, {2, kLatestTimestamp}}});

    const auto   body = broker.call(ApiKey::ListOffsets, encoded(request));
    BufferReader in(body);
    const auto   response = decodeListOffsetsResponse(in);

    // Per partition, not per request. One missing partition must not stop the
    // others being answered.
    CHECK(response.topics[0].partitions[0].error == ErrorCode::None);
    CHECK(response.topics[0].partitions[1].error == ErrorCode::NotLeaderForPartition);
    CHECK(response.topics[0].partitions[2].error == ErrorCode::NotLeaderForPartition);
}

TEST_CASE("ListOffsets reflects retention having moved the start") {
    ServingBroker broker("handler-listoffsets-retention");
    storage::LogConfig config = testConfig();
    config.roll.maxSegmentBytes  = 400;
    config.retention.retentionMs = 1;
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0}, config);
    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(100'000 + i, 48);
        log.append(bytes);
    }
    log.applyRetention(1'000'000);
    REQUIRE(log.logStartOffset() > Offset(0));

    ListOffsetsRequest request;
    request.topics.push_back({"orders", {{0, kEarliestTimestamp}}});
    const auto   body = broker.call(ApiKey::ListOffsets, encoded(request));
    BufferReader in(body);
    const auto   response = decodeListOffsetsResponse(in);

    // This is what the API is for: retention made an offset disappear, and this
    // is how a client finds out what replaced it.
    CHECK(response.topics[0].partitions[0].offset == log.logStartOffset());
}

TEST_CASE("A timestamp search is refused rather than answered wrongly") {
    ServingBroker broker("handler-listoffsets-timestamp");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    ListOffsetsRequest request;
    request.topics.push_back({"orders", {{0, 1'700'000'000'000LL}}});

    const auto   body = broker.call(ApiKey::ListOffsets, encoded(request));
    BufferReader in(body);
    const auto   response = decodeListOffsetsResponse(in);

    // Searching by a real timestamp is banked. Returning the latest offset
    // instead would look like an answer and be a wrong one.
    CHECK(response.topics[0].partitions[0].error != ErrorCode::None);
}

TEST_CASE("Metadata describes every partition of every topic") {
    ServingBroker broker("handler-metadata-all");
    for (PartitionId p = 0; p < 3; ++p) broker.logs.createPartition(TopicPartition{"orders", p});
    broker.logs.createPartition(TopicPartition{"payments", 0});

    MetadataRequest request;
    request.allTopics = true;

    const auto   body = broker.call(ApiKey::Metadata, encoded(request));
    BufferReader in(body);
    const auto   response = decodeMetadataResponse(in);

    REQUIRE(response.brokers.size() == 1);
    CHECK(response.brokers[0].nodeId == 1);
    CHECK(response.brokers[0].host == "127.0.0.1");
    CHECK(response.brokers[0].port == 9092);
    CHECK(response.controllerId == -1);   // none until M8

    REQUIRE(response.topics.size() == 2);
    CHECK(response.topics[0].name == "orders");
    CHECK(response.topics[0].partitions.size() == 3);
    CHECK(response.topics[1].name == "payments");

    // Every partition's leader is this broker, and a client reads it rather than
    // assuming — so M8 can move partitions without the client changing.
    for (const auto& partition : response.topics[0].partitions) CHECK(partition.leader == 1);
}

TEST_CASE("Metadata names every broker and each partition's real leader") {
    ServingBroker broker("handler-metadata-cluster");
    broker.context.cluster = cluster::parseClusterConfig("1@127.0.0.1:7001,2@10.0.0.2:9092");

    // As CreateTopic would have left them: partition 0 led here, partition 1
    // led by node 2 with a copy here.
    storage::LogConfig led = testConfig(), following = testConfig();
    led.replication.replicas       = {1, 2};
    following.replication.replicas = {2, 1};
    broker.logs.createPartition(TopicPartition{"orders", 0}, led);
    broker.logs.createPartition(TopicPartition{"orders", 1}, following);

    MetadataRequest request;
    request.topics = {"orders"};
    const auto   body = broker.call(ApiKey::Metadata, encoded(request));
    BufferReader in(body);
    const auto   response = decodeMetadataResponse(in);

    // Both brokers. Our own entry is what we advertise (port 9092 here), not
    // the 7001 the cluster list was given.
    REQUIRE(response.brokers.size() == 2);
    CHECK(response.brokers[0].port == 9092);
    CHECK(response.brokers[1].nodeId == 2);
    CHECK(response.brokers[1].host == "10.0.0.2");

    const auto& partitions = response.topics.at(0).partitions;
    REQUIRE(partitions.size() == 2);
    CHECK(partitions[0].leader == 1);
    CHECK(partitions[1].leader == 2);   // hosted here, led elsewhere
    CHECK(partitions[1].replicas == vector<NodeId>{2, 1});
}

TEST_CASE("Metadata reports who is in sync, and only where it knows") {
    ServingBroker broker("handler-metadata-isr");

    // Partition 0 led here, followed by 2. Partition 1 led by 2, followed here.
    CreateReplicasRequest::Topic topic;
    topic.settings.name = "orders";
    topic.partitions    = {{0, {1, 2}}, {1, {2, 1}}};
    CreateReplicasRequest create;
    create.topics.push_back(topic);
    broker.call(ApiKey::CreateReplicas, encoded(create));

    const auto describeOrders = [&] {
        MetadataRequest request;
        request.topics = {"orders"};
        const auto   body = broker.call(ApiKey::Metadata, encoded(request));
        BufferReader in(body);
        return decodeMetadataResponse(in).topics.at(0).partitions;
    };

    auto partitions = describeOrders();
    CHECK(partitions[0].isr == vector<NodeId>{1, 2});   // node 2 inside its first window
    CHECK(partitions[1].isr.empty());                   // only node 2 knows

    // Node 2 never fetches. Past the lag window, the timer drops it.
    broker.replicas.shrinkInSync(storage::wallClockMillis() + 31'000);
    partitions = describeOrders();
    CHECK(partitions[0].isr == vector<NodeId>{1});
    CHECK(partitions[0].replicas == vector<NodeId>{1, 2});   // still a replica, just behind
}

TEST_CASE("Metadata is ordered, so two identical brokers answer identically") {
    ServingBroker broker("handler-metadata-order");
    // Created out of order deliberately; LogManager stores them in a hash map.
    broker.logs.createPartition(TopicPartition{"zebra", 2});
    broker.logs.createPartition(TopicPartition{"alpha", 1});
    broker.logs.createPartition(TopicPartition{"zebra", 0});
    broker.logs.createPartition(TopicPartition{"alpha", 0});

    MetadataRequest request;
    request.allTopics = true;
    const auto   body = broker.call(ApiKey::Metadata, encoded(request));
    BufferReader in(body);
    const auto   response = decodeMetadataResponse(in);

    // A wire format that differs between two runs of the same broker is a
    // debugging trap.
    REQUIRE(response.topics.size() == 2);
    CHECK(response.topics[0].name == "alpha");
    CHECK(response.topics[1].name == "zebra");
    CHECK(response.topics[1].partitions[0].partition == 0);
    CHECK(response.topics[1].partitions[1].partition == 2);
}

TEST_CASE("Metadata names back a topic it does not have") {
    ServingBroker broker("handler-metadata-missing");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    MetadataRequest request;
    request.topics = {"orders", "nonexistent"};

    const auto   body = broker.call(ApiKey::Metadata, encoded(request));
    BufferReader in(body);
    const auto   response = decodeMetadataResponse(in);

    REQUIRE(response.topics.size() == 2);
    CHECK(response.topics[0].error == ErrorCode::None);

    // Named back with an error rather than omitted, so a client can tell "you do
    // not have it" from "I forgot to ask".
    CHECK(response.topics[1].name == "nonexistent");
    CHECK(response.topics[1].error == ErrorCode::UnknownTopicOrPartition);
    CHECK(response.topics[1].partitions.empty());
}

TEST_CASE("CreateTopic makes every partition it was asked for") {
    ServingBroker broker("handler-createtopic");

    CreateTopicRequest request;
    request.topics.push_back({"orders", 4, {}, {}, {}, {}});

    const auto   body = broker.call(ApiKey::CreateTopic, encoded(request));
    BufferReader in(body);
    const auto   response = decodeCreateTopicResponse(in);

    CHECK(response.topics[0].error == ErrorCode::None);
    CHECK(broker.logs.partitionCount() == 4);
    for (PartitionId p = 0; p < 4; ++p)
        CHECK(broker.logs.get(TopicPartition{"orders", p}) != nullptr);
}

TEST_CASE("CreateTopic applies the overrides it was given") {
    ServingBroker broker("handler-createtopic-config");

    CreateTopicRequest request;
    CreateTopicRequest::Topic topic;
    topic.name            = "audit";
    topic.partitionCount  = 1;
    topic.retentionMs     = 60'000;
    topic.maxSegmentBytes = 1u << 20;
    request.topics.push_back(topic);

    broker.call(ApiKey::CreateTopic, encoded(request));

    storage::Log* log = broker.logs.get(TopicPartition{"audit", 0});
    REQUIRE(log != nullptr);
    CHECK(log->config().retention.retentionMs == 60'000);
    CHECK(log->config().roll.maxSegmentBytes == (1u << 20));

    // And they reached partition.meta, so a restart keeps them.
    CHECK(storage::readPartitionMeta(broker.logs.dataDir() / "audit-0",
                                     TopicPartition{"audit", 0})
              .config.retention.retentionMs == 60'000);
}

TEST_CASE("Creating a topic that exists changes nothing") {
    ServingBroker broker("handler-createtopic-exists");

    CreateTopicRequest request;
    request.topics.push_back({"orders", 2, {}, {}, {}, {}});
    broker.call(ApiKey::CreateTopic, encoded(request));
    REQUIRE(broker.logs.partitionCount() == 2);

    // Asked for four this time. Checked for EVERY partition before anything is
    // created — three of four would leave a topic that half exists, which no
    // error code describes and no client could act on.
    CreateTopicRequest again;
    again.topics.push_back({"orders", 4, {}, {}, {}, {}});
    const auto   body = broker.call(ApiKey::CreateTopic, encoded(again));
    BufferReader in(body);
    const auto   response = decodeCreateTopicResponse(in);

    CHECK(response.topics[0].error == ErrorCode::TopicAlreadyExists);
    CHECK(broker.logs.partitionCount() == 2);
}

TEST_CASE("A broker creates only the replicas it holds, and records who holds them") {
    ServingBroker broker("handler-createtopic-replicas");
    // We are node 1. Nodes 2 and 3 point at closed ports, so nothing answers.
    broker.context.cluster =
        cluster::parseClusterConfig("1@127.0.0.1:1,2@127.0.0.1:2,3@127.0.0.1:3");

    CreateTopicRequest        request;
    CreateTopicRequest::Topic topic;
    topic.name              = "orders";
    topic.partitionCount    = 3;
    topic.replicationFactor = 2;
    request.topics.push_back(topic);

    // The peers cannot be told, so the client hears Unknown — but this broker's
    // own copies were made first and stay made. A partly created topic is M7's
    // known gap; M8's controller closes it.
    const auto   body = broker.call(ApiKey::CreateTopic, encoded(request));
    BufferReader in(body);
    CHECK(decodeCreateTopicResponse(in).topics[0].error == ErrorCode::Unknown);

    // Partition 0 is {1, 2} and partition 2 is {3, 1}: both ours. Partition 1 is
    // {2, 3}, so it belongs on other brokers and not on this disk.
    REQUIRE(broker.logs.get(TopicPartition{"orders", 0}) != nullptr);
    REQUIRE(broker.logs.get(TopicPartition{"orders", 2}) != nullptr);
    CHECK(broker.logs.get(TopicPartition{"orders", 1}) == nullptr);

    CHECK(broker.logs.get(TopicPartition{"orders", 0})->config().replication.replicas ==
          vector<NodeId>{1, 2});
    CHECK(storage::readPartitionMeta(broker.logs.dataDir() / "orders-2",
                                     TopicPartition{"orders", 2})
              .config.replication.replicas == vector<NodeId>{3, 1});
}

TEST_CASE("A replication factor the cluster cannot hold is refused") {
    ServingBroker broker("handler-createtopic-bad-rf");
    broker.context.cluster = cluster::parseClusterConfig("1@h:1,2@h:2");

    for (const int32_t factor : {0, 3}) {
        CreateTopicRequest        request;
        CreateTopicRequest::Topic topic;
        topic.name              = "orders";
        topic.replicationFactor = factor;
        request.topics.push_back(topic);

        const auto   body = broker.call(ApiKey::CreateTopic, encoded(request));
        BufferReader in(body);
        CHECK(decodeCreateTopicResponse(in).topics[0].error ==
              ErrorCode::InvalidReplicationFactor);
    }
    CHECK(broker.logs.partitionCount() == 0);
}

TEST_CASE("A peer's CreateReplicas makes this broker's copies with the leader's rules") {
    ServingBroker broker("handler-createreplicas");
    broker.context.nodeId = 2;

    REQUIRE(firstError(broker.call(ApiKey::CreateReplicas, encoded(ordersFromBroker1()))) ==
            ErrorCode::None);

    // Node 2 is in {1, 2} and {2, 3}, not in {3, 1}.
    REQUIRE(broker.logs.get(TopicPartition{"orders", 0}) != nullptr);
    REQUIRE(broker.logs.get(TopicPartition{"orders", 1}) != nullptr);
    CHECK(broker.logs.get(TopicPartition{"orders", 2}) == nullptr);

    // The replica list as sent, and the override sent with it — not this
    // broker's seven-day default.
    const auto& config = broker.logs.get(TopicPartition{"orders", 1})->config();
    CHECK(config.replication.replicas == vector<NodeId>{2, 3});
    CHECK(config.retention.retentionMs == 60'000);
}

TEST_CASE("A CreateReplicas for copies that exist, or a bad name, changes nothing") {
    ServingBroker broker("handler-createreplicas-refused");
    broker.context.nodeId = 2;
    broker.call(ApiKey::CreateReplicas, encoded(ordersFromBroker1()));
    REQUIRE(broker.logs.partitionCount() == 2);

    CHECK(firstError(broker.call(ApiKey::CreateReplicas, encoded(ordersFromBroker1()))) ==
          ErrorCode::TopicAlreadyExists);

    auto badName = ordersFromBroker1();
    badName.topics[0].settings.name = "../escape";
    CHECK(firstError(broker.call(ApiKey::CreateReplicas, encoded(badName))) ==
          ErrorCode::InvalidTopic);

    CHECK(broker.logs.partitionCount() == 2);
}

TEST_CASE("A topic name that cannot be a directory name is refused") {
    ServingBroker broker("handler-createtopic-bad-name");

    CreateTopicRequest request;
    request.topics.push_back({"../escape", 1, {}, {}, {}, {}});
    request.topics.push_back({"has space", 1, {}, {}, {}, {}});
    request.topics.push_back({"", 1, {}, {}, {}, {}});
    request.topics.push_back({"orders", 0, {}, {}, {}, {}});   // no partitions

    const auto   body = broker.call(ApiKey::CreateTopic, encoded(request));
    BufferReader in(body);
    const auto   response = decodeCreateTopicResponse(in);

    // The boundary where a client's string becomes a filesystem path, checked
    // before anything is created.
    for (const auto& topic : response.topics) CHECK(topic.error == ErrorCode::InvalidTopic);
    CHECK(broker.logs.partitionCount() == 0);
    CHECK(filesystem::is_empty(broker.logs.dataDir()));
}

// ===========================================================================
// Handlers: Produce and Fetch
// ===========================================================================

namespace {

vector<uint8_t> encoded(const ProduceRequest& request) {
    BufferWriter out;
    encodeProduceRequest(out, request);
    return out.take();
}
vector<uint8_t> encoded(const FetchRequest& request) {
    BufferWriter out;
    encodeFetchRequest(out, request);
    return out.take();
}

}  // namespace

TEST_CASE("Produce appends and reports the offset assigned") {
    ServingBroker broker("handler-produce");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    const auto first  = makeUnstampedBatch(1000, 48);
    const auto second = makeUnstampedBatch(2000, 48, 3);   // three records

    ProduceRequest request;
    request.topics.push_back({"orders",
                              {{0, span<const uint8_t>(first), 0},
                               {0, span<const uint8_t>(second), 0}}});

    const auto   body = broker.call(ApiKey::Produce, encoded(request));
    BufferReader in(body);
    const auto   response = decodeProduceResponse(in);

    REQUIRE(response.topics[0].partitions.size() == 2);
    CHECK(response.topics[0].partitions[0].error == ErrorCode::None);
    CHECK(response.topics[0].partitions[0].baseOffset == Offset(0));

    // The second batch starts where the first ended, and it held three records.
    CHECK(response.topics[0].partitions[1].baseOffset == Offset(1));
    CHECK(broker.logs.get(TopicPartition{"orders", 0})->logEndOffset() == Offset(4));
}

TEST_CASE("A produced batch reaches disk stamped with its assigned offset") {
    ServingBroker broker("handler-produce-stamped");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    const auto batch = makeUnstampedBatch(1000, 48);
    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});
    broker.call(ApiKey::Produce, encoded(request));
    broker.call(ApiKey::Produce, encoded(request));

    // The client sent the same unstamped bytes twice; the broker's sequence is
    // the only authority, so what landed says 0 and 1.
    storage::Log* log = broker.logs.get(TopicPartition{"orders", 0});
    const auto    bytes = readFile(storage::segmentLogPath(
        broker.logs.dataDir() / "orders-0", Offset(0)));

    CHECK(storage::RecordBatch::parseHeader(bytes).baseOffset == Offset(0));
    const size_t firstSize = storage::RecordBatch::totalSizeOf(bytes);
    CHECK(storage::RecordBatch::parseHeader(
              span<const uint8_t>(bytes).subspan(firstSize)).baseOffset == Offset(1));
    CHECK(log->logEndOffset() == Offset(2));
}

TEST_CASE("A produce to a partition this broker does not host is refused") {
    ServingBroker broker("handler-produce-missing");

    const auto     batch = makeUnstampedBatch(1000, 48);
    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});

    const auto   body = broker.call(ApiKey::Produce, encoded(request));
    BufferReader in(body);
    const auto   response = decodeProduceResponse(in);

    CHECK(response.topics[0].partitions[0].error == ErrorCode::NotLeaderForPartition);
    CHECK(response.topics[0].partitions[0].baseOffset == kUnknownOffset);
}

TEST_CASE("A batch that fails its own checksum never reaches a segment") {
    ServingBroker broker("handler-produce-corrupt");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    auto batch = makeUnstampedBatch(1000, 48);
    batch[kBatchHeaderSize + 4] ^= 0xFF;   // flip a byte in the body

    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});

    const auto   body = broker.call(ApiKey::Produce, encoded(request));
    BufferReader in(body);
    const auto   response = decodeProduceResponse(in);

    // The CLIENT's bytes are bad, and it is told precisely — CorruptMessage, not
    // Unknown. The same exception from a Fetch means something else entirely,
    // which is why there is no general exception-to-code mapping.
    CHECK(response.topics[0].partitions[0].error == ErrorCode::CorruptMessage);

    // And nothing was written: recovery would otherwise find it later and
    // truncate everything after it.
    CHECK(broker.logs.get(TopicPartition{"orders", 0})->logEndOffset() == Offset(0));
    CHECK(broker.logs.get(TopicPartition{"orders", 0})->totalSizeBytes() == 0);
}

TEST_CASE("Unparseable bytes are refused as a corrupt message") {
    ServingBroker broker("handler-produce-garbage");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    const vector<uint8_t> garbage(120, 0x7F);   // no valid magic byte anywhere
    ProduceRequest        request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(garbage), 0}}});

    const auto   body = broker.call(ApiKey::Produce, encoded(request));
    BufferReader in(body);
    const auto   response = decodeProduceResponse(in);

    CHECK(response.topics[0].partitions[0].error == ErrorCode::CorruptMessage);
    CHECK(broker.logs.get(TopicPartition{"orders", 0})->logEndOffset() == Offset(0));
}

TEST_CASE("One bad partition does not spoil the others in a produce") {
    ServingBroker broker("handler-produce-mixed");
    broker.logs.createPartition(TopicPartition{"orders", 0});
    broker.logs.createPartition(TopicPartition{"orders", 1});

    const auto good = makeUnstampedBatch(1000, 48);
    auto       bad  = makeUnstampedBatch(2000, 48);
    bad[kBatchHeaderSize + 2] ^= 0xFF;

    ProduceRequest request;
    request.topics.push_back({"orders",
                              {{0, span<const uint8_t>(good), 0},
                               {1, span<const uint8_t>(bad), 0},
                               {2, span<const uint8_t>(good), 0}}});

    const auto   body = broker.call(ApiKey::Produce, encoded(request));
    BufferReader in(body);
    const auto   response = decodeProduceResponse(in);

    CHECK(response.topics[0].partitions[0].error == ErrorCode::None);
    CHECK(response.topics[0].partitions[1].error == ErrorCode::CorruptMessage);
    CHECK(response.topics[0].partitions[2].error == ErrorCode::NotLeaderForPartition);
    CHECK(broker.logs.get(TopicPartition{"orders", 0})->logEndOffset() == Offset(1));
    CHECK(broker.logs.get(TopicPartition{"orders", 1})->logEndOffset() == Offset(0));
}

TEST_CASE("Fetch returns records as file segments") {
    ServingBroker broker("handler-fetch");
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0});
    for (int i = 0; i < 10; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log.append(bytes);
    }

    FetchRequest request;
    request.topics.push_back({"orders", {{0, Offset(0), 1 << 20}}});

    const auto result = dispatch(broker.registry, broker.context,
                                 headerFor(ApiKey::Fetch, 5), encoded(request));

    // Metadata in a buffer, records as a range that was never read here. Two
    // segments, not three: the correlation id dispatch wrote and the handler's
    // metadata coalesce into one.
    REQUIRE(result.response.segments().size() == 2);
    CHECK_FALSE(result.response.segments()[0].isFile());
    CHECK(result.response.segments()[1].isFile());

    const auto   frame = result.response.materialise();
    BufferReader in(frame);
    CHECK(decodeResponseHeader(in) == 5);
    const auto view = decodeFetchResponse(in);

    const auto& partition = view.topics[0].partitions[0];
    CHECK(partition.error == ErrorCode::None);
    CHECK(partition.highWatermark == Offset(10));
    CHECK(partition.logStartOffset == Offset(0));
    CHECK(storage::RecordBatch::verifyCrc(partition.records));
    CHECK(storage::RecordBatch::parseHeader(partition.records).baseOffset == Offset(0));
}

namespace {

// A partition on node 1 replicated to `replicas`, holding three one-record
// batches, of which only offset 0 is committed.
storage::Log& replicatedOrders(ServingBroker& broker, vector<NodeId> replicas) {
    storage::LogConfig config  = testConfig();
    config.replication.replicas = std::move(replicas);
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0}, config);
    log.setHighWatermarkTracksLogEnd(false);
    for (int i = 0; i < 3; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log.append(bytes);
    }
    log.setHighWatermark(Offset(1));
    return log;
}

FetchRequest fetchOrdersFrom(Offset offset, NodeId replicaId) {
    FetchRequest request;
    request.replicaId = replicaId;
    request.topics.push_back({"orders", {{0, offset, 1 << 20}}});
    return request;
}

}  // namespace

TEST_CASE("A consumer reads to the high watermark, a follower to the log end") {
    ServingBroker broker("handler-fetch-ceiling");
    replicatedOrders(broker, {1, 2});
    const size_t batch = makeUnstampedBatch(1000, 48).size();

    const auto   asConsumer = broker.call(ApiKey::Fetch, encoded(fetchOrdersFrom(Offset(0), -1)));
    BufferReader consumerIn(asConsumer);
    const auto   consumer = decodeFetchResponse(consumerIn).topics[0].partitions[0];
    CHECK(consumer.error == ErrorCode::None);
    CHECK(consumer.highWatermark == Offset(1));
    CHECK(consumer.records.size() == batch);   // offset 0 only

    const auto   asFollower = broker.call(ApiKey::Fetch, encoded(fetchOrdersFrom(Offset(0), 2)));
    BufferReader followerIn(asFollower);
    const auto   follower = decodeFetchResponse(followerIn).topics[0].partitions[0];
    CHECK(follower.error == ErrorCode::None);
    CHECK(follower.records.size() == 3 * batch);   // everything, committed or not
}

TEST_CASE("A follower's copy is not served, to consumers or to other followers") {
    ServingBroker broker("handler-fetch-not-leader");
    replicatedOrders(broker, {2, 1});   // node 2 leads; we only hold a copy

    for (const NodeId replicaId : {-1, 3}) {
        const auto   body = broker.call(ApiKey::Fetch, encoded(fetchOrdersFrom(Offset(0), replicaId)));
        BufferReader in(body);
        CHECK(decodeFetchResponse(in).topics[0].partitions[0].error ==
              ErrorCode::NotLeaderForPartition);
    }
}

TEST_CASE("A replica id that is not one of the partition's followers is refused") {
    ServingBroker broker("handler-fetch-stranger");
    replicatedOrders(broker, {1, 2});

    // Node 3 holds no copy, and node 1 is the leader itself. Either would read
    // past the high watermark if believed.
    for (const NodeId replicaId : {3, 1}) {
        const auto   body = broker.call(ApiKey::Fetch, encoded(fetchOrdersFrom(Offset(0), replicaId)));
        BufferReader in(body);
        const auto   partition = decodeFetchResponse(in).topics[0].partitions[0];
        CHECK(partition.error == ErrorCode::UnknownTopicOrPartition);
        CHECK(partition.records.empty());
    }
}

namespace {

// One batch for orders-0, as a producer would send it.
ProduceRequest produceToOrders(const vector<uint8_t>& batch) {
    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});
    return request;
}

FetchResponseView::Partition fetchOrders(ServingBroker& broker, Offset offset, NodeId replicaId,
                                         vector<uint8_t>& body) {
    body = broker.call(ApiKey::Fetch, encoded(fetchOrdersFrom(offset, replicaId)));
    BufferReader in(body);
    return decodeFetchResponse(in).topics[0].partitions[0];
}

}  // namespace

TEST_CASE("Produced records stay invisible until the follower has fetched them") {
    ServingBroker broker("handler-fetch-commits");

    // Created the way a peer's request creates it, so it is tracked from birth.
    CreateReplicasRequest::Topic topic;
    topic.settings.name = "orders";
    topic.partitions    = {{0, {1, 2}}};
    CreateReplicasRequest create;
    create.topics.push_back(topic);
    broker.call(ApiKey::CreateReplicas, encoded(create));

    const auto batch = makeUnstampedBatch(1000, 48);
    for (int i = 0; i < 3; ++i) broker.call(ApiKey::Produce, encoded(produceToOrders(batch)));

    // Written, but node 2 has none of it: a consumer sees nothing.
    vector<uint8_t> body;
    auto consumer = fetchOrders(broker, Offset(0), kConsumerReplicaId, body);
    CHECK(consumer.highWatermark == Offset(0));
    CHECK(consumer.records.empty());

    // Node 2 fetches at 2 — it has offsets 0 and 1 — and hears the new watermark
    // in this same response, not one round trip later.
    const auto follower = fetchOrders(broker, Offset(2), 2, body);
    CHECK(follower.highWatermark == Offset(2));

    consumer = fetchOrders(broker, Offset(0), kConsumerReplicaId, body);
    CHECK(consumer.highWatermark == Offset(2));
    CHECK(consumer.records.size() == 2 * batch.size());
}

TEST_CASE("A follower claiming records the leader lacks moves nothing") {
    ServingBroker broker("handler-fetch-overclaim");
    CreateReplicasRequest::Topic topic;
    topic.settings.name = "orders";
    topic.partitions    = {{0, {1, 2}}};
    CreateReplicasRequest create;
    create.topics.push_back(topic);
    broker.call(ApiKey::CreateReplicas, encoded(create));
    const auto batch = makeUnstampedBatch(1000, 48);
    broker.call(ApiKey::Produce, encoded(produceToOrders(batch)));

    vector<uint8_t> body;
    const auto      claim = fetchOrders(broker, Offset(9), 2, body);
    CHECK(claim.error == ErrorCode::OffsetOutOfRange);
    CHECK(claim.highWatermark == Offset(0));
}

TEST_CASE("A follower's copy refuses writes, and stays exactly as it was") {
    ServingBroker broker("handler-produce-follower");
    storage::LogConfig config   = testConfig();
    config.replication.replicas = {2, 1};   // node 2 leads; we only hold a copy
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0}, config);

    const auto   batch = makeUnstampedBatch(1000, 48);
    const auto   body  = broker.call(ApiKey::Produce, encoded(produceToOrders(batch)));
    BufferReader in(body);
    const auto   result = decodeProduceResponse(in).topics[0].partitions[0];

    // The client refreshes metadata and finds node 2. Had this been accepted,
    // offset 0 here would hold a record the leader never had.
    CHECK(result.error == ErrorCode::NotLeaderForPartition);
    CHECK(result.baseOffset == kUnknownOffset);
    CHECK(log.logEndOffset() == Offset(0));
}

TEST_CASE("A caught-up fetch is a success with no records") {
    ServingBroker broker("handler-fetch-caught-up");
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0});
    auto          bytes = makeUnstampedBatch(1000, 48);
    log.append(bytes);

    FetchRequest request;
    request.topics.push_back({"orders", {{0, Offset(1), 1 << 20}}});

    const auto   body = broker.call(ApiKey::Fetch, encoded(request));
    BufferReader in(body);
    const auto   view = decodeFetchResponse(in);

    // The most common fetch in the system: no error, no bytes, poll again.
    CHECK(view.topics[0].partitions[0].error == ErrorCode::None);
    CHECK(view.topics[0].partitions[0].records.empty());
    CHECK(view.topics[0].partitions[0].highWatermark == Offset(1));
}

TEST_CASE("An out-of-range fetch carries the offsets a client needs to recover") {
    ServingBroker      broker("handler-fetch-out-of-range");
    storage::LogConfig config = testConfig();
    config.roll.maxSegmentBytes  = 400;
    config.retention.retentionMs = 1;
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0}, config);
    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(100'000 + i, 48);
        log.append(bytes);
    }
    log.applyRetention(1'000'000);
    REQUIRE(log.logStartOffset() > Offset(0));

    FetchRequest request;
    request.topics.push_back({"orders", {{0, Offset(0), 1 << 20},          // too old
                                         {0, Offset(9999), 1 << 20}}});     // too new

    const auto   body = broker.call(ApiKey::Fetch, encoded(request));
    BufferReader in(body);
    const auto   view = decodeFetchResponse(in);

    // One code for both, Kafka-shaped — and the two offsets are how a client
    // works out which side it fell on.
    for (const auto& partition : view.topics[0].partitions) {
        CHECK(partition.error == ErrorCode::OffsetOutOfRange);
        CHECK(partition.records.empty());
        CHECK(partition.logStartOffset == log.logStartOffset());
        CHECK(partition.highWatermark == log.logEndOffset());
    }
}

TEST_CASE("A fetch across partitions answers what it can") {
    ServingBroker broker("handler-fetch-mixed");
    storage::Log& log = broker.logs.createPartition(TopicPartition{"orders", 0});
    for (int i = 0; i < 5; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log.append(bytes);
    }

    FetchRequest request;
    request.topics.push_back({"orders", {{0, Offset(0), 1 << 20}, {7, Offset(0), 1 << 20}}});

    const auto   body = broker.call(ApiKey::Fetch, encoded(request));
    BufferReader in(body);
    const auto   view = decodeFetchResponse(in);

    CHECK(view.topics[0].partitions[0].error == ErrorCode::None);
    CHECK_FALSE(view.topics[0].partitions[0].records.empty());
    CHECK(view.topics[0].partitions[1].error == ErrorCode::NotLeaderForPartition);
    CHECK(view.topics[0].partitions[1].records.empty());
}

TEST_CASE("What was produced is what is fetched back") {
    ServingBroker broker("handler-roundtrip");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    const auto     sent = makeUnstampedBatch(4242, 64, 2);
    ProduceRequest produce;
    produce.topics.push_back({"orders", {{0, span<const uint8_t>(sent), 0}}});
    broker.call(ApiKey::Produce, encoded(produce));

    FetchRequest fetch;
    fetch.topics.push_back({"orders", {{0, Offset(0), 1 << 20}}});
    const auto   body = broker.call(ApiKey::Fetch, encoded(fetch));
    BufferReader in(body);
    const auto   view = decodeFetchResponse(in);

    // Produce and fetch through the real handlers, with the storage layer in
    // between — the first time the two halves meet.
    const auto& records = view.topics[0].partitions[0].records;
    REQUIRE_FALSE(records.empty());
    CHECK(storage::RecordBatch::verifyCrc(records));

    const auto header = storage::RecordBatch::parseHeader(records);
    CHECK(header.baseOffset == Offset(0));
    CHECK(header.recordCount == 2);
    CHECK(header.maxTimestamp == 4243);
}

// ===========================================================================
// Sockets and connections
// ===========================================================================

namespace {

template <typename Predicate>
bool waitFor(Predicate predicate, chrono::milliseconds limit = chrono::milliseconds(5000)) {
    const auto deadline = chrono::steady_clock::now() + limit;
    while (chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        this_thread::sleep_for(chrono::milliseconds(1));
    }
    return predicate();
}

// A listening socket on a free port, plus a client connected to it and the
// server's end of that connection.
struct ConnectedPair {
    Socket listening;
    Socket client;
    Socket server;

    ConnectedPair()
        : listening(Socket::listenOn("127.0.0.1", 0)),
          client(Socket::connectTo("127.0.0.1", listening.localPort())),
          server(listening.accept()) {}
};

// Sends one request and reads one response, as a client would.
vector<uint8_t> roundTripOverSocket(int fd, ApiKey key, int32_t correlationId,
                                    const vector<uint8_t>& body) {
    BufferWriter out;
    encodeRequestHeader(out, headerFor(key, correlationId));
    out.writeBytes(body);
    const auto request = out.take();

    writeFrame(fd, request);
    const auto frame = readFrame(fd, kMaxFrameBytes);
    REQUIRE(frame.has_value());
    return *frame;
}

}  // namespace

TEST_CASE("A listening socket reports the port the OS gave it") {
    const Socket listening = Socket::listenOn("127.0.0.1", 0);

    // Asking for 0 and reading back what was bound is what keeps the suite safe
    // to run on a machine already using 9092, and safe to run twice at once.
    CHECK(listening.isOpen());
    CHECK(listening.localPort() > 0);
}

TEST_CASE("A socket is move-only and closes itself") {
    static_assert(!is_copy_constructible_v<Socket>,
                  "a descriptor has one owner; a double close hits whatever file took the number");
    static_assert(is_move_constructible_v<Socket>);

    int borrowed = -1;
    {
        Socket listening = Socket::listenOn("127.0.0.1", 0);
        borrowed         = listening.fd();
        Socket moved     = std::move(listening);
        CHECK_FALSE(listening.isOpen());
        CHECK(moved.fd() == borrowed);
    }
    // Closed by the destructor: connecting to a port nothing listens on fails.
    CHECK(borrowed >= 0);
}

TEST_CASE("Connecting to nothing fails rather than hanging") {
    const Socket listening = Socket::listenOn("127.0.0.1", 0);
    const int32_t port     = listening.localPort();
    const_cast<Socket&>(listening).close();

    CHECK_THROWS_AS(Socket::connectTo("127.0.0.1", port), IoError);
}

TEST_CASE("A timed connect to a closed port is refused at once, not timed out") {
    Socket        listening = Socket::listenOn("127.0.0.1", 0);
    const int32_t port      = listening.localPort();
    listening.close();

    // Through the non-blocking path: connect, poll, then SO_ERROR says why.
    const auto start = chrono::steady_clock::now();
    try {
        Socket::connectTo("127.0.0.1", port, 2'000);
        FAIL("connected to a closed port");
    } catch (const IoError& error) {
        CHECK(error.errnoValue() == ECONNREFUSED);
    }
    CHECK(chrono::steady_clock::now() - start < chrono::milliseconds(1'000));
}

TEST_CASE("A call to a broker that never answers times out instead of hanging") {
    // The kernel completes the handshake from the listen backlog, so the
    // connect succeeds — and then nothing ever reads the request or replies.
    // A peer in a long GC pause looks exactly like this.
    const Socket silent = Socket::listenOn("127.0.0.1", 0);
    protocol::Client client("127.0.0.1", silent.localPort(), "test", /*timeoutMs=*/100);

    const auto start = chrono::steady_clock::now();
    try {
        client.call(ApiKey::Metadata, {});
        FAIL("a broker that never answered was heard from");
    } catch (const IoError& error) {
        CHECK(error.errnoValue() == ETIMEDOUT);
    }
    const auto waited = chrono::steady_clock::now() - start;
    CHECK(waited >= chrono::milliseconds(90));
    CHECK(waited < chrono::milliseconds(2'000));
}

TEST_CASE("A connection serves requests until the client goes away") {
    ServingBroker broker("conn-serve");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    ConnectedPair pair;
    Connection    connection(std::move(pair.server), broker.registry, broker.context);
    thread        server([&] { connection.serve(); });

    // Three requests on one connection, answered in order.
    for (int32_t i = 1; i <= 3; ++i) {
        MetadataRequest request;
        request.allTopics = true;
        BufferWriter body;
        encodeMetadataRequest(body, request);

        const auto   frame = roundTripOverSocket(pair.client.fd(), ApiKey::Metadata, i,
                                                 body.take());
        BufferReader in(frame);
        CHECK(decodeResponseHeader(in) == i);
        const auto response = decodeMetadataResponse(in);
        CHECK(response.topics.size() == 1);
    }

    // Closing the client is how a connection normally ends; serve() returns.
    pair.client.close();
    server.join();
}

TEST_CASE("A produce and a fetch over one socket round-trip") {
    ServingBroker broker("conn-produce-fetch");
    broker.logs.createPartition(TopicPartition{"orders", 0});

    ConnectedPair pair;
    Connection    connection(std::move(pair.server), broker.registry, broker.context);
    thread        server([&] { connection.serve(); });

    const auto     batch = makeUnstampedBatch(7777, 64, 2);
    ProduceRequest produce;
    produce.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});
    BufferWriter produceBody;
    encodeProduceRequest(produceBody, produce);

    {
        const auto   frame = roundTripOverSocket(pair.client.fd(), ApiKey::Produce, 1,
                                                 produceBody.take());
        BufferReader in(frame);
        decodeResponseHeader(in);
        const auto response = decodeProduceResponse(in);
        CHECK(response.topics[0].partitions[0].error == ErrorCode::None);
        CHECK(response.topics[0].partitions[0].baseOffset == Offset(0));
    }

    FetchRequest fetch;
    fetch.topics.push_back({"orders", {{0, Offset(0), 1 << 20}}});
    BufferWriter fetchBody;
    encodeFetchRequest(fetchBody, fetch);

    {
        const auto   frame = roundTripOverSocket(pair.client.fd(), ApiKey::Fetch, 2,
                                                 fetchBody.take());
        BufferReader in(frame);
        decodeResponseHeader(in);
        const auto view = decodeFetchResponse(in);

        // Produced over a socket and fetched back over the same one — the record
        // bytes went out by sendfile and never entered the broker's memory.
        const auto& records = view.topics[0].partitions[0].records;
        REQUIRE_FALSE(records.empty());
        CHECK(storage::RecordBatch::verifyCrc(records));
        CHECK(storage::RecordBatch::parseHeader(records).recordCount == 2);
    }

    pair.client.close();
    server.join();
}

TEST_CASE("An unparseable body is answered and the connection survives") {
    ServingBroker broker("conn-bad-body");
    ConnectedPair pair;
    Connection    connection(std::move(pair.server), broker.registry, broker.context);
    thread        server([&] { connection.serve(); });

    // A well-framed request whose body is nonsense for its api key.
    const auto frame = roundTripOverSocket(pair.client.fd(), ApiKey::Fetch, 1,
                                           vector<uint8_t>{0xFF, 0xFF, 0xFF});
    BufferReader in(frame);
    CHECK(decodeResponseHeader(in) == 1);
    CHECK(static_cast<ErrorCode>(in.readInt16()) == ErrorCode::CorruptMessage);

    // And the next request still works. That is a property of length-prefixed
    // framing: a whole frame was consumed, so the next one begins where it
    // should. A delimiter-based protocol would have to hang up, not knowing
    // where the damage ended.
    MetadataRequest request;
    request.allTopics = true;
    BufferWriter body;
    encodeMetadataRequest(body, request);
    const auto   second = roundTripOverSocket(pair.client.fd(), ApiKey::Metadata, 2,
                                              body.take());
    BufferReader secondIn(second);
    CHECK(decodeResponseHeader(secondIn) == 2);

    pair.client.close();
    server.join();
}

TEST_CASE("An unknown api is answered and the connection survives") {
    ServingBroker broker("conn-unknown-api");
    ConnectedPair pair;
    Connection    connection(std::move(pair.server), broker.registry, broker.context);
    thread        server([&] { connection.serve(); });

    const auto   frame = roundTripOverSocket(pair.client.fd(), static_cast<ApiKey>(777), 9, {});
    BufferReader in(frame);
    CHECK(decodeResponseHeader(in) == 9);
    CHECK(static_cast<ErrorCode>(in.readInt16()) == ErrorCode::UnsupportedVersion);

    pair.client.close();
    server.join();
}

TEST_CASE("An oversized frame ends the connection rather than allocating") {
    ServingBroker broker("conn-oversized");
    ConnectedPair pair;
    Connection    connection(std::move(pair.server), broker.registry, broker.context, 1024);
    thread        server([&] { connection.serve(); });

    // A prefix claiming two gigabytes, and nothing after it. Where the next
    // frame begins is now unknowable, so hanging up is the only honest answer.
    const vector<uint8_t> hostile{0x7F, 0xFF, 0xFF, 0xFF};
    writeAll(pair.client.fd(), hostile);

    // Returning at all is the assertion: two gigabytes were never allocated.
    server.join();

    // And serve() returning does not itself close the socket — the Connection
    // owns it until destroyed. Without this the client would wait forever for a
    // hangup that never comes, which is exactly what the Acceptor has to get
    // right when a connection ends.
    connection.stop();
    CHECK_FALSE(readFrame(pair.client.fd(), kMaxFrameBytes).has_value());
}

TEST_CASE("A connection can be stopped from another thread") {
    ServingBroker broker("conn-stop");
    ConnectedPair pair;
    Connection    connection(std::move(pair.server), broker.registry, broker.context);
    thread        server([&] { connection.serve(); });

    // serve() is blocked in read(), which no flag would interrupt. shutdown()
    // makes the kernel report end of stream instead.
    connection.stop();
    server.join();
    CHECK(true);
}

// ===========================================================================
// Acceptor and Broker
// ===========================================================================

namespace {

// A broker on a free port, stopped when it goes out of scope.
struct RunningBroker {
    TempDir        dir;
    server::Broker broker;

    explicit RunningBroker(const string& name, storage::LogConfig config = testConfig())
        : dir(name), broker(makeOptions(dir.file("data"), config)) {
        broker.start();
    }
    ~RunningBroker() { broker.stop(); }

    Socket connect() { return Socket::connectTo("127.0.0.1", broker.port()); }

private:
    static server::Broker::Options makeOptions(const filesystem::path& dataDir,
                                               storage::LogConfig      config) {
        server::Broker::Options options;
        options.dataDir               = dataDir;
        options.defaults              = config;
        options.host                  = "127.0.0.1";
        options.port                  = 0;   // let the OS pick
        options.maintenanceIntervalMs = 1000;
        return options;
    }
};

}  // namespace

TEST_CASE("A broker listens on the port it was given and reports it") {
    RunningBroker broker("broker-listen");

    // Asked for 0, so the bound port is the only one that exists — and it is
    // what Metadata must advertise, not the one requested.
    CHECK(broker.broker.port() > 0);
    CHECK_NOTHROW(broker.connect());
}

TEST_CASE("A broker answers over a real socket") {
    RunningBroker broker("broker-answers");
    broker.broker.logs().createPartition(TopicPartition{"orders", 0});

    Socket client = broker.connect();

    MetadataRequest request;
    request.allTopics = true;
    BufferWriter body;
    encodeMetadataRequest(body, request);

    const auto   frame = roundTripOverSocket(client.fd(), ApiKey::Metadata, 1, body.take());
    BufferReader in(frame);
    CHECK(decodeResponseHeader(in) == 1);
    const auto response = decodeMetadataResponse(in);

    REQUIRE(response.brokers.size() == 1);
    CHECK(response.brokers[0].port == broker.broker.port());
    CHECK(response.topics[0].name == "orders");
}

TEST_CASE("Several clients are served at once") {
    RunningBroker broker("broker-concurrent");
    broker.broker.logs().createPartition(TopicPartition{"orders", 0});

    constexpr int kClients = 8;
    atomic<int>   answered{0};
    vector<thread> clients;

    for (int c = 0; c < kClients; ++c) {
        clients.emplace_back([&] {
            Socket client = broker.connect();
            for (int i = 0; i < 10; ++i) {
                const auto     batch = makeUnstampedBatch(1000 + i, 48);
                ProduceRequest produce;
                produce.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});
                BufferWriter body;
                encodeProduceRequest(body, produce);

                const auto   frame = roundTripOverSocket(client.fd(), ApiKey::Produce, i,
                                                         body.take());
                BufferReader in(frame);
                decodeResponseHeader(in);
                if (decodeProduceResponse(in).topics[0].partitions[0].error == ErrorCode::None)
                    answered.fetch_add(1, memory_order_relaxed);
            }
        });
    }
    for (auto& client : clients) client.join();

    // One thread per connection, all writing to one partition — which is safe
    // only because Log::append is the single appender and LogManager locks its
    // own registry. There is no broker-wide lock anywhere in this.
    CHECK(answered.load() == kClients * 10);
    CHECK(broker.broker.logs().get(TopicPartition{"orders", 0})->logEndOffset() ==
          Offset(kClients * 10));
}

TEST_CASE("Finished connections are reaped rather than accumulating") {
    RunningBroker broker("broker-reap");
    broker.broker.logs().createPartition(TopicPartition{"orders", 0});

    // Thirty short-lived connections. Without reaping, a broker keeps one thread
    // object per connection it has EVER served.
    for (int i = 0; i < 30; ++i) {
        Socket       client = broker.connect();
        MetadataRequest request;
        BufferWriter body;
        encodeMetadataRequest(body, request);
        roundTripOverSocket(client.fd(), ApiKey::Metadata, i, body.take());
        client.close();
    }

    // One more, to give the accept loop a chance to reap the rest.
    { Socket last = broker.connect(); }

    // One data partition plus __offsets, which the broker creates at startup.
    CHECK(broker.broker.logs().get(TopicPartition{"orders", 0}) != nullptr);
    CHECK(true);   // reaching here without exhausting threads is the assertion
}

TEST_CASE("Stopping a broker with clients connected is prompt and clean") {
    RunningBroker broker("broker-stop");
    broker.broker.logs().createPartition(TopicPartition{"orders", 0});

    vector<Socket> clients;
    for (int i = 0; i < 4; ++i) clients.push_back(broker.connect());

    // Every one of those has a thread blocked in read(), which notices no flag.
    // stop() closes the listening socket to end the accept loop, then shuts down
    // each connection so its read reports end of stream, then joins.
    const auto before = chrono::steady_clock::now();
    broker.broker.stop();
    const auto elapsed = chrono::steady_clock::now() - before;

    CHECK(elapsed < chrono::seconds(3));

    // And the clients see the hangup rather than waiting on a socket nothing
    // will ever write to.
    for (auto& client : clients)
        CHECK_FALSE(readFrame(client.fd(), kMaxFrameBytes).has_value());
}

TEST_CASE("Stopping twice is harmless") {
    RunningBroker broker("broker-stop-twice");
    CHECK_NOTHROW(broker.broker.stop());
    CHECK_NOTHROW(broker.broker.stop());
}

TEST_CASE("A broker restarts into the data it left behind") {
    TempDir dir("broker-restart");
    const filesystem::path dataDir = dir.file("data");

    server::Broker::Options options;
    options.dataDir               = dataDir;
    options.defaults              = testConfig();
    options.host                  = "127.0.0.1";
    options.port                  = 0;
    options.maintenanceIntervalMs = 1000;

    {
        server::Broker broker(options);
        broker.start();
        broker.logs().createPartition(TopicPartition{"orders", 0});

        Socket         client = broker.port() > 0 ? Socket::connectTo("127.0.0.1", broker.port())
                                                  : Socket{};
        const auto     batch  = makeUnstampedBatch(1000, 48, 3);
        ProduceRequest produce;
        produce.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});
        BufferWriter body;
        encodeProduceRequest(body, produce);
        roundTripOverSocket(client.fd(), ApiKey::Produce, 1, body.take());
        broker.stop();
    }

    // A second broker over the same directory: loadAll finds the partition, its
    // partition.meta supplies its config, and the records are still there.
    server::Broker restarted(options);
    restarted.start();

    Socket       client = Socket::connectTo("127.0.0.1", restarted.port());
    FetchRequest fetch;
    fetch.topics.push_back({"orders", {{0, Offset(0), 1 << 20}}});
    BufferWriter fetchBody;
    encodeFetchRequest(fetchBody, fetch);

    const auto   frame = roundTripOverSocket(client.fd(), ApiKey::Fetch, 1, fetchBody.take());
    BufferReader in(frame);
    decodeResponseHeader(in);
    const auto view = decodeFetchResponse(in);

    const auto& records = view.topics[0].partitions[0].records;
    REQUIRE_FALSE(records.empty());
    CHECK(storage::RecordBatch::verifyCrc(records));
    CHECK(storage::RecordBatch::parseHeader(records).recordCount == 3);
    restarted.stop();
}

TEST_CASE("Internal topics are hidden from a request for everything") {
    RunningBroker broker("broker-internal-topics");
    broker.broker.logs().createPartition(TopicPartition{"orders", 0});

    Socket          client = broker.connect();
    MetadataRequest everything;
    everything.allTopics = true;
    BufferWriter body;
    encodeMetadataRequest(body, everything);

    const auto   frame = roundTripOverSocket(client.fd(), ApiKey::Metadata, 1, body.take());
    BufferReader in(frame);
    decodeResponseHeader(in);
    const auto response = decodeMetadataResponse(in);

    // A client subscribing to every topic should not find itself consuming the
    // offsets of every other consumer.
    REQUIRE(response.topics.size() == 1);
    CHECK(response.topics[0].name == "orders");

    // But asked for by name, it is answered — the topic is not a secret, just not
    // part of "everything".
    Socket          second = broker.connect();
    MetadataRequest byName;
    byName.topics = {"__offsets"};
    BufferWriter namedBody;
    encodeMetadataRequest(namedBody, byName);

    const auto   namedFrame = roundTripOverSocket(second.fd(), ApiKey::Metadata, 2,
                                                  namedBody.take());
    BufferReader namedIn(namedFrame);
    decodeResponseHeader(namedIn);
    const auto named = decodeMetadataResponse(namedIn);
    REQUIRE(named.topics.size() == 1);
    CHECK(named.topics[0].name == "__offsets");
    CHECK(named.topics[0].partitions.size() == group::kDefaultOffsetsPartitions);
}

TEST_CASE("A broker tracks the partitions it leads from the moment it starts") {
    TempDir dir("broker-tracks-on-start");
    {
        // On disk from a previous life: led by node 1, followed by node 2.
        storage::LogManager logs(dir.file("data"), testConfig());
        storage::LogConfig  config  = testConfig();
        config.replication.replicas = {1, 2};
        logs.createPartition(TopicPartition{"orders", 0}, config);
    }

    server::Broker::Options options;
    options.dataDir  = dir.file("data");
    options.defaults = testConfig();
    options.host     = "127.0.0.1";
    options.port     = 0;
    options.nodeId   = 1;
    server::Broker broker(options);
    broker.start();

    // Tracked, so an append no longer commits itself: node 2 has not fetched.
    storage::Log* log   = broker.logs().get(TopicPartition{"orders", 0});
    auto          bytes = makeUnstampedBatch(1000);
    REQUIRE(log != nullptr);
    log->append(bytes);
    CHECK(log->highWatermark() == Offset(0));

    broker.stop();
}

TEST_CASE("A follower that never fetches is dropped by the sweep, not by a test") {
    TempDir dir("broker-sweeps-isr");
    {
        storage::LogManager logs(dir.file("data"), testConfig());
        storage::LogConfig  config  = testConfig();
        config.replication.replicas = {1, 2};
        logs.createPartition(TopicPartition{"orders", 0}, config);
    }

    server::Broker::Options options;
    options.dataDir             = dir.file("data");
    options.defaults            = testConfig();
    options.host                = "127.0.0.1";
    options.port                = 0;
    options.nodeId              = 1;
    options.sweepIntervalMs     = 10;
    options.replicaLagTimeMaxMs = 50;
    server::Broker broker(options);
    broker.start();

    storage::Log* log   = broker.logs().get(TopicPartition{"orders", 0});
    auto          bytes = makeUnstampedBatch(1000);
    REQUIRE(log != nullptr);
    log->append(bytes);
    REQUIRE(log->highWatermark() == Offset(0));   // node 2 has not fetched

    // Node 2 never does. Once its window runs out, the broker's own sweep drops
    // it, and the record it was holding back is committed. Nothing here calls
    // shrinkInSync: if the sweep did not, this would wait out the full two
    // seconds and fail.
    for (int waited = 0; waited < 200 && log->highWatermark() < Offset(1); ++waited)
        this_thread::sleep_for(chrono::milliseconds(10));
    CHECK(log->highWatermark() == Offset(1));

    broker.stop();
}
