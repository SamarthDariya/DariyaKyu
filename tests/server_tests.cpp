#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "protocol/error_codes.hpp"
#include "protocol/create_topic.hpp"
#include "protocol/list_offsets.hpp"
#include "protocol/metadata.hpp"
#include "server/api_registry.hpp"
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
    TempDir              dir;
    storage::LogManager  logs;
    BrokerContext        context;

    explicit BareBroker(const string& name)
        : dir(name), logs(dir.file("data"), testConfig()), context{logs, 1, "127.0.0.1", 9092} {}
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
    request.topics.push_back({"orders", 4, {}, {}, {}});

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
    request.topics.push_back({"orders", 2, {}, {}, {}});
    broker.call(ApiKey::CreateTopic, encoded(request));
    REQUIRE(broker.logs.partitionCount() == 2);

    // Asked for four this time. Checked for EVERY partition before anything is
    // created — three of four would leave a topic that half exists, which no
    // error code describes and no client could act on.
    CreateTopicRequest again;
    again.topics.push_back({"orders", 4, {}, {}, {}});
    const auto   body = broker.call(ApiKey::CreateTopic, encoded(again));
    BufferReader in(body);
    const auto   response = decodeCreateTopicResponse(in);

    CHECK(response.topics[0].error == ErrorCode::TopicAlreadyExists);
    CHECK(broker.logs.partitionCount() == 2);
}

TEST_CASE("A topic name that cannot be a directory name is refused") {
    ServingBroker broker("handler-createtopic-bad-name");

    CreateTopicRequest request;
    request.topics.push_back({"../escape", 1, {}, {}, {}});
    request.topics.push_back({"has space", 1, {}, {}, {}});
    request.topics.push_back({"", 1, {}, {}, {}});
    request.topics.push_back({"orders", 0, {}, {}, {}});   // no partitions

    const auto   body = broker.call(ApiKey::CreateTopic, encoded(request));
    BufferReader in(body);
    const auto   response = decodeCreateTopicResponse(in);

    // The boundary where a client's string becomes a filesystem path, checked
    // before anything is created.
    for (const auto& topic : response.topics) CHECK(topic.error == ErrorCode::InvalidTopic);
    CHECK(broker.logs.partitionCount() == 0);
    CHECK(filesystem::is_empty(broker.logs.dataDir()));
}
