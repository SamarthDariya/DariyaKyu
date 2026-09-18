#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "protocol/error_codes.hpp"
#include "server/api_registry.hpp"
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
