#include "cluster/cluster_config.hpp"

#include <algorithm>
#include <charconv>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::cluster {

namespace {

// The whole of `text` as an int32, or an error naming what was being parsed.
// from_chars alone would accept "92x" as 92, so it must also consume every byte.
int32_t parseNumber(string_view text, string_view entry, const char* what) {
    int32_t value = 0;
    const auto [end, ec] = from_chars(text.data(), text.data() + text.size(), value);
    if (ec != errc{} || end != text.data() + text.size() || text.empty())
        throw Error("cluster: bad " + string(what) + " in '" + string(entry) + "'");
    return value;
}

// One "id@host:port". rfind for the colon, so a host cannot hide one from us.
NodeAddress parseNode(string_view entry) {
    const size_t at    = entry.find('@');
    const size_t colon = entry.rfind(':');
    if (at == string_view::npos || colon == string_view::npos || colon < at)
        throw Error("cluster: expected id@host:port, got '" + string(entry) + "'");

    NodeAddress node;
    node.id   = parseNumber(entry.substr(0, at), entry, "node id");
    node.host = string(entry.substr(at + 1, colon - at - 1));
    node.port = parseNumber(entry.substr(colon + 1), entry, "port");

    if (node.id < 0) throw Error("cluster: node ids are never negative, got '" + string(entry) + "'");
    if (node.host.empty()) throw Error("cluster: no host in '" + string(entry) + "'");
    if (node.port <= 0 || node.port > 65535)
        throw Error("cluster: port out of range in '" + string(entry) + "'");
    return node;
}

}  // namespace

const NodeAddress* ClusterConfig::find(NodeId id) const {
    for (const auto& node : nodes)
        if (node.id == id) return &node;
    return nullptr;
}

ClusterConfig parseClusterConfig(string_view spec) {
    ClusterConfig config;
    while (!spec.empty()) {
        const size_t comma = spec.find(',');
        config.nodes.push_back(parseNode(spec.substr(0, comma)));
        spec = comma == string_view::npos ? string_view{} : spec.substr(comma + 1);
    }
    if (config.nodes.empty()) throw Error("cluster: no nodes given");

    // Sorted by id, so two brokers handed the same nodes in a different order
    // still agree on every assignment. Then a duplicate is a neighbour.
    ranges::sort(config.nodes, {}, &NodeAddress::id);
    for (size_t i = 1; i < config.nodes.size(); ++i)
        if (config.nodes[i].id == config.nodes[i - 1].id)
            throw Error("cluster: node id " + to_string(config.nodes[i].id) + " appears twice");
    return config;
}

}  // namespace dariyakyu::cluster
