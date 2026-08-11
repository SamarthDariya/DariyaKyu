#include "common/types.hpp"

using namespace std;

namespace dariyakyu {

string Offset::toString() const {
    return to_string(value_);
}

string TopicPartition::toString() const {
    return topic + "-" + to_string(partition);
}

}  // namespace dariyakyu
