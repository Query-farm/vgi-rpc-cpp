#pragma once

#include <string>
#include <unordered_map>

namespace vgi_rpc {

struct MethodInfo;

// Register the __describe__ handler in the method map.
// Called by ServerBuilder::build() when describe is enabled.
void register_describe(
    std::unordered_map<std::string, MethodInfo>& methods,
    const std::string& protocol_name,
    const std::string& server_id);

}  // namespace vgi_rpc
