// Copyright (c) 2019 Michael Vilim
// 
// This file is part of the bamboo library. It is currently hosted at
// https://github.com/mvilim/bamboo
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//    http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <json.hpp>

namespace bamboo {
namespace json {

using value_t = json::detail::value_t;

unique_ptr<Node> convert(std::istream& is, const ColumnFilter* column_filter) {
    json::json j;
    is >> j;
    JsonConverter converter;
    unique_ptr<Node> node = make_unique<IncompleteNode>();
    converter.convert(node, j);
    return node;
}

ObjType JsonConverter::type(json::json& datum) {
    switch (datum.type()) {
        case value_t::null:
            return ObjType::INCOMPLETE;
        case value_t::array:
            return ObjType::LIST;
        case value_t::object:
            return ObjType::RECORD;
        case value_t::discarded:
            throw std::runtime_error("Not sure how to handle discarded symbols");
        default:
            return ObjType::PRIMITIVE;
    }
}

FieldIterator JsonConverter::fields(json::json& datum) {
    return FieldIterator(datum);
}

ListIterator JsonConverter::get_list(json::json& datum) {
    return ListIterator(datum);
}

void JsonConverter::add_primitive(PrimitiveNode& v, json::json& datum) {
    switch (datum.type()) {
        case value_t::string:
            v.add(datum.get<std::string>());
            return;
        case value_t::boolean:
            v.add(datum.get<bool>());
            return;
        case value_t::number_unsigned:
            v.add(datum.get<uint64_t>());
            return;
        case value_t::number_integer:
            v.add(datum.get<int64_t>());
            return;
        case value_t::number_float:
            v.add(datum.get<double>());
            return;
        default:
            throw std::runtime_error("Unexpected primitive type");
    }
}

}  // namespace json
}  // namespace bamboo
