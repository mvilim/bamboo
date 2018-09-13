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

#include <columns.hpp>

namespace bamboo {

void PrimitiveEnumVector::add(const DynamicEnumValue& t) {
    if (enums.values.get() == NULL) {
        enums.values = t.values;
    }

    if (enums.values->same_source(*t.values)) {
        enums.index.push_back(t.index);
    } else {
        throw std::logic_error("Mixed enums not implemented");
    }
}

void NullIndicator::add_null() {
    index.push_back(size);
    size++;
}

void NullIndicator::add_not_null() {
    size++;
}

const DynamicEnumVector& PrimitiveVector::get_enums() {
    if (type == PrimitiveType::ENUM) {
        return static_cast<PrimitiveEnumVector&>(*this).get_enums_vector();
    } else {
        throw std::logic_error("Attempted to access values with wrong type");
    }
}


PrimitiveType PrimitiveNode::get_type() {
    return values->type;
}

unique_ptr<PrimitiveVector>& PrimitiveNode::get_vector() {
    return values;
}

unique_ptr<Node>& ListNode::get_list() {
    return child;
}

void ListNode::add_list(size_t length) {
    index.push_back(length);
};

const vector<size_t>& ListNode::get_index() {
    return index;
}

void RecordNode::add_field(const string& name) {
    fields_map[name] = make_unique<IncompleteNode>();
    field_vec.push_back(reference_wrapper<unique_ptr<Node>>(fields_map[name]));
    field_names.push_back(name);
}

RecordNode::RecordNode(vector<string> names) : RecordNode() {
    for (const string& name : names) {
        add_field(name);
    }
}

unique_ptr<Node>& RecordNode::get_field(size_t index) {
    return field_vec[index];
}

unique_ptr<Node>& RecordNode::get_field(const string& name) {
    // for those formats that have a schema, we could probably find a way to
    // optimize away this check
    unique_ptr<Node>& node_field = fields_map[name];
    if (!node_field) {
        add_field(name);
    }
    return node_field;
}

const vector<string> RecordNode::get_fields() {
    // return keys(fields_map);
    return field_names;
}

}  // namespace bamboo_cpp
