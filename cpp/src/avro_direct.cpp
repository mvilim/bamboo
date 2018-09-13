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

#include <DataFile.hh>
#include <avro_direct.hpp>

namespace ba = bamboo::avro;

namespace bamboo {
namespace avro {
namespace direct {

// this union checking adds a non-inconsequential cost for tight loops on simple datamodels. it
// would be better if we could check it with closer to zero cost (perhaps inside the type classifier)
static const CNode& resolve_if_union(const CNode& datum) {
    if (datum.type() == AVRO_UNION) {
        return resolve_union(datum);
    }
    return datum;
}

// should pull out the shared pieces of the avro decoder
const CNode& AvroDirectConverter::read_union(const CNode& datum) {
    return datum.leafAt(decoder.decodeUnionIndex());
}

ObjType AvroDirectConverter::type(const CNode& datum) {
    if (datum.type() == AVRO_UNION) {
        return type(read_union(datum));
    }
    return ba::type(datum.type());
}

FieldIterator AvroDirectConverter::fields(const CNode& datum) {
    const CNode& resolved = resolve_if_union(datum);
    // should handle out of order fields
    if (resolved.type() == AVRO_RECORD) {
        return FieldIterator(resolved);
    } else {
        throw std::invalid_argument("Expected record type");
    }
}

ListIterator AvroDirectConverter::get_list(const CNode& datum) {
    // should also handle map
    const CNode& resolved = resolve_if_union(datum);
    Type type = resolved.type();
    if (type == AVRO_ARRAY) {
        return ListIterator(decoder, datum);
    } else if (type == AVRO_MAP) {
        throw std::logic_error("Not implemented");
    } else {
        throw std::invalid_argument("Expected list type");
    }
}

void AvroDirectConverter::add_primitive(PrimitiveNode& node, const CNode& datum) {
    const CNode& resolved = resolve_if_union(datum);
    ba::add_primitive(resolved, node, decoder);
}

// should share with FSM
void initialize(const NodePtr& schema, unique_ptr<Node>& node) {
    switch (schema->type()) {
        case AVRO_RECORD: {
            node = make_unique<RecordNode>();
            RecordNode& record_node = *static_cast<RecordNode*>(node.get());
            for (size_t i = 0; i < schema->leaves(); i++) {
                unique_ptr<Node>& field_node = record_node.get_field(schema->nameAt(i));
                initialize(schema->leafAt(i), field_node);
            }
            break;
        }
        case AVRO_ARRAY: {
            node = make_unique<ListNode>();
            ListNode& list_node = *static_cast<ListNode*>(node.get());
            initialize(schema->leafAt(0), list_node.get_list());
            break;
        }
        case AVRO_UNION: {
            initialize(resolve_union(schema), node);
            break;
        }
        default:
            break;
    }
}

unique_ptr<Node> convert(std::istream& is, boost::optional<const ValidSchema> schema) {
    DataFileReaderBase rb(is, "unidentified stream");
    if (schema) {
        rb.init(schema.get());
    } else {
        rb.init();
    }

    AvroDirectConverter converter(rb.decoder());
    unique_ptr<ListNode> node = make_unique<ListNode>();
    initialize(rb.readerSchema().root(), node->get_list());
    size_t counter = 0;
    const CNode cnode(rb.readerSchema().root());
    while (rb.hasMore()) {
        rb.decr();
        converter.convert(node->get_list(), cnode);
        counter++;
    }
    node->add_list(counter);
    node->add_not_null();
    rb.close();
    return std::move(node);
}

unique_ptr<Node> convert_with_schema(std::istream& is, const ValidSchema& schema) {
    return convert(is, boost::optional<const ValidSchema>(schema));
}

unique_ptr<Node> convert(std::istream& is) {
    return convert(is, boost::optional<const ValidSchema>());
}

}  // namespace direct
}  // namespace avro
}  // namespace bamboo
