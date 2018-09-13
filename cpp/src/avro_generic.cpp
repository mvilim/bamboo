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
#include <Generic.hh>
#include <avro_generic.hpp>

namespace bamboo {
namespace avro {
namespace generic {

ObjType AvroConverter::type(const GenericDatum& datum) {
    return bamboo::avro::generic::type(datum);
}

static const GenericDatum& resolve_union(const GenericDatum& datum) {
    if (datum.isUnion()) {
        return datum.value<GenericUnion>().datum();
    }
    return datum;
}

FieldIterator AvroConverter::fields(const GenericDatum& datum) {
    // should also handle map
    const GenericDatum& d = resolve_union(datum);
    if (d.type() == AVRO_RECORD) {
        const GenericRecord& gr = d.value<GenericRecord>();
        return FieldIterator(gr);
    } else {
        throw std::invalid_argument("Expected record type");
    }
}

ListIterator AvroConverter::get_list(const GenericDatum& datum) {
    // should also handle map
    const GenericDatum& d = resolve_union(datum);
    if (d.type() == AVRO_ARRAY) {
        const vector<GenericDatum>& vec = d.value<GenericArray>().value();
        return ListIterator(vec);
    } else if (d.type() == AVRO_MAP) {
        throw std::logic_error("Not implemented");
    } else {
        throw std::invalid_argument("Expected list type");
    }
}

void AvroConverter::add_primitive(PrimitiveNode& v, const GenericDatum& datum) {
    // need to add safety checking
    const GenericDatum& d = resolve_union(datum);
    switch (d.type()) {
        case AVRO_BYTES:
            v.add(d.value<AvroPrimitiveType<AVRO_BYTES>::primitive_type>());
            break;
        case AVRO_INT: {
            v.add(d.value<AvroPrimitiveType<AVRO_INT>::primitive_type>());
        } break;
        case AVRO_LONG:
            v.add(d.value<AvroPrimitiveType<AVRO_LONG>::primitive_type>());
            break;
        case AVRO_FIXED: {
            v.add(d.value<GenericFixed>().value());
            break;
        }
        case AVRO_FLOAT:
            v.add(d.value<AvroPrimitiveType<AVRO_FLOAT>::primitive_type>());
            break;
        case AVRO_DOUBLE:
            v.add(d.value<AvroPrimitiveType<AVRO_DOUBLE>::primitive_type>());
            break;
        case AVRO_BOOL:
            v.add(d.value<AvroPrimitiveType<AVRO_BOOL>::primitive_type>());
            break;
        case AVRO_STRING: {
            v.add(d.value<AvroPrimitiveType<AVRO_STRING>::primitive_type>());
            break;
        }
        case AVRO_ENUM: {
            const GenericEnum& e = d.value<GenericEnum>();
            v.add(DynamicEnumValue(e.value(), std::make_shared<AvroEnum>(e.schema())));
            break;
        }
        default:
            throw std::invalid_argument("Expected primitive type");
    }
}

unique_ptr<Node> convert(std::istream& is, boost::optional<const ValidSchema> schema) {
    auto rb = std::auto_ptr<DataFileReaderBase>(new DataFileReaderBase(is, "unidentified stream"));
    unique_ptr<DataFileReader<Pair>> r =
        schema ? bamboo::make_unique<DataFileReader<Pair>>(rb, schema.get())
               : bamboo::make_unique<DataFileReader<Pair>>(rb);
    Pair p(r->readerSchema(), GenericDatum());

    AvroConverter converter;
    unique_ptr<ListNode> node = bamboo::make_unique<ListNode>();

    size_t counter = 0;
    while (r->read(p)) {
        const GenericDatum& ci = p.second;
        converter.convert(node->get_list(), ci);
        counter++;
    }
    node->add_list(counter);
    r->close();
    return std::move(node);
}

unique_ptr<Node> convert_with_schema(std::istream& is, const ValidSchema& schema) {
    return convert(is, boost::optional<const ValidSchema>(schema));
}

unique_ptr<Node> convert(std::istream& is) {
    return convert(is, boost::optional<const ValidSchema>());
}

}  // namespace generic
}  // namespace avro
}  // namespace bamboo
