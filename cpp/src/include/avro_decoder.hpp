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

#pragma once

#include <avro.hpp>

using namespace avro;

namespace bamboo {
namespace avro {

// this is a simplified version of the avro schema that can be used to improve conversion
// performance
class CNode {
    Type a_type;
    vector<CNode> vec;
    const NodePtr& source_node;

   public:
    CNode(const NodePtr& node) : source_node(node), a_type(node->type()) {
        for (size_t i = 0; i < node->leaves(); i++) {
            vec.emplace_back(node->leafAt(i));
        }
    }

    Type type() const {
        return a_type;
    }

    const CNode& leafAt(size_t index) const {
        return vec[index];
    }

    size_t leaves() const {
        return vec.size();
    }

    const NodePtr& source() const {
        return source_node;
    }
};

static ObjType type(const CNode& t) {
    return type(t.type());
}

static const NodePtr& resolve_union(const NodePtr& datum) {
    return datum->leafAt(non_null_branch(datum));
}

static size_t non_null_branch(const CNode& schema) {
    size_t num_null = 0;
    size_t non_null_branch = 0;
    for (size_t i = 0; i < schema.leaves(); i++) {
        if (schema.leafAt(i).type() == AVRO_NULL) {
            num_null++;
        } else {
            non_null_branch = i;
        }
    }
    if (num_null == 1 && schema.leaves() == 2) {
        return non_null_branch;
    } else {
        throw std::invalid_argument("Union schemas not supported");
    }
}

static const CNode& resolve_union(const CNode& datum) {
    return datum.leafAt(non_null_branch(datum));
}

template <Type T, typename AvroPrimitiveType<T>::primitive_type (Decoder::*F)()>
struct DecodingType {
    static typename AvroPrimitiveType<T>::primitive_type decode(Decoder& d) {
        return (d.*F)();
    }
};

template <Type T>
struct AvroType : AvroPrimitiveType<T>,
                  PrimitiveEnum<typename AvroPrimitiveType<T>::primitive_type> {};

template <Type T, typename AvroPrimitiveType<T>::primitive_type (Decoder::*)()> struct Asdf;
template <Type T, typename Q, Q> struct ArgDecodingType {};

template <Type T, typename... Args,
          typename AvroPrimitiveType<T>::primitive_type (Decoder::*F)(Args...)>
struct ArgDecodingType<T, typename AvroPrimitiveType<T>::primitive_type (Decoder::*)(Args...), F> {
    static typename AvroPrimitiveType<T>::primitive_type decode(Decoder& d, Args&&... args) {
        return (d.*F)(std::forward<Args>(args)...);
    }
};

template <> struct AvroType<AVRO_STRING> : DecodingType<AVRO_STRING, &Decoder::decodeString> {};
template <> struct AvroType<AVRO_BYTES> : DecodingType<AVRO_BYTES, &Decoder::decodeBytes> {};
template <> struct AvroType<AVRO_INT> : DecodingType<AVRO_INT, &Decoder::decodeInt> {};
template <> struct AvroType<AVRO_LONG> : DecodingType<AVRO_LONG, &Decoder::decodeLong> {};
template <> struct AvroType<AVRO_FLOAT> : DecodingType<AVRO_FLOAT, &Decoder::decodeFloat> {};
template <> struct AvroType<AVRO_DOUBLE> : DecodingType<AVRO_DOUBLE, &Decoder::decodeDouble> {};
template <> struct AvroType<AVRO_BOOL> : DecodingType<AVRO_BOOL, &Decoder::decodeBool> {};
template <>
struct AvroType<AVRO_FIXED>
    : ArgDecodingType<AVRO_FIXED, vector<uint8_t> (Decoder::*)(size_t), &Decoder::decodeFixed> {};
template <> struct AvroType<AVRO_ENUM> : DecodingType<AVRO_ENUM, &Decoder::decodeEnum> {};

// should make an "add unsafe" version
template <Type T, class... Args>
static void add_primitive(PrimitiveNode& node, Decoder& decoder, Args... args) {
    node.add(AvroType<T>::decode(decoder, std::forward<Args>(args)...));
}

static void add_primitive(const CNode& schema, PrimitiveNode& node, Decoder& decoder) {
    switch (schema.type()) {
        case AVRO_BYTES:
            add_primitive<AVRO_BYTES>(node, decoder);
            break;
        case AVRO_INT:
            add_primitive<AVRO_INT>(node, decoder);
            break;
        case AVRO_LONG:
            add_primitive<AVRO_LONG>(node, decoder);
            break;
        case AVRO_FIXED:
            add_primitive<AVRO_FIXED>(node, decoder, schema.source()->fixedSize());
            break;
        case AVRO_FLOAT:
            add_primitive<AVRO_FLOAT>(node, decoder);
            break;
        case AVRO_DOUBLE:
            add_primitive<AVRO_DOUBLE>(node, decoder);
            break;
        case AVRO_BOOL:
            add_primitive<AVRO_BOOL>(node, decoder);
            break;
        case AVRO_STRING:
            add_primitive<AVRO_STRING>(node, decoder);
            break;
        case AVRO_ENUM: {
            node.add(DynamicEnumValue(AvroType<AVRO_ENUM>::decode(decoder),
                                      std::make_shared<AvroEnum>(schema.source())));
            break;
        }
        default:
            throw std::invalid_argument("Expected record type");
    }
}

}  // namespace avro
}  // namespace bamboo
