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

#include <columns.hpp>

#include <Decoder.hh>
#include <Schema.hh>

using namespace avro;

namespace bamboo {
namespace avro {

static bool is_nullable_union(const NodePtr& schema) {
    size_t num_null = 0;
    for (size_t i = 0; i < schema->leaves(); i++) {
        if (schema->leafAt(i)->type() == AVRO_NULL) {
            num_null++;
        }
    }
    return num_null == 1 && schema->leaves() == 2;
}

static size_t non_null_branch(const NodePtr& schema) {
    size_t num_null = 0;
    size_t non_null_branch = 0;
    for (size_t i = 0; i < schema->leaves(); i++) {
        if (schema->leafAt(i)->type() == AVRO_NULL) {
            num_null++;
        } else {
            non_null_branch = i;
        }
    }
    if (num_null == 1 && schema->leaves() == 2) {
        return non_null_branch;
    } else {
        throw std::invalid_argument("Union schemas not supported");
    }
}

static ObjType type(Type type) {
    switch (type) {
        case AVRO_STRING:
        case AVRO_BYTES:
        case AVRO_INT:
        case AVRO_LONG:
        case AVRO_FLOAT:
        case AVRO_DOUBLE:
        case AVRO_BOOL:
        case AVRO_ENUM:
        case AVRO_FIXED:
            return ObjType::PRIMITIVE;
        case AVRO_ARRAY:
        case AVRO_MAP:
            return ObjType::LIST;
        case AVRO_RECORD:
            return ObjType::RECORD;
        case AVRO_NULL:
            return ObjType::INCOMPLETE;
        case AVRO_UNION:
        case AVRO_NUM_TYPES:
        case AVRO_UNKNOWN:
        default:
            throw std::runtime_error("Unexpected avro type");
    }
}

struct AvroEnum final : public DynamicEnum {
    // making this a reference does not work because that reference can become invalid
    // but making this an actual shared pointer will be really slow (because we create one everytime
    // we decode an enum) need to find a way to avoid taking possession of the shared pointer except
    // at the beginning)
    const NodePtr schema;

    unique_ptr<PrimitiveSimpleVector<string>> enum_values;

    AvroEnum(const NodePtr& schema) : schema(schema){};

    virtual PrimitiveVector& get_enums() override {
        // lazily compute the values of the enum
        if (!enum_values) {
            enum_values = make_unique<PrimitiveSimpleVector<string>>();
            for (size_t i = 0; i < schema->names(); i++) {
                enum_values->add(schema->nameAt(i));
            }
        }
        return *enum_values;
    }

    virtual const void* source() override {
        return schema.get();
    };
};

template <class P> struct AvroPrimitiveTyper { typedef P primitive_type; };
template <Type T> struct AvroPrimitiveType;

// these are also incorrect when reading generic datum (e.g. enum is a GenericEnum, not a size_t)

// there is duplication between the decoded type and the vector type -- can we remove this
// duplication?
template <> struct AvroPrimitiveType<AVRO_STRING> : AvroPrimitiveTyper<string> {};
template <> struct AvroPrimitiveType<AVRO_BYTES> : AvroPrimitiveTyper<vector<uint8_t>> {};
template <> struct AvroPrimitiveType<AVRO_INT> : AvroPrimitiveTyper<int32_t> {};
template <> struct AvroPrimitiveType<AVRO_LONG> : AvroPrimitiveTyper<int64_t> {};
template <> struct AvroPrimitiveType<AVRO_FLOAT> : AvroPrimitiveTyper<float> {};
template <> struct AvroPrimitiveType<AVRO_DOUBLE> : AvroPrimitiveTyper<double> {};
template <> struct AvroPrimitiveType<AVRO_BOOL> : AvroPrimitiveTyper<bool> {};
template <> struct AvroPrimitiveType<AVRO_FIXED> : AvroPrimitiveTyper<vector<uint8_t>> {};
template <> struct AvroPrimitiveType<AVRO_ENUM> : AvroPrimitiveTyper<size_t> {};

}  // namespace avro
}  // namespace bamboo
