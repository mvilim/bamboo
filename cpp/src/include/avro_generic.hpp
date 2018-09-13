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

#include <Schema.hh>
#include <avro.hpp>
#include <columns.hpp>

using namespace avro;

namespace bamboo {
namespace avro {
namespace generic {

typedef std::pair<ValidSchema, GenericDatum> Pair;

typedef KeyValueIterator<const string, const GenericDatum&> FieldIteratorType;
typedef ValueIterator<const GenericDatum&> ListIteratorType;

static ObjType type(const GenericDatum& datum) {
    if (datum.isUnion()) {
        const NodePtr& schema = datum.value<GenericUnion>().schema();
        if (is_nullable_union(schema)) {
            return bamboo::avro::type(schema->leafAt(datum.unionBranch())->type());
        } else {
            std::invalid_argument("Mixed unions are not yet supported");
        }
    }
    return bamboo::avro::type(datum.type());
}

class FieldIterator final : public FieldIteratorType {
    size_t pos = -1;
    const GenericRecord& datum;

   public:
    FieldIterator(const GenericRecord& datum) : datum(datum){};

    ~FieldIterator() final override = default;

    virtual bool next() final override {
        return ++pos < datum.schema()->leaves();
    }

    virtual const string key() final override {
        return datum.schema()->nameAt(pos);
    }

    virtual const GenericDatum& value() final override {
        return datum.fieldAt(pos);
    }
};

class ListIterator final : public ListIteratorType {
    const vector<GenericDatum>& datum;
    size_t pos = -1;

    // it's possible we could use this size information to reserve vector
    // space (under some assumptions of nullity)
   public:
    ListIterator(const vector<GenericDatum>& datum) : datum(datum){};

    ~ListIterator() final override = default;

    virtual bool next() final override {
        return ++pos < datum.size();
    };

    virtual const GenericDatum& value() final override {
        return datum[pos];
    }
};

class AvroConverter final
    : public virtual Converter<const GenericDatum&, FieldIterator, ListIterator> {
   public:
    virtual ObjType type(const GenericDatum& datum) final override;

    virtual FieldIterator fields(const GenericDatum& datum) final override;

    virtual ListIterator get_list(const GenericDatum& datum) final override;

    virtual void add_primitive(PrimitiveNode& v, const GenericDatum& datum) final override;

    virtual ~AvroConverter() final override = default;
};

unique_ptr<Node> convert_with_schema(std::istream& is, const ValidSchema& schema);

unique_ptr<Node> convert(std::istream& is);

}  // namespace generic
}  // namespace avro
}  // namespace bamboo
