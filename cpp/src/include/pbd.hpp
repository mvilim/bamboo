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
#include <pbd/pbd.hpp>

namespace bamboo {
namespace pbd {

namespace pb = google::protobuf;

struct Datum {
    const pb::Message& message;
    const pb::FieldDescriptor* field = nullptr;
    int64_t counter = -1;

    Datum(const pb::Message& message) : message(message){};

    Datum(const pb::Message& message, const pb::FieldDescriptor* field)
        : message(message), field(field), counter(0){};
};

typedef KeyValueIterator<const string, Datum&> FieldIteratorType;
typedef ValueIterator<Datum&> ListIteratorType;

static inline Datum selectDatum(Datum& datum) {
    if (datum.field) {
        if (datum.field->is_repeated()) {
            const pb::Message& message = datum.message.GetReflection()->GetRepeatedMessage(
                datum.message, datum.field, datum.counter);
        }
        const pb::Message& message =
            datum.message.GetReflection()->GetMessage(datum.message, datum.field);
        return Datum(message);

    } else {
        return datum;
    }
}

class FieldIterator final : public FieldIteratorType {
    Datum datum;
    size_t field_counter = 0;

   public:
    FieldIterator(Datum& datum) : datum(selectDatum(datum)){};

    virtual ~FieldIterator() final override = default;

    virtual bool next() final override {
        if (field_counter < datum.message.GetDescriptor()->field_count()) {
            datum.field = datum.message.GetDescriptor()->field(field_counter++);
            return true;
        }
        return false;
    }

    // should probably change this to be an index
    virtual const string key() final override {
        return datum.field->name();
    }

    virtual Datum& value() final override {
        return datum;
    }
};

class ListIterator final : public ListIteratorType {
    size_t count;
    Datum datum;

   public:
    ListIterator(Datum& datum)
        : datum(Datum(datum.message, datum.field)),
          count(datum.message.GetReflection()->FieldSize(datum.message, datum.field)){};

    virtual ~ListIterator() final override = default;

    virtual bool next() override {
        if (!datum.field || !datum.field->is_repeated()) {
            throw std::runtime_error("Not a repeated field");
        }

        return datum.counter < count;
    };

    virtual Datum& value() override {
        return datum;
    }
};

class PBDConverter final : public virtual Converter<Datum&, FieldIterator, ListIterator> {
   public:
    virtual ObjType type(Datum& datum) final override;

    virtual FieldIterator fields(Datum& datum) final override;

    virtual ListIterator get_list(Datum& datum) final override;

    virtual void add_primitive(PrimitiveNode& v, Datum& datum) final override;

    virtual ~PBDConverter() final override = default;
};

unique_ptr<Node> convert(std::istream& is);

}  // namespace pbd
}  // namespace bamboo
