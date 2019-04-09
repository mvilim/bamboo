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

#include <avro_decoder.hpp>

using namespace avro;

namespace bamboo {
namespace avro {
namespace direct {

// we could move all of this into the implementation
class FieldIterator {
    size_t pos = -1;
    const CNode& datum;
    size_t limit;

   public:
    FieldIterator(const CNode& datum) : datum(datum), limit(datum.leaves()){};

    bool next() {
        return ++pos < limit;
    }

    size_t key() {
        return pos;
    }

    const CNode& value() {
        return datum.leafAt(pos);
    }
};

class ListIterator {
    Decoder& decoder;
    const CNode& element_schema;
    size_t remaining;
    bool try_next;

    // it's possible we could use this size information to reserve vector
    // space (under some assumptions of nullity)
   public:
    ListIterator(Decoder& decoder, const CNode& datum)
        : decoder(decoder), element_schema(datum.leafAt(0)), remaining(decoder.arrayStart()) {
        try_next = remaining > 0;
    };

    bool next() {
        if (remaining == 0 && try_next) {
            remaining += decoder.arrayNext();
        }

        return remaining-- > 0;
    };

    const CNode& value() {
        return element_schema;
    }
};

class AvroDirectConverter final : public Converter<const CNode&, FieldIterator, ListIterator> {
   private:
    Decoder& decoder;

   public:
    AvroDirectConverter(Decoder& decoder) : decoder(decoder){};

    virtual ~AvroDirectConverter() final override = default;

    virtual ObjType type(const CNode& datum) final override;

    virtual FieldIterator fields(const CNode& datum) final override;

    virtual ListIterator get_list(const CNode& datum) final override;

    virtual void add_primitive(PrimitiveNode& v, const CNode& datum) final override;

    const CNode& read_union(const CNode& datum);
};

unique_ptr<Node> convert(std::istream& is, const ColumnFilter* column_filter);

}  // namespace direct
}  // namespace avro
}  // namespace bamboo
