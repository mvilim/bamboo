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

using Limit = pb::io::CodedInputStream::Limit;
using WireType = pb::internal::WireFormatLite::WireType;

struct FieldDescriptor;
struct MessageDescriptor;

struct FieldDescriptor {
    const shared_ptr<const MessageDescriptor> message_type;
    const pb::FieldDescriptor* pb_field;
    const int index;

    FieldDescriptor(const pb::FieldDescriptor* pb_field, int index,
                    const ColumnFilter* column_filter, bool implicit_include);

    bool has_fields() const;
};

struct MessageDescriptor {
    const pb::Descriptor* pb_descriptor;
    vector<shared_ptr<FieldDescriptor>> fields;
    // the optimal map type here depends on the access pattern
    map<int, FieldDescriptor*> number_to_field;

    MessageDescriptor(const pb::Descriptor* pb_descriptor, const ColumnFilter* column_filter,
                      bool implicit_include);

    void add_field(const pb::FieldDescriptor* field, const ColumnFilter* column_filter,
                   bool implicit_include);

    bool has_fields() const;
};

struct Datum {
    pb::io::CodedInputStream& stream;
    const MessageDescriptor* descriptor;

    int message_size = -1;
    vector<bool> field_processed;
    bool reading_list = false;
    bool reading_missing;

    const FieldDescriptor* field;
    uint32_t current_tag;
    uint32_t read_ahead_tag = 0;

    Datum(pb::io::CodedInputStream& stream, const MessageDescriptor* descriptor,
          bool reading_missing = false)
        : Datum(stream, descriptor, nullptr, reading_missing){};

    Datum(pb::io::CodedInputStream& stream, const MessageDescriptor* descriptor,
          const FieldDescriptor* field, bool reading_missing = false)
        : stream(stream), descriptor(descriptor), field(field), reading_missing(reading_missing) {
        // can reset with
        // field_processed.assign(field_processed.size(), false);
        field_processed = vector<bool>(descriptor->fields.size(), false);
    };
};

typedef KeyValueIterator<const int, Datum&> FieldIteratorType;
typedef ValueIterator<Datum&> ListIteratorType;

// we can probably reuse the iterator instances (by building the schema with all necessary
// references at the beginning)

static inline Datum selectDatum(Datum& datum) {
    if (datum.field) {
        if (!datum.field->message_type) {
            throw std::runtime_error("missing message type");
        }
        return Datum(datum.stream, datum.field->message_type.get(), datum.reading_missing);
    } else {
        return datum;
    }
}

class FieldIterator final : public FieldIteratorType {
    Datum datum;
    Limit limit;
    int field_index;
    vector<bool>::iterator begin;
    vector<bool>::iterator end;
    vector<bool>::iterator current;

   public:
    FieldIterator(Datum& input_datum) : datum(selectDatum(input_datum)) {
        if (datum.message_size < 0) {
            if (datum.reading_missing) {
                datum.message_size = 0;
            } else {
                if (!datum.stream.ReadVarintSizeAsInt(&datum.message_size)) {
                    throw std::runtime_error("Unable to read nested message size");
                }
            }
        }

        limit = datum.stream.PushLimit(datum.message_size);
        begin = datum.field_processed.begin();
        end = datum.field_processed.end();
        current = begin;
    };

    ~FieldIterator() final override = default;

    bool nextMissing() {
        current = std::find(current, end, false);
        field_index = std::distance(begin, current);
        if (field_index == datum.field_processed.size()) {
            datum.stream.CheckEntireMessageConsumedAndPopLimit(limit);
            return false;
        }
        current++;
        datum.field = datum.descriptor->fields.at(field_index).get();

        return true;
    }

    bool next() final override {
        while (true) {
            if (datum.reading_missing) {
                return nextMissing();
            }

            if (datum.read_ahead_tag != 0) {
                datum.current_tag = datum.read_ahead_tag;
                datum.read_ahead_tag = 0;
            } else {
                datum.current_tag = datum.stream.ReadTagNoLastTag();
            }

            if (datum.current_tag == 0) {
                datum.reading_missing = true;

                return nextMissing();
            }

            unsigned char wire_type = datum.current_tag & 0x07;
            uint32_t field_number = datum.current_tag >> 3;

            if (!datum.descriptor) {
                throw std::runtime_error("Null descriptor");
            }

            if (datum.descriptor->number_to_field.count(field_number)) {
                // should not be mutating the field
                datum.field = datum.descriptor->number_to_field.at(field_number);
                field_index = datum.field->index;
                datum.field_processed[field_index] = true;
                return true;
            } else {
                // discard the unnecessary data
                switch (wire_type) {
                    case WireType::WIRETYPE_VARINT: {
                        uint32_t unused;
                        datum.stream.ReadVarint32(&unused);
                        break;
                    }
                    case WireType::WIRETYPE_FIXED64: {
                        datum.stream.Skip(sizeof(uint64_t));
                        break;
                    }
                    case WireType::WIRETYPE_LENGTH_DELIMITED: {
                        int size;
                        datum.stream.ReadVarintSizeAsInt(&size);
                        datum.stream.Skip(size);
                        break;
                    }
                    case WireType::WIRETYPE_START_GROUP: {
                        throw std::runtime_error("Groups not supported");
                    }
                    case WireType::WIRETYPE_END_GROUP: {
                        throw std::runtime_error("Groups not supported");
                    }
                    case WireType::WIRETYPE_FIXED32: {
                        datum.stream.Skip(sizeof(uint32_t));
                        break;
                    }
                    default:
                        throw std::runtime_error("Unexpected wire type");
                }
            }
        }
    }

    const int key() final override {
        return field_index;
    }

    Datum& value() final override {
        return datum;
    }
};

class ListIterator final : public ListIteratorType {
    Datum& datum;
    bool packed;
    Limit limit;
    bool read_first = false;

   public:
    ListIterator(Datum& datum) : datum(datum) {
        if (!datum.field || !datum.field->pb_field->is_repeated()) {
            throw std::runtime_error("Not a repeated field");
        }

        datum.reading_list = true;
        if (!datum.reading_missing) {
            unsigned char wire_type = datum.current_tag & 0x07;
            if (wire_type == WireType::WIRETYPE_LENGTH_DELIMITED &&
                datum.field->pb_field->is_packable()) {
                packed = true;
                limit = datum.stream.ReadLengthAndPushLimit();
            } else {
                packed = false;
            }
        }
    };

    ~ListIterator() final override = default;

    bool next() final override {
        if (datum.reading_missing) {
            datum.reading_list = false;
            return false;
        }

        if (packed) {
            bool hasBytesRemaining = datum.stream.BytesUntilLimit() > 0;
            if (!hasBytesRemaining) {
                datum.stream.PopLimit(limit);
                datum.reading_list = false;
            }
            return hasBytesRemaining;
        } else {
            // this approach to reading unpacked repeated fields makes the assumption that every
            // element of the repeated field is encoded sequentially. The protobuf spec explicitly
            // says that this is not required (though most implementations will take that approach).
            // This assumption should be fixed (though it is difficult to resolve this with the
            // generic converter)
            if (read_first) {
                uint32_t tag = datum.stream.ReadTagNoLastTag();

                if (tag == datum.current_tag) {
                    return true;
                } else {
                    datum.read_ahead_tag = tag;
                    datum.reading_list = false;
                    return false;
                }
            } else {
                read_first = true;
                return true;
            }
        }
    };

    Datum& value() final override {
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

unique_ptr<Node> convert(std::istream& is, const ColumnFilter* column_filter);

}  // namespace pbd
}  // namespace bamboo
