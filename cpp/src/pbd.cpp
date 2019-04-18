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

#include <pbd.hpp>

namespace bamboo {
namespace pbd {

using namespace ::pbd;

using WireFormatLite = pb::internal::WireFormatLite;

void initialize(const pb::Descriptor* descriptor, unique_ptr<Node>& node) {
    node = make_unique<RecordNode>();
    RecordNode& record_node = *static_cast<RecordNode*>(node.get());
    for (int i = 0; i < descriptor->field_count(); i++) {
        const pb::FieldDescriptor* field = descriptor->field(i);
        unique_ptr<Node>* field_node = &record_node.get_field(field->name());

        if (field->is_repeated()) {
            *field_node = make_unique<ListNode>();
            ListNode& list_node = *static_cast<ListNode*>(node.get());
            field_node = &list_node.get_list();
        }

        pb::FieldDescriptor::Type type = field->type();
        switch (type) {
            case pb::FieldDescriptor::TYPE_MESSAGE:
            case pb::FieldDescriptor::TYPE_GROUP:
                initialize(field->message_type(), *field_node);
            default:
                break;
        }
    }
}

unique_ptr<Node> convert(std::istream& is, const ColumnFilter* column_filter) {
    PBDReader reader(is);
    PBDConverter converter;
    unique_ptr<Node> node = make_unique<IncompleteNode>();
    initialize(reader.descriptor(), node);

    Datum datum(reader.stream(), reader.descriptor());
    int protoMessageSize = 0;
    while (datum.stream.ReadVarintSizeAsInt(&protoMessageSize)) {
        datum.message_size = protoMessageSize;
        converter.convert(node, datum);
    }

    return node;
}

ObjType PBDConverter::type(Datum& datum) {
    if (datum.field) {
        if (datum.field->is_repeated() && !datum.reading_list) {
            return ObjType::LIST;
        }
        pb::FieldDescriptor::Type type = datum.field->type();
        switch (type) {
            case pb::FieldDescriptor::TYPE_MESSAGE:
            case pb::FieldDescriptor::TYPE_GROUP:
                break;
            default:
                return ObjType::PRIMITIVE;
        }
    }
    return ObjType::RECORD;
}

FieldIterator PBDConverter::fields(Datum& datum) {
    return FieldIterator(datum);
}

ListIterator PBDConverter::get_list(Datum& datum) {
    return ListIterator(datum);
}

struct ProtoEnum final : public DynamicEnum {
    const pb::EnumDescriptor* descriptor;

    PrimitiveSimpleVector<string> enum_values;

    ProtoEnum(const pb::EnumDescriptor* descriptor) : descriptor(descriptor) {
        for (size_t i = 0; i < descriptor->value_count(); i++) {
            enum_values.add(descriptor->value(i)->name());
        }
    };

    virtual PrimitiveVector& get_enums() override {
        return enum_values;
    }

    virtual const void* source() override {
        return descriptor;
    };
};

template <class T, WireFormatLite::FieldType E>
static inline void add(PrimitiveNode& v, pb::io::CodedInputStream& stream) {
    T value;
    WireFormatLite::ReadPrimitive<T, E>(&stream, &value);
    v.add(value);
}

static inline void add_enum(PrimitiveNode& v, Datum& datum, int index) {
    if (v.get_type() == PrimitiveType::ENUM) {
        shared_ptr<DynamicEnum> enum_values = v.get_enums().values;
        v.add(DynamicEnumValue(index, enum_values));
    } else {
        v.add(DynamicEnumValue(index, std::make_shared<ProtoEnum>(datum.field->enum_type())));
    }
}

static inline void add_missing(PrimitiveNode& v, Datum& datum) {
    switch (datum.field->type()) {
        case pb::FieldDescriptor::TYPE_FLOAT:
            v.add(datum.field->default_value_float());
            break;
        case pb::FieldDescriptor::TYPE_DOUBLE:
            v.add(datum.field->default_value_double());
            break;
        case pb::FieldDescriptor::TYPE_ENUM:
            add_enum(v, datum, datum.field->default_value_enum()->index());
            break;
        case pb::FieldDescriptor::TYPE_BOOL:
            v.add(datum.field->default_value_bool());
            break;
        case pb::FieldDescriptor::TYPE_INT32:
        case pb::FieldDescriptor::TYPE_SINT32:
        case pb::FieldDescriptor::TYPE_SFIXED32:
            v.add(datum.field->default_value_int32());
            break;
        case pb::FieldDescriptor::TYPE_INT64:
        case pb::FieldDescriptor::TYPE_SINT64:
        case pb::FieldDescriptor::TYPE_SFIXED64:
            v.add(datum.field->default_value_int64());
            break;
        case pb::FieldDescriptor::TYPE_STRING:
        case pb::FieldDescriptor::TYPE_BYTES:
            v.add(datum.field->default_value_string());
            break;
        case pb::FieldDescriptor::TYPE_UINT32:
        case pb::FieldDescriptor::TYPE_FIXED32:
            v.add(datum.field->default_value_uint32());
            return;
        case pb::FieldDescriptor::TYPE_UINT64:
        case pb::FieldDescriptor::TYPE_FIXED64:
            v.add(datum.field->default_value_uint64());
            break;
        default:
            throw std::runtime_error("Unexpected primitive type");
    }
}

static inline void add_existing(PrimitiveNode& v, Datum& datum) {
    switch (datum.field->type()) {
        case pb::FieldDescriptor::TYPE_FLOAT: {
            add<float, WireFormatLite::TYPE_FLOAT>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_DOUBLE: {
            add<double, WireFormatLite::TYPE_DOUBLE>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_ENUM: {
            int index = WireFormatLite::ReadPrimitive<int, WireFormatLite::TYPE_ENUM>(&datum.stream,
                                                                                      &index);
            add_enum(v, datum, index);
            break;
        }
        case pb::FieldDescriptor::TYPE_BOOL: {
            add<bool, WireFormatLite::TYPE_BOOL>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_INT32: {
            add<int32_t, WireFormatLite::TYPE_INT32>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_SINT32: {
            add<int32_t, WireFormatLite::TYPE_SINT32>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_SFIXED32: {
            add<int32_t, WireFormatLite::TYPE_SFIXED32>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_INT64: {
            add<int64_t, WireFormatLite::TYPE_INT64>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_SINT64: {
            add<int64_t, WireFormatLite::TYPE_SINT64>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_SFIXED64: {
            add<int64_t, WireFormatLite::TYPE_SFIXED64>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_STRING: {
            std::string* value;
            WireFormatLite::ReadString(&datum.stream, &value);
            v.add(*value);
            break;
        }
        case pb::FieldDescriptor::TYPE_BYTES: {
            // this reads the value as a string, instead of a vector of uint8_t -- we should
            // probably change it
            std::string* value;
            WireFormatLite::ReadBytes(&datum.stream, &value);
            v.add(*value);
            break;
        }
        case pb::FieldDescriptor::TYPE_UINT32: {
            add<uint32_t, WireFormatLite::TYPE_UINT32>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_FIXED32: {
            add<uint32_t, WireFormatLite::TYPE_FIXED32>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_UINT64: {
            add<uint64_t, WireFormatLite::TYPE_UINT64>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_FIXED64: {
            add<uint64_t, WireFormatLite::TYPE_FIXED64>(v, datum.stream);
            break;
        }
        default:
            throw std::runtime_error("Unexpected primitive type");
    }
}

void PBDConverter::add_primitive(PrimitiveNode& v, Datum& datum) {
    if (datum.reading_missing) {
        add_missing(v, datum);
    } else {
        add_existing(v, datum);
    }
}

}  // namespace pbd
}  // namespace bamboo
