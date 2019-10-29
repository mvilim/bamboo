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

static shared_ptr<const MessageDescriptor> create_message_type(const pb::FieldDescriptor* pb_field,
                                                               const ColumnFilter* column_filter,
                                                               bool implicit_include) {
    if (!pb_field) {
        throw std::runtime_error("Attempting to init empty field");
    } else if (pb_field->type() == pb::FieldDescriptor::TYPE_MESSAGE) {
        return make_shared<const MessageDescriptor>(pb_field->message_type(), column_filter,
                                                    implicit_include);
    }
    return nullptr;
}

FieldDescriptor::FieldDescriptor(const pb::FieldDescriptor* pb_field, int index,
                                 const ColumnFilter* column_filter, bool implicit_include)
    : pb_field(pb_field),
      index(index),
      message_type(create_message_type(pb_field, column_filter, implicit_include)) {}

bool FieldDescriptor::has_fields() const
{
    return message_type && message_type->has_fields();
}

void MessageDescriptor::add_field(const pb::FieldDescriptor* field,
                                  const ColumnFilter* column_filter, bool implicit_include) {
    bool explicit_include = column_filter && column_filter->explicitly_include;
    bool explicit_exclude = column_filter && column_filter->explicitly_exclude;
    bool included = explicit_include || (implicit_include && !explicit_exclude);

    int index = fields.size();
    auto fieldDesc = std::make_shared<FieldDescriptor>(field, index, column_filter, included);
    if (fieldDesc->has_fields() || (!fieldDesc->message_type && included)) {
        fields.push_back(fieldDesc);
        FieldDescriptor* fd = fields.back().get();
        number_to_field.emplace(field->number(), fd);
    }
}

bool MessageDescriptor::has_fields() const
{
    return fields.size();
}

MessageDescriptor::MessageDescriptor(const pb::Descriptor* pb_descriptor,
                                     const ColumnFilter* column_filter, bool implicit_include)
    : pb_descriptor(pb_descriptor) {
    // if using a different map type, we should reserve space here
    for (int i = 0; i < pb_descriptor->field_count(); i++) {
        const ColumnFilter* field_filter = nullptr;
        const pb::FieldDescriptor* field = pb_descriptor->field(i);
        const string& field_name = field->name();
        if (column_filter && column_filter->field_filters.count(field_name)) {
            field_filter = column_filter->field_filters.at(field_name).get();
        }
        add_field(pb_descriptor->field(i), field_filter, implicit_include);
    }
}

void initialize(const MessageDescriptor* descriptor, unique_ptr<Node>& node) {
    node = make_unique<RecordNode>();
    RecordNode& record_node = *static_cast<RecordNode*>(node.get());
    for (int i = 0; i < descriptor->fields.size(); i++) {
        const FieldDescriptor* field = descriptor->fields.at(i).get();
        unique_ptr<Node>* field_node = &record_node.get_field(field->pb_field->name());

        if (field->pb_field->is_repeated()) {
            *field_node = make_unique<ListNode>();
            ListNode& list_node = *static_cast<ListNode*>(field_node->get());
            field_node = &list_node.get_list();
        }

        pb::FieldDescriptor::Type type = field->pb_field->type();
        switch (type) {
            case pb::FieldDescriptor::TYPE_MESSAGE:
            case pb::FieldDescriptor::TYPE_GROUP:
                initialize(field->message_type.get(), *field_node);
                break;
            default:
                *field_node = make_unique<PrimitiveNode>();
                PrimitiveNode& prim_node = static_cast<PrimitiveNode&>(*field_node->get());
                switch (type) {
                    case pb::FieldDescriptor::TYPE_FLOAT:
                        prim_node.init_type<PrimitiveType::FLOAT32>();
                        break;
                    case pb::FieldDescriptor::TYPE_DOUBLE:
                        prim_node.init_type<PrimitiveType::FLOAT64>();
                        break;
                    case pb::FieldDescriptor::TYPE_ENUM:
                        // we don't initialize the type for enums because it is used to identify
                        // whether we need to read the enum string values
                        break;
                    case pb::FieldDescriptor::TYPE_BOOL:
                        prim_node.init_type<PrimitiveType::BOOL>();
                        break;
                    case pb::FieldDescriptor::TYPE_INT32:
                    case pb::FieldDescriptor::TYPE_SINT32:
                    case pb::FieldDescriptor::TYPE_SFIXED32:
                        prim_node.init_type<PrimitiveType::INT32>();
                        break;
                    case pb::FieldDescriptor::TYPE_INT64:
                    case pb::FieldDescriptor::TYPE_SINT64:
                    case pb::FieldDescriptor::TYPE_SFIXED64:
                        prim_node.init_type<PrimitiveType::INT64>();
                        break;
                    case pb::FieldDescriptor::TYPE_STRING:
                        prim_node.init_type<PrimitiveType::STRING>();
                        break;
                    case pb::FieldDescriptor::TYPE_BYTES:
                        prim_node.init_type<PrimitiveType::BYTE_ARRAY>();
                        break;
                    case pb::FieldDescriptor::TYPE_UINT32:
                    case pb::FieldDescriptor::TYPE_FIXED32:
                        prim_node.init_type<PrimitiveType::UINT32>();
                        break;
                    case pb::FieldDescriptor::TYPE_UINT64:
                    case pb::FieldDescriptor::TYPE_FIXED64:
                        prim_node.init_type<PrimitiveType::UINT64>();
                        break;
                    default:
                        break;
                }
        }
    }
}

unique_ptr<Node> convert(std::istream& is, const ColumnFilter* column_filter) {
    PBDReader reader(is);
    PBDConverter converter;
    unique_ptr<Node> node = make_unique<IncompleteNode>();

    MessageDescriptor descriptor(reader.descriptor(), column_filter,
                                 !column_filter || !column_filter->has_includes());
    initialize(&descriptor, node);
    Datum datum(reader.stream(), &descriptor);
    int protoMessageSize = 0;
    while (datum.stream.ReadVarintSizeAsInt(&protoMessageSize)) {
        datum.message_size = protoMessageSize;
        converter.convert(node, datum);
    }

    return node;
}

ObjType PBDConverter::type(Datum& datum) {
    if (datum.field) {
        if (datum.field->pb_field->is_repeated() && !datum.reading_list) {
            return ObjType::LIST;
        }
        pb::FieldDescriptor::Type type = datum.field->pb_field->type();
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
    v.add_unsafe(value);
}

static inline void add_enum(PrimitiveNode& v, Datum& datum, int number) {
    int index = datum.field->pb_field->enum_type()->FindValueByNumber(number)->index();
    if (v.get_type() == PrimitiveType::ENUM) {
        shared_ptr<DynamicEnum> enum_values = v.get_enums().values;
        v.add(DynamicEnumValue(index, enum_values));
    } else {
        v.add(DynamicEnumValue(index,
                               std::make_shared<ProtoEnum>(datum.field->pb_field->enum_type())));
    }
}

static inline void add_missing(PrimitiveNode& v, Datum& datum) {
    const pb::FieldDescriptor* field = datum.field->pb_field;
    switch (field->type()) {
        case pb::FieldDescriptor::TYPE_FLOAT:
            v.add_unsafe(field->default_value_float());
            break;
        case pb::FieldDescriptor::TYPE_DOUBLE:
            v.add_unsafe(field->default_value_double());
            break;
        case pb::FieldDescriptor::TYPE_ENUM:
            add_enum(v, datum, field->default_value_enum()->number());
            break;
        case pb::FieldDescriptor::TYPE_BOOL:
            v.add_unsafe(field->default_value_bool());
            break;
        case pb::FieldDescriptor::TYPE_INT32:
        case pb::FieldDescriptor::TYPE_SINT32:
        case pb::FieldDescriptor::TYPE_SFIXED32:
            v.add_unsafe(field->default_value_int32());
            break;
        case pb::FieldDescriptor::TYPE_INT64:
        case pb::FieldDescriptor::TYPE_SINT64:
        case pb::FieldDescriptor::TYPE_SFIXED64:
            v.add_unsafe(field->default_value_int64());
            break;
        case pb::FieldDescriptor::TYPE_STRING:
            v.add_unsafe(field->default_value_string());
            break;
        case pb::FieldDescriptor::TYPE_BYTES: {
            const string& s = field->default_value_string();
            std::vector<uint8_t> vec(s.begin(), s.end());
            v.add_unsafe(vec);
            break;
        }
        case pb::FieldDescriptor::TYPE_UINT32:
        case pb::FieldDescriptor::TYPE_FIXED32:
            v.add_unsafe(field->default_value_uint32());
            return;
        case pb::FieldDescriptor::TYPE_UINT64:
        case pb::FieldDescriptor::TYPE_FIXED64:
            v.add_unsafe(field->default_value_uint64());
            break;
        default:
            throw std::runtime_error("Unexpected primitive type");
    }
}

static inline void add_existing(PrimitiveNode& v, Datum& datum) {
    const pb::FieldDescriptor* field = datum.field->pb_field;
    switch (field->type()) {
        case pb::FieldDescriptor::TYPE_FLOAT: {
            add<float, WireFormatLite::TYPE_FLOAT>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_DOUBLE: {
            add<double, WireFormatLite::TYPE_DOUBLE>(v, datum.stream);
            break;
        }
        case pb::FieldDescriptor::TYPE_ENUM: {
            int number;
            WireFormatLite::ReadPrimitive<int, WireFormatLite::TYPE_ENUM>(&datum.stream, &number);
            add_enum(v, datum, number);
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
            string& s = v.add_string();
            WireFormatLite::ReadString(&datum.stream, &s);
            break;
        }
        case pb::FieldDescriptor::TYPE_BYTES: {
            // this causes unnecessary copying, it should be made more efficient
            string s;
            WireFormatLite::ReadBytes(&datum.stream, &s);
            std::vector<uint8_t> vec(s.begin(), s.end());
            v.add_unsafe(vec);
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
