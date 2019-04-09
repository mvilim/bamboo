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

unique_ptr<Node> convert(std::istream& is, const ColumnFilter* column_filter) {
    PBDReader reader(is);
    PBDConverter converter;
    unique_ptr<Node> node = make_unique<IncompleteNode>();
    unique_ptr<pb::Message> message = reader.readMessage();
    while (message) {
        Datum datum(*message);
        converter.convert(node, datum);
        reader.readMessage(message);
    }

    return node;
}

ObjType PBDConverter::type(Datum& datum) {
    if (datum.field) {
        if (datum.field->is_repeated() && datum.counter < 0) {
            return ObjType::LIST;
        }
        pb::FieldDescriptor::Type type = datum.field->type();
        switch (type) {
            case pb::FieldDescriptor::TYPE_MESSAGE:
            case pb::FieldDescriptor::TYPE_GROUP:
                if (datum.message.GetReflection()->HasField(datum.message, datum.field)) {
                    return ObjType::RECORD;
                }
                return ObjType::INCOMPLETE;
            default:
                return ObjType::PRIMITIVE;
        }
    } else {
        return ObjType::RECORD;
    }
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

void PBDConverter::add_primitive(PrimitiveNode& v, Datum& datum) {
    switch (datum.field->type()) {
        case pb::FieldDescriptor::TYPE_ENUM: {
            const pb::EnumValueDescriptor* e;
            if (datum.field->is_repeated()) {
                e = datum.message.GetReflection()->GetRepeatedEnum(datum.message, datum.field,
                                                                   datum.counter++);
            } else {
                e = datum.message.GetReflection()->GetEnum(datum.message, datum.field);
            }
            if (v.get_type() == PrimitiveType::ENUM) {
                shared_ptr<DynamicEnum> enum_values = v.get_enums().values;
                v.add(DynamicEnumValue(e->index(), enum_values));
            } else {
                v.add(DynamicEnumValue(e->index(), std::make_shared<ProtoEnum>(e->type())));
            }
            return;
        }
        case pb::FieldDescriptor::TYPE_BOOL:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedBool(datum.message, datum.field,
                                                                     datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetBool(datum.message, datum.field));
            }
            return;
        case pb::FieldDescriptor::TYPE_FLOAT:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedFloat(datum.message, datum.field,
                                                                      datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetFloat(datum.message, datum.field));
            }
            return;
        case pb::FieldDescriptor::TYPE_INT32:
        case pb::FieldDescriptor::TYPE_SINT32:
        case pb::FieldDescriptor::TYPE_FIXED32:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedInt32(datum.message, datum.field,
                                                                      datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetInt32(datum.message, datum.field));
            }
            return;
        case pb::FieldDescriptor::TYPE_INT64:
        case pb::FieldDescriptor::TYPE_SINT64:
        case pb::FieldDescriptor::TYPE_FIXED64:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedInt64(datum.message, datum.field,
                                                                      datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetInt64(datum.message, datum.field));
            }
            return;
        case pb::FieldDescriptor::TYPE_DOUBLE:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedDouble(datum.message, datum.field,
                                                                       datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetDouble(datum.message, datum.field));
            }
            return;
        case pb::FieldDescriptor::TYPE_STRING:
        case pb::FieldDescriptor::TYPE_BYTES:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedString(datum.message, datum.field,
                                                                       datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetString(datum.message, datum.field));
            }
            return;
        case pb::FieldDescriptor::TYPE_UINT32:
        case pb::FieldDescriptor::TYPE_SFIXED32:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedUInt32(datum.message, datum.field,
                                                                       datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetUInt32(datum.message, datum.field));
            }
            return;
        case pb::FieldDescriptor::TYPE_UINT64:
        case pb::FieldDescriptor::TYPE_SFIXED64:
            if (datum.field->is_repeated()) {
                v.add(datum.message.GetReflection()->GetRepeatedUInt64(datum.message, datum.field,
                                                                       datum.counter++));
            } else {
                v.add(datum.message.GetReflection()->GetUInt64(datum.message, datum.field));
            }
            return;
        default:
            throw std::runtime_error("Unexpected primitive type");
    }
}

}  // namespace pbd
}  // namespace bamboo
