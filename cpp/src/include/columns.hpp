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

#include <map>
#include <memory>
#include <string>
#include <util.hpp>
#include <vector>
#include <iostream>

namespace bamboo {

using std::map;
using std::reference_wrapper;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

enum class PrimitiveType {
    EMPTY,
    BOOL,
    CHAR,
    INT8,
    INT16,
    INT32,
    INT64,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    FLOAT16,
    FLOAT32,
    FLOAT64,
    STRING,
    BYTE_ARRAY,
    ENUM
};

template <class T> struct PrimitiveEnum;
template <PrimitiveType T> struct PrimitiveTyper {
    static constexpr PrimitiveType primitive_enum = T;
};

template <class T> constexpr PrimitiveType primitive_enum(const T& t) {
    return PrimitiveEnum<T>::primitive_enum;
}

template <class T> struct VectorType { typedef T vector_type; };
template <PrimitiveType T> struct VectorTyper;

struct DynamicEnumVector;
struct PrimitiveEnumVector;

struct PrimitiveVector {
    PrimitiveType type;

   public:
    virtual ~PrimitiveVector() = default;

    PrimitiveVector() : PrimitiveVector(PrimitiveType::EMPTY) {}

    PrimitiveVector(PrimitiveType type) : type(type) {}

    PrimitiveType get_type() {
        return type;
    }

    template <PrimitiveType T> auto& get_values() {
        if (type == T) {
            return static_cast<typename VectorTyper<T>::vector_type&>(*this).get_vector();
        } else {
            throw std::logic_error("Attempted to access values with wrong type");
        }
    }

    const DynamicEnumVector& get_enums();

    template <PrimitiveType T> static auto create() {
        return make_unique<typename VectorTyper<T>::vector_type>(T);
    }
};

struct DynamicEnum {
    virtual PrimitiveVector& get_enums() = 0;

    // We currently expose the source of the enums (i.e. the schema) using an
    // untyped pointer. Another potential way to do this would be to templatize
    // the enum, but that would make our nodes related to the type of converter,
    // which is ugly. Is there a cleaner way to do this? Possibly store the
    // previous enum type inside the converter so we know if it changes?
    virtual const void* source() = 0;

    bool same_source(DynamicEnum& other) {
        return (source() == other.source()) && source() != NULL;
    };

    virtual ~DynamicEnum() = default;
};

struct DynamicEnumValue {
    size_t index;
    shared_ptr<DynamicEnum> values;

    DynamicEnumValue(size_t index, shared_ptr<DynamicEnum> values) : index(index), values(values){};
};

struct DynamicEnumVector {
    vector<std::size_t> index;
    shared_ptr<DynamicEnum> values;
};

template <class T> class PrimitiveSimpleVector : public PrimitiveVector {
   private:
    vector<T> vec;

   public:
    virtual ~PrimitiveSimpleVector() = default;

    PrimitiveSimpleVector() : PrimitiveVector(PrimitiveEnum<T>::primitive_enum) {}

    PrimitiveSimpleVector(PrimitiveType type) : PrimitiveVector(type) {}

    void add(T t) {
        vec.push_back(t);
    }

    vector<T>& get_vector() {
        return vec;
    }
};

class PrimitiveEnumVector : public PrimitiveVector {
   private:
    DynamicEnumVector enums;

   public:
    virtual ~PrimitiveEnumVector() = default;

    // we don't specify the type because it will always be passed in by the templatized create
    PrimitiveEnumVector(PrimitiveType type) : PrimitiveVector(type) {}

    PrimitiveEnumVector(DynamicEnumVector&& enums)
        : PrimitiveVector(PrimitiveType::ENUM), enums(enums) {}

    void add(const DynamicEnumValue& t);

    const DynamicEnumVector& get_enums_vector() {
        return enums;
    }
};

template <> struct PrimitiveEnum<char> : PrimitiveTyper<PrimitiveType::CHAR> {};
template <> struct PrimitiveEnum<int8_t> : PrimitiveTyper<PrimitiveType::INT8> {};
template <> struct PrimitiveEnum<int16_t> : PrimitiveTyper<PrimitiveType::INT16> {};
template <> struct PrimitiveEnum<int32_t> : PrimitiveTyper<PrimitiveType::INT32> {};
template <> struct PrimitiveEnum<int64_t> : PrimitiveTyper<PrimitiveType::INT64> {};
template <> struct PrimitiveEnum<uint8_t> : PrimitiveTyper<PrimitiveType::UINT8> {};
template <> struct PrimitiveEnum<uint16_t> : PrimitiveTyper<PrimitiveType::UINT16> {};
template <> struct PrimitiveEnum<uint32_t> : PrimitiveTyper<PrimitiveType::UINT32> {};
template <> struct PrimitiveEnum<uint64_t> : PrimitiveTyper<PrimitiveType::UINT64> {};
template <> struct PrimitiveEnum<float> : PrimitiveTyper<PrimitiveType::FLOAT32> {};
template <> struct PrimitiveEnum<double> : PrimitiveTyper<PrimitiveType::FLOAT64> {};
template <> struct PrimitiveEnum<string> : PrimitiveTyper<PrimitiveType::STRING> {};
template <> struct PrimitiveEnum<bool> : PrimitiveTyper<PrimitiveType::BOOL> {};
template <> struct PrimitiveEnum<DynamicEnumValue> : PrimitiveTyper<PrimitiveType::ENUM> {};
template <> struct PrimitiveEnum<vector<uint8_t>> : PrimitiveTyper<PrimitiveType::BYTE_ARRAY> {};

// it would be nice if we could define all the symmetric types in a single line (i.e. all except
// bool)
template <class T> struct PrimitiveVectorType : VectorType<PrimitiveSimpleVector<T>> {};

// do we need to know the non-vector types? (i.e. DynamicEnumValue)
template <> struct VectorTyper<PrimitiveType::CHAR> : PrimitiveVectorType<char> {};
template <> struct VectorTyper<PrimitiveType::INT8> : PrimitiveVectorType<int8_t> {};
template <> struct VectorTyper<PrimitiveType::INT16> : PrimitiveVectorType<int16_t> {};
template <> struct VectorTyper<PrimitiveType::INT32> : PrimitiveVectorType<int32_t> {};
template <> struct VectorTyper<PrimitiveType::INT64> : PrimitiveVectorType<int64_t> {};
template <> struct VectorTyper<PrimitiveType::UINT8> : PrimitiveVectorType<uint8_t> {};
template <> struct VectorTyper<PrimitiveType::UINT16> : PrimitiveVectorType<uint16_t> {};
template <> struct VectorTyper<PrimitiveType::UINT32> : PrimitiveVectorType<uint32_t> {};
template <> struct VectorTyper<PrimitiveType::UINT64> : PrimitiveVectorType<uint64_t> {};
template <> struct VectorTyper<PrimitiveType::FLOAT16> : PrimitiveVectorType<uint16_t> {};
template <> struct VectorTyper<PrimitiveType::FLOAT32> : PrimitiveVectorType<float> {};
template <> struct VectorTyper<PrimitiveType::FLOAT64> : PrimitiveVectorType<double> {};
template <> struct VectorTyper<PrimitiveType::STRING> : PrimitiveVectorType<string> {};
template <> struct VectorTyper<PrimitiveType::BOOL> : PrimitiveVectorType<uint8_t> {};
template <> struct VectorTyper<PrimitiveType::ENUM> : VectorType<PrimitiveEnumVector> {};
template <> struct VectorTyper<PrimitiveType::BYTE_ARRAY> : PrimitiveVectorType<vector<uint8_t>> {};

class NullIndicator {
    size_t size = 0;
    vector<size_t> index;

   public:
    virtual ~NullIndicator() = default;

    NullIndicator() {}

    NullIndicator(NullIndicator&& source) {
        index = std::move(source.index);
        size = source.size;
    }

    void add_null();

    void add_not_null();

    size_t get_size() {
        return size;
    }

    const vector<size_t>& get_indices() {
        return index;
    }
};

enum class ObjType { INCOMPLETE, RECORD, LIST, PRIMITIVE };

class NodeVisitable;
class Node;
class IncompleteNode;
class PrimitiveNode;
class ListNode;
class RecordNode;
template <class T> class Visitor;

template <class V> class Visitor {
   public:
    virtual ~Visitor() = default;

    virtual void visit(V&) = 0;
};

template <class V> class Visitable {
   public:
    virtual ~Visitable() = default;

    void accept(Visitor<V>& visitor) {
        visitor.visit(static_cast<V&>(*this));
    }
};

class Node : public NullIndicator {
   public:
    virtual ~Node() = default;

    ObjType type;

    Node(ObjType type) : type(type){};

    Node(ObjType type, NullIndicator&& null_indicator)
        : NullIndicator(std::move(null_indicator)), type(type){};
};

class IncompleteNode : public Node, Visitable<IncompleteNode> {
   public:
    virtual ~IncompleteNode() = default;

    IncompleteNode(Node&& source) : Node(ObjType::INCOMPLETE, std::move(source)){};

    IncompleteNode() : Node(ObjType::INCOMPLETE){};
};

class PrimitiveNode : public Node, Visitable<PrimitiveNode> {
    unique_ptr<PrimitiveVector> values = make_unique<PrimitiveVector>();

   public:
    virtual ~PrimitiveNode() = default;

    PrimitiveNode() : Node(ObjType::PRIMITIVE){};

    PrimitiveNode(Node&& source) : Node(ObjType::PRIMITIVE, std::move(source)){};

    PrimitiveType get_type();

    unique_ptr<PrimitiveVector>& get_vector();

    // we should put the init piece inside the vector type (something where you pass it the
    // primitive enum and get back a vector of the correct type with the type set)
    template <class T> void init() {
        values = PrimitiveVector::create<PrimitiveEnum<T>::primitive_enum>();
    }

    template <PrimitiveType T> void init_type() {
        values = PrimitiveVector::create<T>();
    }

    template <class T> void add(T t) {
        if (values->type == PrimitiveType::EMPTY) {
            init<T>();
        }
        // this check should be reversed, as it is not injective in this direction. we should look
        // at the defined abstract type (PrimitiveType) and verify that its underlying storage type
        // matches the passed variable type we can probably use the add_by_type (and derive the type
        // from the data type to abstract type template mappings)
        PrimitiveType prim_type = primitive_enum(t);
        if (values->type == prim_type) {
            add_unsafe(t);
        } else {
            throw std::invalid_argument("Mismatched primitive types");
        }
    }

    template <PrimitiveType PT, class T> void add_by_type(T t) {
        if (values->type == PrimitiveType::EMPTY) {
            init_type<PT>();
        }
        static_cast<typename VectorTyper<PT>::vector_type&>(*values).add(t);
    }

    template <class T> void add_unsafe(T t) {
        static_cast<typename VectorTyper<PrimitiveEnum<T>::primitive_enum>::vector_type&>(*values)
            .add(t);
    }

    // it would be better to adapt the add method to be able to efficiently move strings (a simple tested showed a performance impact)
    string& add_string() {
        vector<string>& vec = static_cast<typename VectorTyper<PrimitiveType::STRING>::vector_type&>(*values)
            .get_vector();
        vec.emplace_back();
        return vec.back();
    }

    template <PrimitiveType T> auto& get_values() {
        return values->get_values<T>();
    }

    const DynamicEnumVector& get_enums() {
        return values->get_enums();
    }
};

class ListNode : public Node, Visitable<ListNode> {
    vector<size_t> index;
    unique_ptr<Node> child = make_unique<IncompleteNode>();

   public:
    virtual ~ListNode() = default;

    ListNode() : Node(ObjType::LIST){};

    ListNode(Node&& source) : Node(ObjType::LIST, std::move(source)){};

    unique_ptr<Node>& get_list();

    void add_list(size_t length);

    const vector<size_t>& get_index();
};

class RecordNode : public Node, Visitable<ListNode> {
    map<string, unique_ptr<Node>> fields_map;
    vector<reference_wrapper<unique_ptr<Node>>> field_vec;
    vector<string> field_names;

    void add_field(const string& name);

   public:
    RecordNode() : Node(ObjType::RECORD){};

    RecordNode(vector<string> names);

    RecordNode(Node&& source) : Node(ObjType::RECORD, std::move(source)){};

    virtual ~RecordNode() = default;

    unique_ptr<Node>& get_field(const string& name);

    unique_ptr<Node>& get_field(size_t index);

    const vector<string> get_fields();
};

class NodeBuilder : public Visitor<PrimitiveNode>,
                    public Visitor<IncompleteNode>,
                    public Visitor<ListNode>,
                    public Visitor<RecordNode> {
   public:
    void visit(PrimitiveNode& node);

    void visit(IncompleteNode& node);

    void visit(ListNode& node);

    void visit(RecordNode& node);
};

struct ColumnFilter {
    bool explicitly_include;
    bool explicitly_exclude;
    const map<const string, const shared_ptr<ColumnFilter>> field_filters;

    ColumnFilter(bool explicitly_include, bool explicitly_exclude,
                   const map<const string, const shared_ptr<ColumnFilter>> field_filters)
        : explicitly_include(explicitly_include),
          explicitly_exclude(explicitly_exclude),
          field_filters(field_filters) {
        if (explicitly_include && explicitly_exclude) {
            throw std::runtime_error("Cannot both explicitly include and exclude a field");
        }
    }

    bool has_includes() const {
        bool has_includes = explicitly_include;
        for (const auto& field : field_filters) {
            has_includes |= (field.second && field.second->has_includes());
        }
        return has_includes;
    }
};

template <class T> class ValueIterator {
   public:
    virtual bool next() = 0;

    virtual T value() = 0;

    virtual ~ValueIterator() = default;
};

template <class K, class V> class KeyValueIterator {
   public:
    virtual bool next() = 0;

    virtual K key() = 0;

    virtual V value() = 0;

    virtual ~KeyValueIterator() = default;
};

static void init(unique_ptr<Node>& node, ObjType type) {
    switch (type) {
        case ObjType::RECORD:
            node = make_unique<RecordNode>(std::move(*node));
            break;
        case ObjType::LIST:
            node = make_unique<ListNode>(std::move(*node));
            break;
        case ObjType::PRIMITIVE:
            node = make_unique<PrimitiveNode>(std::move(*node));
            break;
        case ObjType::INCOMPLETE:
            break;
    }
}

template <class T, class F, class L> struct Converter {
    virtual ObjType type(T datum) = 0;

    virtual F fields(T datum) = 0;

    virtual L get_list(T datum) = 0;

    virtual void add_primitive(PrimitiveNode& v, T datum) = 0;

    virtual ~Converter() = default;

    void convert(unique_ptr<Node>& node, T t) {
        ObjType obj_type = type(t);
        if (node->type == ObjType::INCOMPLETE) {
            init(node, obj_type);
        }

        if (node->type != obj_type && obj_type != ObjType::INCOMPLETE) {
            throw std::invalid_argument("Inconsistent schema");
        }

        switch (obj_type) {
            case ObjType::RECORD: {
                RecordNode& record_node = *static_cast<RecordNode*>(node.get());

                auto f = fields(t);
                while (f.next()) {
                    unique_ptr<Node>& field_node = record_node.get_field(f.key());
                    convert(field_node, f.value());
                }
                record_node.add_not_null();
                break;
            }
            case ObjType::LIST: {
                ListNode& list_node = *static_cast<ListNode*>(node.get());

                unique_ptr<Node>& sub_node = list_node.get_list();
                size_t counter = 0;
                auto l = get_list(t);
                while (l.next()) {
                    convert(sub_node, l.value());
                    counter++;
                }
                list_node.add_list(counter);
                list_node.add_not_null();
                break;
            }
            case ObjType::PRIMITIVE: {
                PrimitiveNode& primitive_node = *static_cast<PrimitiveNode*>(node.get());
                add_primitive(primitive_node, t);
                primitive_node.add_not_null();
                break;
            }
            case ObjType::INCOMPLETE: {
                node->add_null();
            }
        }
    };
};

}  // namespace bamboo
