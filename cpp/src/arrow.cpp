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

#include <arrow.hpp>

namespace bamboo {
namespace arrow {

unique_ptr<Node> convert(const Array& array);

void update_nulls(const Array& array, Node& node) {
    // it would be better to directly copy the null array values (or do it in a way that is more
    // conducive to optimization)
    for (size_t i; i < array.length(); i++) {
        if (array.IsNull(i)) {
            node.add_null();
        } else {
            node.add_not_null();
        }
    }
}

template <class T> void add_primitives(const NumericArray<T>& array, PrimitiveNode& node) {
    for (size_t i = 0; i < array.length(); i++) {
        node.add(array.Value(i));
    }
}

// we should share pieces of this (indexing) with the node visitor, but the templating is a bit
// tricky
class IndexArrayVisitor : public virtual ArrayVisitor {
   private:
    vector<size_t> indices;
    PrimitiveNode& enum_node;

   public:
    IndexArrayVisitor(PrimitiveNode& enum_node) : enum_node(enum_node) {}

    vector<size_t> take_result() {
        return std::move(indices);
    }

    template <class T> Status handle_numeric(const NumericArray<T>& array) {
        for (size_t i = 0; i < array.length(); i++) {
            if (!array.IsNull(i)) {
                indices.push_back(array.Value(i));
            }
        }
        return Status::OK();
    }

    virtual Status Visit(const Int8Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const Int16Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const Int32Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const Int64Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt8Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt16Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt32Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt64Array& array) final override {
        return handle_numeric(array);
    }
};

struct ArrowDynamicEnum : public DynamicEnum {
    ArrowDynamicEnum(unique_ptr<PrimitiveNode> enum_values_node)
        : enum_values_node(std::move(enum_values_node)){};

    virtual ~ArrowDynamicEnum() final override = default;

    virtual PrimitiveVector& get_enums() final override {
        return *enum_values_node->get_vector();
    }

    // assume that every arrow enum is consistently sourced
    virtual const void* source() final override {
        return 0;
    }

   private:
    unique_ptr<PrimitiveNode> enum_values_node;
};

class NodeArrayVisitor : public virtual ArrayVisitor {
   private:
    unique_ptr<Node> node;

   public:
    unique_ptr<Node> take_result() {
        return std::move(node);
    }

    template <class A, class P>
    Status handle_generic(A array, std::function<P(A, size_t i)> const& extractor) {
        node = make_unique<PrimitiveNode>();
        PrimitiveNode& pn = static_cast<PrimitiveNode&>(*node);
        for (size_t i = 0; i < array.length(); i++) {
            if (!array.IsNull(i)) {
                pn.add(extractor(array, i));
            }
        }
        return Status::OK();
    }

    // could we share more of this code with the generic version?
    Status handle_float16(const HalfFloatArray& array) {
        node = make_unique<PrimitiveNode>();
        PrimitiveNode& pn = static_cast<PrimitiveNode&>(*node);
        for (size_t i = 0; i < array.length(); i++) {
            if (!array.IsNull(i)) {
                pn.add_by_type<PrimitiveType::FLOAT16>(array.Value(i));
            }
        }
        return Status::OK();
    }

    template <class T> Status handle_numeric(const NumericArray<T>& array) {
        return handle_generic<const NumericArray<T>&, typename T::c_type>(
            array, [](const NumericArray<T>& a, size_t i) { return a.Value(i); });
    }

    virtual Status Visit(const NullArray& array) final override {
        // how do we merge the nulls into the combined array?
        return Status::NotImplemented("NullArray not implemented");
    };

    virtual Status Visit(const BooleanArray& array) final override {
        return handle_generic<const BooleanArray&, bool>(
            array, [](const BooleanArray& a, size_t i) { return a.Value(i); });
    }
    virtual Status Visit(const Int8Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const Int16Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const Int32Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const Int64Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt8Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt16Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt32Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const UInt64Array& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const HalfFloatArray& array) final override {
        return handle_float16(array);
    }
    virtual Status Visit(const FloatArray& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const DoubleArray& array) final override {
        return handle_numeric(array);
    }
    virtual Status Visit(const StringArray& array) final override {
        node = make_unique<PrimitiveNode>();
        PrimitiveNode& pn = static_cast<PrimitiveNode&>(*node);
        for (size_t i = 0; i < array.length(); i++) {
            pn.add(array.GetString(i));
        }
        return Status::OK();
    }
    virtual Status Visit(const BinaryArray& array) final override {
        return Status::NotImplemented("BinaryArray not implemented");
    }
    virtual Status Visit(const FixedSizeBinaryArray& array) final override {
        return Status::NotImplemented("FixedSizeBinaryArray not implemented");
    }
    virtual Status Visit(const Date32Array& array) final override {
        return Status::NotImplemented("Date32Array not implemented");
    }
    virtual Status Visit(const Date64Array& array) final override {
        return Status::NotImplemented("Date64Array not implemented");
    }
    virtual Status Visit(const Time32Array& array) final override {
        return Status::NotImplemented("Time32Array not implemented");
    }
    virtual Status Visit(const Time64Array& array) final override {
        return Status::NotImplemented("Time64Array not implemented");
    }
    virtual Status Visit(const TimestampArray& array) final override {
        return Status::NotImplemented("TimestampArray not implemented");
    }
    virtual Status Visit(const IntervalArray& array) final override {
        return Status::NotImplemented("IntervalArray not implemented");
    }
    virtual Status Visit(const Decimal128Array& array) final override {
        return Status::NotImplemented("Decimal128Array not implemented");
    }
    virtual Status Visit(const ListArray& array) final override {
        node = make_unique<ListNode>();
        // is there a cleaner way to do this without a raw pointer or cast?
        ListNode& ln = static_cast<ListNode&>(*node);
        for (size_t i = 0; i < array.length(); i++) {
            if (!array.IsNull(i)) {
                size_t length = array.value_offset(i + 1) - array.value_offset(i);
                ln.add_list(length);
            }
        }
        unique_ptr<Node>& sub_node = ln.get_list();
        sub_node = convert(*array.values());
        return Status::OK();
    }

    virtual Status Visit(const StructArray& array) final override {
        node = make_unique<RecordNode>();
        // is there a cleaner way to do this without a raw pointer or cast?
        RecordNode& rn = static_cast<RecordNode&>(*node);
        for (auto child : array.struct_type()->children()) {
            unique_ptr<Node>& field_node = rn.get_field(child->name());
            field_node = convert(*array.GetFieldByName(child->name()));
        }
        return Status::OK();
    }

    virtual Status Visit(const UnionArray& array) final override {
        return Status::NotImplemented("UnionArray not implemented");
    }

    virtual Status Visit(const DictionaryArray& array) final override {
        node = make_unique<PrimitiveNode>();
        // is there a cleaner way to do this without a raw pointer or cast?
        PrimitiveNode& pn = static_cast<PrimitiveNode&>(*node);
        NodeArrayVisitor enum_visitor;
        Status status = array.dictionary()->Accept(&enum_visitor);
        // can an enum have a non-primitive type?
        // this is a bit ugly -- do we have a better way?
        std::unique_ptr<PrimitiveNode> enum_values_node = std::unique_ptr<PrimitiveNode>(
            dynamic_cast<PrimitiveNode*>(enum_visitor.take_result().release()));
        shared_ptr<ArrowDynamicEnum> enum_value =
            std::make_shared<ArrowDynamicEnum>(std::move(enum_values_node));
        IndexArrayVisitor index_visitor(pn);
        Status index_status = array.indices()->Accept(&index_visitor);
        // would be better if we could move this
        DynamicEnumVector enum_vector;
        enum_vector.index = index_visitor.take_result();
        enum_vector.values = enum_value;
        pn.get_vector() = make_unique<PrimitiveEnumVector>(std::move(enum_vector));

        return Status::OK();
    }
};

unique_ptr<Node> convert(const Array& array) {
    NodeArrayVisitor node_visitor;
    Status status = array.Accept(&node_visitor);
    if (status.ok()) {
        unique_ptr<Node> node = node_visitor.take_result();
        update_nulls(array, *node);
        return node;
    } else {
        throw std::runtime_error(status.message());
    }
}

unique_ptr<Node> convert(std::istream& is) {
    std::shared_ptr<RecordBatchReader> output;
    std::shared_ptr<ArrowInputStream> ais = std::make_shared<ArrowInputStream>(is);
    std::shared_ptr<InputStream> ais_base = std::static_pointer_cast<InputStream>(ais);
    Status status = ipc::RecordBatchStreamReader::Open(ais_base, &output);
    std::shared_ptr<RecordBatch> batch;
    // need to merge the nodes after these record batches
    // or should the merge be done on the python side?
    unique_ptr<ListNode> ln = make_unique<ListNode>();
    ln->get_list() = make_unique<RecordNode>();
    int64_t list_counter = 0;
    while (true) {
        Status batch_status = output->ReadNext(&batch);

        if (!batch_status.ok()) {
            throw std::runtime_error("Error while running Arrow batch reader");
        }

        if (batch) {
            RecordNode& rn = static_cast<RecordNode&>(*ln->get_list().get());
            for (size_t i = 0; i < batch->num_columns(); i++) {
                std::shared_ptr<Array> column = batch->column(i);
                unique_ptr<Node>& column_node = rn.get_field(batch->column_name(i));
                column_node = convert(*column);
            }
            for (size_t i = 0; i < batch->num_rows(); i++) {
                rn.add_not_null();
            }
            list_counter += batch->num_rows();
        } else {
            break;
        }
    }
    ln->add_list(list_counter);
    ln->add_not_null();

    return std::move(ln);
}

}  // namespace arrow
}  // namespace bamboo
