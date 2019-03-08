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

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <arrow.hpp>
#include <avro_direct.hpp>
#include <avro_generic.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/stream.hpp>
#include <iostream>
#include <json.hpp>

namespace py = pybind11;
namespace io = boost::iostreams;

using std::streamsize;
using std::string;
using std::vector;

namespace bamboo {

template <class T, class F> class BufferVector {
   public:
    vector<F>& vec;

    // this const cast is required to expose the const (on the C++ side at
    // least) vector to the python side (which can technically break the
    // constness)
    BufferVector(const vector<F>& vec) : vec(const_cast<vector<F>&>(vec)) {}

    py::buffer_info buffer() {
        return py::buffer_info(vec.data(), sizeof(T), py::format_descriptor<T>::format(), 1,
                               {vec.size() * sizeof(F) / sizeof(T)}, {sizeof(T)});
    }
};

py::array convert_strings(const std::vector<std::string>& str) {
    py::dtype obj = py::dtype("O");
    vector<py::str> strc;
    for (const string& s : str) {
        strc.push_back(s);
    }
    return py::array(obj, strc.size(), strc.data());
}

py::array get_strings(PrimitiveVector& vec) {
    return convert_strings(vec.get_values<PrimitiveType::STRING>());
};

py::array get_node_strings(PrimitiveNode& node) {
    return get_strings(*node.get_vector());
};

py::array get_unicode_strings(PrimitiveNode& node) {
    return get_strings(*node.get_vector()).attr("astype")('U');
};

template <class T, class F> py::object as_array(const vector<F>& vec) {
    return py::module::import("numpy").attr("array")(BufferVector<T, F>(vec),
                                                     py::arg("copy") = false);
}

py::object as_dtype(py::object vec, string dtype) {
    return vec.attr("view")(py::module::import("numpy").attr(dtype.c_str()));
}

template <class T> py::object s_as_array(const vector<T>& vec) {
    return as_array<T, T>(vec);
}

size_t get_size(NullIndicator& null_indicator) {
    return null_indicator.get_size();
};

py::object get_indices(NullIndicator& null_indicator) {
    return s_as_array(null_indicator.get_indices());
};

py::array get_bytes(PrimitiveVector& vec) {
    py::dtype obj = py::dtype("O");
    const vector<vector<uint8_t>>& bytes = vec.get_values<PrimitiveType::BYTE_ARRAY>();
    vector<py::bytes> arr;
    for (const vector<uint8_t>& b : bytes) {
        string s(b.begin(), b.end());
        arr.push_back(py::bytes(s));
    }
    return py::array(obj, arr.size(), arr.data());
}

py::object get_enum_indices(PrimitiveVector& vec) {
    return s_as_array<size_t>(vec.get_enums().index);
};

py::object get_node_enum_indices(PrimitiveNode& node) {
    return get_enum_indices(*node.get_vector());
};

py::object get_enum_values(PrimitiveVector& vec);

template <class T, class F> void buffer(py::handle m) {
    const char* name = typeid(BufferVector<T, F>).name();
    // don't try to register the same buffer vectors twice (this can happen in the case of
    // overlapping types, e.g. size_t and uint64_t on many 64 bit systems)
    if (!hasattr(m, name)) {
        py::class_<BufferVector<T, F>>(m, name, py::buffer_protocol())
            .def_buffer([](BufferVector<T, F>& a) -> py::buffer_info { return a.buffer(); });
    }
};

template <class T> void sbuffer(py::handle m) {
    buffer<T, T>(m);
};

class PythonBufferStream {
   public:
    typedef char char_type;
    typedef io::source_tag category;

    PythonBufferStream(py::object& pystream)
        : pystream(pystream), pystream_readinto(pystream.attr("readinto")) {}

    PythonBufferStream(const PythonBufferStream& other)
        : pystream(other.pystream), pystream_readinto(other.pystream_readinto){};

    streamsize read(char_type* s, streamsize n) {
        py::buffer_info info(s, sizeof(char_type), py::format_descriptor<char_type>::format(), 1,
                             {n}, {sizeof(char_type)});
        py::memoryview mv(info);
        streamsize num_read = pystream_readinto(mv).cast<streamsize>();
        return num_read != 0 ? num_read : -1;
    }

   private:
    py::object& pystream;
    const py::detail::str_attr_accessor pystream_readinto;
};

constexpr size_t DEFAULT_BUFFER_SIZE = 65536;

static auto convert(std::function<unique_ptr<Node>(std::istream&)> converter) {
    return [converter](py::object stream) -> unique_ptr<Node> {
        // the large buffer is necessary to amortize the cost of a
        // locking stream buffer managed on the Python side (the larger the
        // buffer, the fewer times we run python code)
        io::stream<PythonBufferStream> is(PythonBufferStream(stream), DEFAULT_BUFFER_SIZE);
        is.exceptions(std::istream::badbit);
        return converter(is);
    };
}

py::object extract_values(PrimitiveVector& vec) {
    // it would be nice if we could automatically map these
    switch (vec.get_type()) {
        case PrimitiveType::BOOL:
            return as_array<bool, uint8_t>(vec.get_values<PrimitiveType::BOOL>());
        case PrimitiveType::CHAR:
            return s_as_array(vec.get_values<PrimitiveType::CHAR>());
        case PrimitiveType::UINT8:
            return s_as_array(vec.get_values<PrimitiveType::UINT8>());
        case PrimitiveType::UINT16:
            return s_as_array(vec.get_values<PrimitiveType::UINT16>());
        case PrimitiveType::UINT32:
            return s_as_array(vec.get_values<PrimitiveType::UINT32>());
        case PrimitiveType::UINT64:
            return s_as_array(vec.get_values<PrimitiveType::UINT64>());
        case PrimitiveType::INT8:
            return s_as_array(vec.get_values<PrimitiveType::INT8>());
        case PrimitiveType::INT16:
            return s_as_array(vec.get_values<PrimitiveType::INT16>());
        case PrimitiveType::INT32:
            return s_as_array(vec.get_values<PrimitiveType::INT32>());
        case PrimitiveType::INT64:
            return s_as_array(vec.get_values<PrimitiveType::INT64>());
        case PrimitiveType::FLOAT16:
            return as_dtype(s_as_array(vec.get_values<PrimitiveType::FLOAT16>()), "float16");
        case PrimitiveType::FLOAT32:
            return s_as_array(vec.get_values<PrimitiveType::FLOAT32>());
        case PrimitiveType::FLOAT64:
            return s_as_array(vec.get_values<PrimitiveType::FLOAT64>());
        case PrimitiveType::STRING:
            return get_strings(vec);
        case PrimitiveType::BYTE_ARRAY:
            return get_bytes(vec);
        case PrimitiveType::ENUM:
            return get_enum_values(vec).attr("__getitem__")(get_enum_indices(vec));
        default:
            throw std::runtime_error("Unknown primitive type");
    }
}

py::object get_enum_values(PrimitiveVector& vec) {
    // TODO: because we don't take ownership of the unique_ptr to the enum values, it
    // appears that we have a dangling reference here
    return extract_values(vec.get_enums().values->get_enums());
};

py::object get_node_enum_values(PrimitiveNode& node) {
    return get_enum_values(*node.get_vector());
};

PYBIND11_MODULE(bamboo_cpp_bind, m) {
    sbuffer<size_t>(m);
    sbuffer<int8_t>(m);
    sbuffer<int16_t>(m);
    sbuffer<int32_t>(m);
    sbuffer<int64_t>(m);
    sbuffer<uint8_t>(m);
    sbuffer<uint16_t>(m);
    sbuffer<uint32_t>(m);
    sbuffer<uint64_t>(m);
    sbuffer<float>(m);
    sbuffer<double>(m);
    buffer<bool, uint8_t>(m);

    py::class_<ListNode>(m, "ListNode")
        .def("get_size", [](ListNode& node) { return get_size(node); })
        .def("get_null_indices", [](ListNode& node) { return get_indices(node); },
             py::return_value_policy::reference_internal)
        .def("get_index", [](ListNode& node) { return as_array<size_t, size_t>(node.get_index()); },
             py::return_value_policy::reference_internal)
        .def("get_list", [](ListNode& node) -> Node& { return *node.get_list(); },
             py::return_value_policy::reference_internal);

    py::class_<RecordNode>(m, "RecordNode")
        .def("get_size", [](RecordNode& node) { return get_size(node); })
        .def("get_null_indices", [](RecordNode& node) { return get_indices(node); },
             py::return_value_policy::reference_internal)
        .def("get_field",
             [](RecordNode& node, std::string name) -> Node& { return *node.get_field(name); },
             py::return_value_policy::reference_internal)
        .def("get_fields", &RecordNode::get_fields);

    py::enum_<PrimitiveType>(m, "PrimitiveType")
        .value("EMPTY", PrimitiveType::EMPTY)
        .value("BOOL", PrimitiveType::BOOL)
        .value("CHAR", PrimitiveType::CHAR)
        .value("INT8", PrimitiveType::INT8)
        .value("INT16", PrimitiveType::INT16)
        .value("INT32", PrimitiveType::INT32)
        .value("INT64", PrimitiveType::INT64)
        .value("UINT8", PrimitiveType::UINT8)
        .value("UINT16", PrimitiveType::UINT16)
        .value("UINT32", PrimitiveType::UINT32)
        .value("UINT64", PrimitiveType::UINT64)
        .value("FLOAT16", PrimitiveType::FLOAT16)
        .value("FLOAT32", PrimitiveType::FLOAT32)
        .value("FLOAT64", PrimitiveType::FLOAT64)
        .value("STRING", PrimitiveType::STRING)
        .value("ENUM", PrimitiveType::ENUM)
        .value("BYTE_ARRAY", PrimitiveType::BYTE_ARRAY);

    py::class_<PrimitiveNode>(m, "PrimitiveNode")
        .def("get_size", [](PrimitiveNode& node) { return get_size(node); })
        .def("get_null_indices", [](PrimitiveNode& node) { return get_indices(node); },
             py::return_value_policy::reference_internal)
        .def("get_values",
             [](PrimitiveNode& node) -> py::object { return extract_values(*node.get_vector()); },
             py::return_value_policy::reference_internal)  // this is inconsistent -- some of the
                                                           // return types (i.e. string) create
                                                           // copies, some only create views; we may
                                                           // keep unneeded copies in memory for
                                                           // string values
        .def("get_type", &PrimitiveNode::get_type)
        .def("get_strings", &get_node_strings)
        .def("get_unicode_strings", &get_unicode_strings)
        .def("get_enum_values", &get_node_enum_values, py::return_value_policy::reference_internal)
        .def("get_enum_indices", &get_node_enum_indices,
             py::return_value_policy::reference_internal);

    py::class_<IncompleteNode>(m, "IncompleteNode")
        .def("get_size", [](PrimitiveNode& node) { return get_size(node); })
        .def("get_null_indices", [](PrimitiveNode& node) { return get_indices(node); },
             py::return_value_policy::reference_internal);

    m.def("convert_avro", convert(bamboo::avro::direct::convert));

    m.def("convert_arrow", convert(bamboo::arrow::convert));

    m.def("convert_json", convert(bamboo::json::convert));

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "dev";
#endif
}
}  // namespace bamboo
