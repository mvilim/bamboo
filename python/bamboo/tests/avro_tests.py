# Copyright (c) 2019 Michael Vilim
# 
# This file is part of the bamboo library. It is currently hosted at
# https://github.com/mvilim/bamboo
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import TestCase

import numpy as np
from fastavro import reader
from io import BytesIO
from avro import schema
from avro import io
from avro import datafile

from time import perf_counter

import bamboo_cpp_bind as bamboo_cpp
from bamboo import from_avro

from bamboo.tests.test_utils import df_equality


def simple_schema(field_name, primitive_schema):
    names = schema.Names()
    field = schema.Field(schema.PrimitiveSchema(primitive_schema), field_name, 0, False)
    return schema.RecordSchema('test', 'test', [field], names=names)


# we write directly to the raw buffer so that writing a large number of objects is not incredibly slow
def write_n_simple_objects(field_name, primitive_schema, n, value):
    buffer = BytesIO()
    obj_schema = simple_schema(field_name, primitive_schema)
    datum_writer = io.DatumWriter(obj_schema)
    file_writer = datafile.DataFileWriter(buffer, datum_writer, writer_schema=obj_schema)
    file_writer._block_count += n
    # for small positive integers, zig-zag encoding will be 2x
    file_writer._buffer_writer.write(bytes(n * [value * 2]))
    file_writer.flush()
    value = buffer.getvalue()
    return BytesIO(value)


def simple_object(field_name, primitive_schema, primitive_value):
    buffer = BytesIO()
    obj_schema = simple_schema(field_name, primitive_schema)
    datum_writer = io.DatumWriter(obj_schema)
    file_writer = datafile.DataFileWriter(buffer, datum_writer, writer_schema=obj_schema)
    file_writer.append({field_name: primitive_value})
    file_writer.flush()
    return BytesIO(buffer.getvalue())


def object(obj_schema, value, iterate_over_values=False):
    buffer = BytesIO()
    datum_writer = io.DatumWriter(obj_schema)
    file_writer = datafile.DataFileWriter(buffer, datum_writer, writer_schema=obj_schema)
    if iterate_over_values:
        for v in value:
            file_writer.append(v)
    else:
        file_writer.append(value)
    file_writer.flush()
    return BytesIO(buffer.getvalue())


class AvroTests(TestCase):
    def assert_primitive(self, primitive_schema, primitive_value):
        field_name = 'a'
        b = simple_object(field_name, primitive_schema, primitive_value)
        node = bamboo_cpp.convert_avro(b)
        list_node = node.get_list()
        primitive_node = list_node.get_field(field_name)
        self.assertListEqual(primitive_node.get_values().tolist(), [primitive_value])

    def test_integer(self):
        self.assert_primitive(schema.INT, 3)

    def test_long(self):
        self.assert_primitive(schema.LONG, 1)

    def test_bool(self):
        self.assert_primitive(schema.BOOLEAN, False)

    def test_float(self):
        self.assert_primitive(schema.FLOAT, 0.5)

    def test_double(self):
        self.assert_primitive(schema.DOUBLE, 0.5)

    def test_string(self):
        self.assert_primitive(schema.STRING, 'test')

    def test_bytes(self):
        self.assert_primitive(schema.BYTES, b'test')

    def test_fixed(self):
        names = schema.Names()
        fixed_schema = schema.FixedSchema("test", "test", 3, names=names)
        value = b'abc'
        b = object(fixed_schema, value)
        node = bamboo_cpp.convert_avro(b)
        self.assertListEqual(node.get_list().get_values().tolist(), [value])

    def test_enum(self):
        names = schema.Names()
        enum_schema = schema.EnumSchema("test", "test", ['a', 'b'], names=names)
        b = object(enum_schema, 'b')
        node = bamboo_cpp.convert_avro(b)
        self.assertListEqual(node.get_list().get_values().tolist(), ['b'])

    def test_list(self):
        list_schema = schema.ArraySchema(schema.PrimitiveSchema(schema.INT))
        value = [1, 2]
        b = object(list_schema, value)
        node = bamboo_cpp.convert_avro(b)
        self.assertEqual(node.get_list().get_list().get_size(), 2)
        self.assertListEqual(node.get_list().get_list().get_values().tolist(), value)

    def test_null(self):
        union_schema = schema.UnionSchema([schema.PrimitiveSchema(schema.INT), schema.PrimitiveSchema(schema.NULL)])
        value = 1
        values = [value, None]
        b = object(union_schema, values, True)
        node = bamboo_cpp.convert_avro(b)
        list_node = node.get_list()
        self.assertListEqual(list_node.get_values().tolist(), [value])
        self.assertListEqual(list_node.get_null_indices().tolist(), [1])

    def test_flatten(self):
        field_name = 'a'
        b = simple_object(field_name, schema.INT, 3)
        node = from_avro(b)
        df = node.flatten()
        df_equality(self, {'a': [3]}, df)

    def test_perf(self):
        field_name = 'a'
        n = 10_000_000
        value = 2
        b = write_n_simple_objects(field_name, schema.INT, n, value)
        buf = b.getvalue()
        start = perf_counter()
        node = bamboo_cpp.convert_avro(BytesIO(buf))
        end = perf_counter()
        bamboo_time = end - start
        list_node = node.get_list()
        primitive_node = list_node.get_field(field_name)
        self.assertListEqual(primitive_node.get_values().tolist(), [value] * n)

        result = np.ndarray((n,))
        i = 0
        start = perf_counter()
        for record in reader(b):
            result[i] = record['a']
            i = i + 1
        end = perf_counter()
        fastavro_time = end - start
        self.assertListEqual(result.tolist(), [value] * n)

        bamboo_speedup = fastavro_time / bamboo_time
        self.assertGreater(bamboo_speedup, 10)
