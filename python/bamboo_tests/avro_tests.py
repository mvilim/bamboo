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

import sys

from avro import schema
from avro import io
from avro import datafile

import numpy as np
from fastavro import reader
from io import BytesIO

import bamboo_cpp_bind as bamboo_cpp
from bamboo import from_avro

from bamboo.tests.test_utils import df_equality


class PrimitiveSchemas:
    pass


primitive_schemas = PrimitiveSchemas()
if sys.version_info > (3, 0):
    from time import perf_counter as clock

    primitive_schemas.INT = schema.PrimitiveSchema(schema.INT)
    primitive_schemas.LONG = schema.PrimitiveSchema(schema.LONG)
    primitive_schemas.BOOLEAN = schema.PrimitiveSchema(schema.BOOLEAN)
    primitive_schemas.FLOAT = schema.PrimitiveSchema(schema.FLOAT)
    primitive_schemas.DOUBLE = schema.PrimitiveSchema(schema.DOUBLE)
    primitive_schemas.STRING = schema.PrimitiveSchema(schema.STRING)
    primitive_schemas.BYTES = schema.PrimitiveSchema(schema.BYTES)
    primitive_schemas.NULL = schema.PrimitiveSchema(schema.NULL)

    def make_field(field_name, field_schema, names=None):
        return schema.Field(field_schema, field_name, 0, False, names=names, default=None)

    def make_file_writer(out, datum_writer, datum_schema):
        return datafile.DataFileWriter(out, datum_writer, writer_schema=datum_schema)

    def make_array_schema(element_schema):
        return schema.ArraySchema(element_schema)

    def make_union_schema(element_schemas):
        return schema.UnionSchema(element_schemas)
else:
    from time import clock

    primitive_schemas.INT = 'int'
    primitive_schemas.LONG = 'long'
    primitive_schemas.BOOLEAN = 'boolean'
    primitive_schemas.FLOAT = 'float'
    primitive_schemas.DOUBLE = 'double'
    primitive_schemas.STRING = 'string'
    primitive_schemas.BYTES = 'bytes'
    primitive_schemas.NULL = 'null'

    def make_field(field_name, field_schema, names=None):
        if isinstance(field_schema, schema.RecordSchema):
            field_schema = field_schema.fullname
        return schema.Field(field_schema, field_name, False, names=names, default=None).to_json()

    def make_file_writer(out, datum_writer, datum_schema):
        return datafile.DataFileWriter(out, datum_writer, writers_schema=datum_schema)

    def make_array_schema(element_schema):
        names = schema.Names()
        return schema.ArraySchema(element_schema, names)

    def make_union_schema(element_schemas):
        names = schema.Names()
        return schema.UnionSchema(element_schemas, names=names)


def simple_schema(field_name, field_schema):
    names = schema.Names()
    field = make_field(field_name, field_schema)
    return schema.RecordSchema('test', 'test', [field], names=names)


# we write directly to the raw buffer so that writing a large number of objects is not incredibly slow
def write_n_simple_objects(field_name, field_schema, n, value):
    out = BytesIO()
    datum_schema = simple_schema(field_name, field_schema)
    datum_writer = io.DatumWriter(datum_schema)
    file_writer = make_file_writer(out, datum_writer, datum_schema)
    file_writer._block_count += n
    # for small positive integers, zig-zag encoding will be 2x
    file_writer._buffer_writer.write(bytearray(n * [value * 2]))
    file_writer.flush()
    value = out.getvalue()
    return BytesIO(value)


def simple_object(field_name, field_schema, primitive_value):
    out = BytesIO()
    datum_schema = simple_schema(field_name, field_schema)
    datum_writer = io.DatumWriter(datum_schema)
    file_writer = make_file_writer(out, datum_writer, datum_schema=datum_schema)
    file_writer.append({field_name: primitive_value})
    file_writer.flush()
    return BytesIO(out.getvalue())


def object(datum_schema, value, iterate_over_values=False):
    out = BytesIO()
    datum_writer = io.DatumWriter(datum_schema)
    file_writer = make_file_writer(out, datum_writer, datum_schema=datum_schema)
    if iterate_over_values:
        for v in value:
            file_writer.append(v)
    else:
        file_writer.append(value)
    file_writer.flush()
    return BytesIO(out.getvalue())


class AvroTests(TestCase):
    def assert_primitive(self, primitive_schema, primitive_value):
        field_name = 'a'
        b = simple_object(field_name, primitive_schema, primitive_value)
        node = bamboo_cpp.convert_avro(b)
        list_node = node.get_list()
        primitive_node = list_node.get_field(field_name)
        self.assertListEqual(primitive_node.get_values().tolist(), [primitive_value])

    def test_integer(self):
        self.assert_primitive(primitive_schemas.INT, 3)

    def test_long(self):
        self.assert_primitive(primitive_schemas.LONG, 1)

    def test_bool(self):
        self.assert_primitive(primitive_schemas.BOOLEAN, False)

    def test_float(self):
        self.assert_primitive(primitive_schemas.FLOAT, 0.5)

    def test_double(self):
        self.assert_primitive(primitive_schemas.DOUBLE, 0.5)

    def test_string(self):
        self.assert_primitive(primitive_schemas.STRING, 'test')

    def test_bytes(self):
        self.assert_primitive(primitive_schemas.BYTES, b'test')

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
        list_schema = make_array_schema(primitive_schemas.INT)
        value = [1, 2]
        b = object(list_schema, value)
        node = bamboo_cpp.convert_avro(b)
        self.assertEqual(node.get_list().get_list().get_size(), 2)
        self.assertListEqual(node.get_list().get_list().get_values().tolist(), value)

    def test_null(self):
        union_schema = make_union_schema([primitive_schemas.INT, primitive_schemas.NULL])
        value = 1
        values = [value, None]
        b = object(union_schema, values, True)
        node = bamboo_cpp.convert_avro(b)
        list_node = node.get_list()
        self.assertListEqual(list_node.get_values().tolist(), [value])
        self.assertListEqual(list_node.get_null_indices().tolist(), [1])

    def test_flatten(self):
        field_name = 'a'
        b = simple_object(field_name, primitive_schemas.INT, 3)
        node = from_avro(b)
        df = node.flatten()
        df_equality(self, {'a': [3]}, df)

    def test_column_filter(self):
        field_name = 'a'
        b = simple_object(field_name, primitive_schemas.INT, 3)
        node = from_avro(b)
        df = node.flatten()
        df_equality(self, {'a': [3]}, df)

        b = simple_object(field_name, primitive_schemas.INT, 3)
        node = from_avro(b, exclude={'a': {}})
        df = node.flatten()
        df_equality(self, {}, df)

        b = simple_object(field_name, primitive_schemas.INT, 3)
        node = from_avro(b, exclude=['a'])
        df = node.flatten()
        df_equality(self, {}, df)

    def test_deep_column_filter(self):
        names = schema.Names()
        ia = 'ia'
        ib = 'ib'
        oa = 'oa'
        ob = 'ob'
        inner_a = make_field(ia, primitive_schemas.INT, names=names)
        inner_b = make_field(ib, primitive_schemas.INT, names=names)
        inner_a_record = schema.RecordSchema('inner_a', 'test', [inner_a, inner_b], names=names)
        inner_b_record = schema.RecordSchema('inner_b', 'test', [inner_a, inner_b], names=names)
        outer_a = make_field(oa, inner_a_record, names=names)
        outer_b = make_field(ob, inner_b_record, names=names)
        outer_record = schema.RecordSchema('outer', 'test', [outer_a, outer_b], names=schema.Names())

        b = object(outer_record, {oa: {ia: 1, ib: 2}, ob: {ia: 3, ib: 4}})
        node = from_avro(b)
        df = node.flatten()
        df_equality(self, {oa + '_' + ia: [1], oa + '_' + ib: [2], ob + '_' + ia: [3], ob + '_' + ib: [4]}, df)

        b = object(outer_record, {oa: {ia: 1, ib: 2}, ob: {ia: 3, ib: 4}})
        node = from_avro(b, exclude='oa')
        df = node.flatten()
        df_equality(self, {ia: [3], ib: [4]}, df)

        b = object(outer_record, {oa: {ia: 1, ib: 2}, ob: {ia: 3, ib: 4}})
        node = from_avro(b, exclude='oa', include=[{}, 'oa.ia'])
        df = node.flatten()
        df_equality(self, {oa + '_' + ia: [1], ob + '_' + ia: [3], ib: [4]}, df)

    def test_perf(self):
        field_name = 'a'
        n = 1000000
        value = 2
        b = write_n_simple_objects(field_name, primitive_schemas.INT, n, value)
        buf = b.getvalue()
        start = clock()
        node = bamboo_cpp.convert_avro(BytesIO(buf))
        end = clock()
        bamboo_time = end - start
        list_node = node.get_list()
        primitive_node = list_node.get_field(field_name)
        self.assertListEqual(primitive_node.get_values().tolist(), [value] * n)

        result = np.ndarray((n,))
        i = 0
        start = clock()
        for record in reader(b):
            result[i] = record['a']
            i = i + 1
        end = clock()
        fastavro_time = end - start
        self.assertListEqual(result.tolist(), [value] * n)

        bamboo_speedup = fastavro_time / bamboo_time
        self.assertGreater(bamboo_speedup, 10)
