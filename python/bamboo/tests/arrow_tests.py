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

import pyarrow as pa
import numpy as np
import io as io

import bamboo_cpp_bind as bamboo_cpp
from bamboo import from_arrow

from bamboo.tests.test_utils import df_equality


class ArrowTests(TestCase):
    def to_iostream(self, arr, field_name):
        sink = pa.BufferOutputStream()
        batch = pa.RecordBatch.from_arrays([arr], [field_name])
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        sink.flush()
        return io.BytesIO(sink.getvalue())

    def array_convert(self, arr):
        field_name = 'arr'
        node = bamboo_cpp.convert_arrow(self.to_iostream(arr, field_name))
        return node.get_list().get_field(field_name)

    def assert_array(self, arr):
        node = self.array_convert(arr)
        self.assertListEqual(node.get_values().tolist(), np.array(arr).tolist())

    def test_int8(self):
        arr = pa.array([1, 2], type=pa.int8())
        self.assert_array(arr)

    def test_int32(self):
        arr = pa.array([1, 2], type=pa.int32())
        self.assert_array(arr)

    def test_int64(self):
        arr = pa.array([1, 2], type=pa.int64())
        self.assert_array(arr)

    def test_null_int64(self):
        arr = pa.array([1, None], type=pa.int64())
        node = self.array_convert(arr)
        self.assertListEqual(node.get_values().tolist(), [1])
        self.assertListEqual(node.get_null_indices().tolist(), [1])

    def test_time32(self):
        with self.assertRaises(RuntimeError) as context:
            arr = pa.array(np.array([1, 2], dtype='int32'), pa.time32('s'))
            self.assert_array(arr)

        self.assertTrue('not implemented' in str(context.exception))

    def test_null_time32(self):
        with self.assertRaises(TypeError) as context:
            arr = pa.array(np.array([1, None], dtype='int32'), pa.time32('s'))  # arrow complains about the null int
            self.assert_array(arr)

    def test_float16(self):
        arr = pa.array([np.float16(1), np.float16(2)], type=pa.float16())
        self.assert_array(arr)

    def test_float32(self):
        arr = pa.array([np.float32(1), np.float32(2)], type=pa.float32())
        self.assert_array(arr)

    def test_float64(self):
        arr = pa.array([np.float64(1), np.float64(2)], type=pa.float64())
        self.assert_array(arr)

    def test_null_float64(self):
        arr = pa.array([np.float64(1), None], type=pa.float64())
        node = self.array_convert(arr)
        self.assertListEqual(node.get_values().tolist(), [1])
        self.assertListEqual(node.get_null_indices().tolist(), [1])

    def test_binary(self):
        with self.assertRaises(RuntimeError) as context:
            arr = pa.array('test', type=pa.binary())
            self.assert_array(arr)

        self.assertTrue('not implemented' in str(context.exception))

    def test_bool(self):
        arr = pa.array([False, True], type=pa.bool_())
        self.assert_array(arr)

    def test_dictionary(self):
        indices = pa.array([0, 1, 0, 1, 2, 0, None, 2])
        dictionary = pa.array([u'foo', u'bar', u'baz'])
        arr = pa.DictionaryArray.from_arrays(indices, dictionary)
        node = self.array_convert(arr)
        self.assertListEqual(node.get_values().tolist(), ['foo', 'bar', 'foo', 'bar', 'baz', 'foo', 'baz'])
        self.assertListEqual(node.get_null_indices().tolist(), [6])
        self.assertEqual(node.get_size(), 8)

    def test_list_struct(self):
        arr = pa.array([{'x': 1, 'y': [{'a': 3, 'b': 6}]}, {'x': 2, 'y': [{'a': 4, 'b': 7}, {'a': 5, 'b': 8}]}])
        node = self.array_convert(arr)
        self.assertEqual(node.get_field('y').get_list().get_size(), 3)
        self.assertListEqual(node.get_field('y').get_list().get_field('b').get_values().tolist(), [6, 7, 8])

    def test_list_of_list(self):
        arr = pa.array([[1, 2, None], None, [3]])
        node = self.array_convert(arr)
        self.assertEqual(node.get_size(), 3)
        self.assertListEqual(node.get_index().tolist(), [3, 1])
        self.assertListEqual(node.get_null_indices().tolist(), [1])
        self.assertEqual(node.get_list().get_size(), 4)
        self.assertListEqual(node.get_list().get_values().tolist(), [1, 2, 3])
        self.assertListEqual(node.get_list().get_null_indices().tolist(), [2])

    def test_flatten(self):
        arr = pa.array([{'x': 1, 'y': [{'a': 3, 'b': 6}]}, {'x': 2, 'y': [{'a': 4, 'b': 7}, {'a': 5, 'b': 8}]}])
        field_name = 'arr'
        b = self.to_iostream(arr, field_name)
        node = from_arrow(b)
        df = node.flatten()
        df_equality(self, {'x': [1, 2, 2], 'a': [3, 4, 5], 'b': [6, 7, 8]}, df)

