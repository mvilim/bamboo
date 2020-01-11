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
import io as io

from bamboo_tests.test_utils import df_equality

# because of a conflict between our compiled arrow and pyarrow shared libraries (TBD) and static linking has another problem, pyarrow and bamboo cannot be both run in the same interpreter, so we have to run these functions in another process to generate arrow data
import multiprocessing as mp

FIELD_NAME = 'arr'

def pyarrow_runner(pipe):
    while True:
        r = pipe.recv()
        if r is None:
            pipe.close()
            break
        else:
            pipe.send(r())

def convert(arr, create_list=True):
    import pyarrow as pa
    sink = pa.BufferOutputStream()
    batch = pa.RecordBatch.from_arrays([arr], [FIELD_NAME])
    writer = pa.RecordBatchStreamWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    sink.flush()
    if create_list:
        l = np.array(arr).tolist()
    else:
        l = None
    return bytes(sink.getvalue()), l

def create_int8():
    import pyarrow as pa
    return convert(pa.array([1, 2], type=pa.int8()))


def create_int32():
    import pyarrow as pa
    return convert(pa.array([1, 2], type=pa.int32()))


def create_int64():
    import pyarrow as pa
    return convert(pa.array([1, 2], type=pa.int64()))


def create_null_int64():
    import pyarrow as pa
    return convert(pa.array([1, None], type=pa.int64()))


def create_time32():
    import pyarrow as pa
    return convert(pa.array(np.array([1, 2], dtype='int32'), pa.time32('s')))


def create_null_time32():
    import pyarrow as pa
    return convert(pa.array(np.array([1, None], dtype='int32'), pa.time32('s')))  # arrow complains about the null int


def create_float16():
    import pyarrow as pa
    return convert(pa.array([np.float16(1), np.float16(2)], type=pa.float16()))


def create_float32():
    import pyarrow as pa
    return convert(pa.array([np.float32(1), np.float32(2)], type=pa.float32()))


def create_float64():
    import pyarrow as pa
    return convert(pa.array([np.float64(1), np.float64(2)], type=pa.float64()))


def create_null_float64():
    import pyarrow as pa
    return convert(pa.array([np.float64(1), None], type=pa.float64()))


def create_binary():
    import pyarrow as pa
    return convert(pa.array('test', type=pa.binary()))


def create_bool():
    import pyarrow as pa
    return convert(pa.array([False, True], type=pa.bool_()))


def create_dictionary():
    import pyarrow as pa
    indices = pa.array([0, 1, 0, 1, 2, 0, None, 2])
    dictionary = pa.array([u'foo', u'bar', u'baz'])
    return convert(pa.DictionaryArray.from_arrays(indices, dictionary), create_list=False)


def create_list_struct():
    import pyarrow as pa
    return convert(pa.array([{'x': 1, 'y': [{'a': 3, 'b': 6}]}, {'x': 2, 'y': [{'a': 4, 'b': 7}, {'a': 5, 'b': 8}]}]), create_list=False)


def create_list_of_list():
    import pyarrow as pa
    return convert(pa.array([[1, 2, None], None, [3]]), create_list=False)


def create_flatten():
    import pyarrow as pa
    return convert(pa.array([{'x': 1, 'y': [{'a': 3, 'b': 6}]}, {'x': 2, 'y': [{'a': 4, 'b': 7}, {'a': 5, 'b': 8}]}]), create_list=False)

class ArrowTests(TestCase):
    def pa(self, f):
        type(self)._parent_pipe.send(f)
        return type(self)._parent_pipe.recv()

    @classmethod
    def stop(cls):
        cls._parent_pipe.send(None)
        cls._runner.join()

    @classmethod
    def setUpClass(cls):
        context = mp.get_context('spawn')
        cls._parent_pipe, child_pipe = context.Pipe()
        cls._runner = context.Process(target=pyarrow_runner, args=(child_pipe,))
        cls._runner.start()

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def array_convert(self, b):
        import bamboo_cpp_bind as bamboo_cpp
        node = bamboo_cpp.convert_arrow(io.BytesIO(b))
        return node.get_list().get_field(FIELD_NAME)

    def assert_array(self, b, arr):
        node = self.array_convert(b)
        self.assertListEqual(node.get_values().tolist(), arr)

    def test_int8(self):
        b, arr = self.pa(create_int8)
        self.assert_array(b, arr)

    def test_int32(self):
        b, arr = self.pa(create_int32)
        self.assert_array(b, arr)

    def test_int64(self):
        b, arr = self.pa(create_int64)
        self.assert_array(b, arr)

    def test_null_int64(self):
        b, arr = self.pa(create_null_int64)
        node = self.array_convert(b)
        self.assertListEqual(node.get_values().tolist(), [1])
        self.assertListEqual(node.get_null_indices().tolist(), [1])

    def test_time32(self):
        with self.assertRaises(RuntimeError) as context:
            b, arr = self.pa(create_time32)
            self.assert_array(b, arr)

        self.assertTrue('not implemented' in str(context.exception))

    # arrow does not support null time32
    #def test_null_time32(self):
    #    with self.assertRaises(TypeError) as context:
    #        arr = pa.array(np.array([1, None], dtype='int32'), pa.time32('s'))
    #        self.assert_array(arr)

    def test_float16(self):
        b, arr = self.pa(create_float16)
        self.assert_array(b, arr)

    def test_float32(self):
        b, arr = self.pa(create_float32)
        self.assert_array(b, arr)

    def test_float64(self):
        b, arr = self.pa(create_float64)
        self.assert_array(b, arr)

    def test_null_float64(self):
        b, arr = self.pa(create_null_float64)
        node = self.array_convert(b)
        self.assertListEqual(node.get_values().tolist(), [1])
        self.assertListEqual(node.get_null_indices().tolist(), [1])

    def test_binary(self):
        b, arr = self.pa(create_binary)
        with self.assertRaises(RuntimeError) as context:
            self.assert_array(b, arr)

        self.assertTrue('not implemented' in str(context.exception))

    def test_bool(self):
        b, arr = self.pa(create_bool)
        self.assert_array(b, arr)

    def test_dictionary(self):
        b, arr = self.pa(create_dictionary)
        node = self.array_convert(b)
        self.assertListEqual(node.get_values().tolist(), ['foo', 'bar', 'foo', 'bar', 'baz', 'foo', 'baz'])
        self.assertListEqual(node.get_null_indices().tolist(), [6])
        self.assertEqual(node.get_size(), 8)

    def test_list_struct(self):
        b, arr = self.pa(create_list_struct)
        node = self.array_convert(b)
        self.assertEqual(node.get_field('y').get_list().get_size(), 3)
        self.assertListEqual(node.get_field('y').get_list().get_field('b').get_values().tolist(), [6, 7, 8])

    def test_list_of_list(self):
        b, arr = self.pa(create_list_of_list)
        node = self.array_convert(b)
        self.assertEqual(node.get_size(), 3)
        self.assertListEqual(node.get_index().tolist(), [3, 1])
        self.assertListEqual(node.get_null_indices().tolist(), [1])
        self.assertEqual(node.get_list().get_size(), 4)
        self.assertListEqual(node.get_list().get_values().tolist(), [1, 2, 3])
        self.assertListEqual(node.get_list().get_null_indices().tolist(), [2])

    def test_flatten(self):
        from bamboo import from_arrow
        b, arr = self.pa(create_flatten)
        node = from_arrow(io.BytesIO(b))
        df = node.flatten()
        df_equality(self, {'x': [1, 2, 2], 'a': [3, 4, 5], 'b': [6, 7, 8]}, df)

