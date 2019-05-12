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

import io
import os
import sys

from unittest import TestCase

from bamboo import from_pbd

from bamboo.tests.test_utils import df_equality

if sys.version_info > (3, 0):
    from time import perf_counter as clock
else:
    from time import clock


class PBDTests(TestCase):
    def read_example(self, include=None, exclude=None):
        file = open(os.path.join(os.path.dirname(__file__), 'data', 'example.pbd'), 'rb')
        example = file.read()
        file.close()

        return from_pbd(io.BytesIO(example), include=include, exclude=exclude)

    def test_perf(self):
        file = open(os.path.join(os.path.dirname(__file__), 'data', 'perf_example.pbd'), 'rb')
        example = io.BytesIO(file.read())
        file.close()
        n_record_bytes = 82
        header_bytes = bytearray(example.getvalue()[:-n_record_bytes])
        record_bytes = bytearray(example.getvalue()[-n_record_bytes:])
        n = 1000000
        b = header_bytes + record_bytes * n

        t0 = clock()
        node = from_pbd(io.BytesIO(b))
        t1 = clock()
        df = node.flatten()
        t2 = clock()

        self.assertLess(t1 - t0, 5)
        self.assertLess(t2 - t1, 1)
        self.assertEqual(len(df), n)

    def test_example(self):
        df = self.read_example().flatten(exclude=['rm'])

        df_equality(self, {'a': [13, 13], 'b': [23, 23], 'c': [33, 33],
                           'd': [-1.3, -1.3], 'e': ['B', 'B'], 'f': [2.3, 3.3], 's': ['test', 'test'],
                           'sd': ['', ''], 'de': ['DE1', 'DE1']}, df)

    def test_repeated_message(self):
        node = self.read_example()
        df = node.flatten(include=['rm'])

        self.assertEqual(node.rm._value._index.lengths.array().tolist(), [2])
        df_equality(self, {'b': [11, 22]}, df)

    def test_inclusion(self):
        df = self.read_example(include=['a']).flatten()
        df_equality(self, {'a': [13]}, df)

    def test_exclusion(self):
        df = self.read_example(exclude='m.b').flatten(exclude=['rm'])
        df_equality(self, {'a': [13, 13], 'c': [33, 33],
                           'd': [-1.3, -1.3], 'e': ['B', 'B'], 'f': [2.3, 3.3], 's': ['test', 'test'],
                           'sd': ['', ''], 'de': ['DE1', 'DE1']}, df)

    def test_conflict(self):
        read = lambda: self.read_example(include='m.b', exclude='m.b').flatten()
        self.assertRaises(Exception, read)
