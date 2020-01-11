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

import six

from unittest import TestCase

import json
import io
import numpy as np

import bamboo_cpp_bind as bamboo_cpp
from bamboo import from_json

from bamboo.tests.test_utils import df_equality


class JsonTests(TestCase):
    def convert_obj(self, obj):
        jsons = json.dumps(obj)
        json_stream = io.BytesIO(six.ensure_binary(jsons, 'utf8'))

        return bamboo_cpp.convert_json(json_stream)

    def test_int(self):
        obj = 3
        node = self.convert_obj(obj)
        self.assertListEqual(node.get_values().tolist(), [3])

    def test_struct_with_list(self):
        obj = [{'a': None, 'b': [2, 3]}, {'a': 1, 'b': [2, 4]}]
        node = self.convert_obj(obj)
        self.assertListEqual(node.get_list().get_field('b').get_list().get_values().tolist(), [2, 3, 2, 4])
        self.assertListEqual(node.get_list().get_field('a').get_values().tolist(), [1])
        self.assertListEqual(node.get_list().get_field('a').get_null_indices().tolist(), [0])

    def test_flatten(self):
        obj = [{'a': None, 'b': [1, 2], 'c': [5, 6]}, {'a': -1.0, 'b': [3, 4], 'c': [7, 8]}]
        node = from_json(json.dumps(obj))
        df_a = node.flatten(include=['a'])
        df_b = node.flatten(include=['a', 'b'])
        df_equality(self, {'a': [np.nan, -1.0]}, df_a)
        df_equality(self, {'a': [np.nan, np.nan, -1.0, -1.0], 'b': [1, 2, 3, 4]}, df_b)

    def test_mixed_schema(self):
        with self.assertRaises(ValueError) as context:
            obj = [{'a': None, 'b': [2, False]}, {'a': 1, 'b': [2, 4]}]
            self.convert_obj(obj)

        self.assertTrue('Mismatched primitive types' in str(context.exception))

