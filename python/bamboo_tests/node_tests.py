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

from bamboo import from_object, NameStrategy, FlattenStrategy
from bamboo.nodes import column_names
from bamboo.tests.test_utils import df_equality


class SimpleObject:
    def __init__(self, value):
        self.value = value


class ListObject:
    def __init__(self, values):
        self.values = values


class ColumnNameTests(TestCase):
    def test_single_resolution(self):
        names = [['a']]
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS, names)
        self.assertEqual(['a'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS_VERBOSE, names)
        self.assertEqual(['a'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_ALWAYS, names)
        self.assertEqual(['a'], resolved)

    def test_simple_resolution(self):
        names = [['a'], ['b']]
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS, names)
        self.assertEqual(['a', 'b'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS_VERBOSE, names)
        self.assertEqual(['a', 'b'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_ALWAYS, names)
        self.assertEqual(['a', 'b'], resolved)

    def test_partial_conflict_resolution(self):
        names = [['a', 'a'], ['a', 'b'], ['c', 'd']]
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS, names)
        self.assertEqual(['a_a', 'b_a', 'c'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS_VERBOSE, names)
        self.assertEqual(['a_a', 'b_a', 'c'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_ALWAYS, names)
        self.assertEqual(['a_a', 'b_a', 'd_c'], resolved)

    def test_overlapping_names(self):
        names = [['b'], ['b', 'a'], ['b', 'a', 'z']]
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS, names)
        self.assertEqual(['b', 'a_b', 'z_a_b'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS_VERBOSE, names)
        self.assertEqual(['b', 'a_b', 'z_a_b'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_ALWAYS, names)
        self.assertEqual(['b', 'a_b', 'z_a_b'], resolved)

    def test_exact_match(self):
        self.assertRaises(ValueError, lambda: column_names(NameStrategy.CONCATENATE_CONFLICTS, [['b'], ['b']]))

    def test_verbose_conflict_resolution(self):
        names = [['d', 'c', 'a'], ['d', 'c', 'b']]
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS, names)
        self.assertEqual(['a_d', 'b_d'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_CONFLICTS_VERBOSE, names)
        self.assertEqual(['a_c_d', 'b_c_d'], resolved)
        resolved = column_names(NameStrategy.CONCATENATE_ALWAYS, names)
        self.assertEqual(['a_c_d', 'b_c_d'], resolved)

    def test_tuple_naming(self):
        names = [['c', 'a'], ['b']]
        resolved = column_names(NameStrategy.MULTI_INDEX, names)
        self.assertEqual([('a', 'c'), ('b', '')], resolved)


class FlattenTests(TestCase):
    def df_equality(self, expected, df):
        df_equality(self, expected, df)

    def test_attr(self):
        a1 = 1.0
        a2 = None
        a = SimpleObject(a1)
        b = SimpleObject(a2)
        c = ListObject([a, b])
        d = SimpleObject(c)
        node = from_object(d)
        primitive_node = node.value.values.value
        flattened = primitive_node.flatten()
        self.df_equality({'value': [1, np.nan]}, flattened)

    def test_flatten_list(self):
        a1 = 1.0
        a2 = None
        a = SimpleObject(a1)
        b = SimpleObject(a2)
        c = ListObject([a, b])
        d = {'a': c, 'b': 3}
        node = from_object(d)
        flattened = node.flatten()
        self.df_equality({'value': [1, np.nan], 'b': [3, 3]}, flattened)

    def test_flatten_nested_list(self):
        a1 = 1.0
        a2 = None
        a = SimpleObject(a1)
        b = SimpleObject(a2)
        c = ListObject([a, b])
        c_a = ListObject([c, c])
        d = {'a': c_a, 'b': 3}
        node = from_object(d)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'a_values_values_value': [1, np.nan, 1, np.nan], 'b': [3, 3, 3, 3]}, flattened)

    def test_null_record(self):
        a = 1.0
        b = [a, a, a]
        c = {'b': b}
        d = [c, None]
        c = {'d': d, 'e': 2.0}
        node = from_object(c)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'d_b': [1, 1, 1], 'e': [2, 2, 2]}, flattened)

    def test_null_primitive(self):
        a = 1.0
        b = [a, a, None]
        c = {'b': b}
        d = [c, c]
        c = {'d': d, 'e': 2.0}
        node = from_object(c)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'d_b': [1, 1, np.nan, 1, 1, np.nan], 'e': [2, 2, 2, 2, 2, 2]}, flattened)

    def test_null_list(self):
        b = [1.0, 1.0, 5.0]
        c = {'b': b, 'c': 2.0}
        c_null = {'b': None, 'c': 3.0}
        d = [c, c_null]
        c = {'d': d, 'e': 4.0}
        node = from_object(c)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'d_b': [1, 1, 5], 'd_c': [2, 2, 2], 'e': [4, 4, 4]}, flattened)

    def test_simple_flatten(self):
        a1 = 1
        a2 = 2
        a = SimpleObject(a1)
        b = SimpleObject(a2)
        d = {'a': a, 'b': b}
        node = from_object(d)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'a_value': [1], 'b_value': [2]}, flattened)

    def test_flatten_null(self):
        # this demonstrates that when we can't determine the schema, we drop the field entirely. for data formats where
        # the schema is known even when there is no data, should we have a way to fill out an empty node? I guess that
        # is dependent on the converter, as it can create the necessary "empty" nodes (though it must fill out a
        # primitive node at the end)
        a1 = 1
        a2 = None
        a = SimpleObject(a1)
        b = SimpleObject(a2)
        d = {'a': a, 'b': b}
        node = from_object(d)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'a_value': [1]}, flattened)

    def test_strings(self):
        a1 = 'a'
        a = SimpleObject(a1)
        node = from_object(a)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'value': 'a'}, flattened)

    def test_flatten_multiple_lists(self):
        a = [1, 2]
        b = [3, 4]
        c = 1
        d = {'a': a, 'b': b, 'c': c}
        node = from_object(d)
        self.assertRaises(ValueError, node.flatten)
        flattened = node.flatten(flatten_strategy=FlattenStrategy.FLATTEN_AVAILABLE)
        self.df_equality({'c': [c]}, flattened)

    def test_mixed_schema(self):
        a_1 = {'a': 1.0, 'b': 2.0}
        a_2 = {'a': 3.0, 'c': 4.0}
        b = [a_1, a_2]
        node = from_object(b)
        flattened = node.flatten(name_strategy=NameStrategy.CONCATENATE_ALWAYS)
        self.df_equality({'a': [1, 3], 'b': [2, np.nan], 'c': [np.nan, 4]}, flattened)

    def test_exclude(self):
        node = from_object({'a': 1, 'b': 2})
        flattened = node.flatten(exclude={'b'})
        self.df_equality({'a': [1]}, flattened)

    def test_conflicting_clusions(self):
        c = {'a': 1, 'b': 2}
        node = from_object(c)
        self.assertRaises(AssertionError, lambda: node.flatten(include={'a'}, exclude={'a'}))

    def test_multi_index_naming(self):
        node = from_object({'a': {'c': 1}, 'b': 2})
        flattened = node.flatten(name_strategy=NameStrategy.MULTI_INDEX)
        self.df_equality({('a', 'c'): [1], ('b',): [2]}, flattened)

    def test_excluded_lists_are_ignored(self):
        node = from_object([{'a': None, 'b': [2, 3], 'c': [-2, -3]}, {'a': 1, 'b': [2, 4], 'c': [-2, -4]}])
        flattened = node.flatten(include=['a'])
        self.df_equality({'a': [0, 1]}, flattened)
        flattened = node.flatten(include=['b'])
        self.df_equality({'b': [2, 3, 2, 4]}, flattened)

    def test_incomplete_node(self):
        node = from_object([])
        flattened = node.flatten()
        self.df_equality({}, flattened)

    def test_str(self):
        a = 1.0
        b = [a, a, a]
        c = {'b': b}
        d = [c, c]
        c = {'d': d, 'e': 2.0}
        node = from_object(c)
        elements = list()
        strings = {'d': '- d []\n    - b []float64', 'e': '- e float64'}
        for key in node._children:
            elements.append(strings[key])
        s = '\n'.join(elements)
        text = str(node)
        self.assertEqual(s, text)

    def test_prim_only(self):
        a = [1, 2, 3]
        node = from_object(a)
        flattened = node.flatten()
        self.df_equality({None: a}, flattened)

