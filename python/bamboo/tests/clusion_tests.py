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

from bamboo.clusions import parse_clusions, Clusion


class ClusionTests(TestCase):
    def test_str(self):
        c = parse_clusions('a')
        self.assertEqual(c, Clusion(False, {'a': Clusion(True, {})}))

    def test_list(self):
        c = parse_clusions(['a', 'b'])
        self.assertEqual(c, Clusion(False, {'a': Clusion(True, {}), 'b': Clusion(True, {})}))

    def test_separators(self):
        c = parse_clusions('a.b.c')
        self.assertEqual(c, Clusion(False, {'a': Clusion(False, {'b': Clusion(False, {'c': Clusion(True, {})})})}))

    def test_merging(self):
        c = parse_clusions([{'a': {'b': {}}}, {'a': {'b': {'c': {}}}}])
        self.assertEqual(c, Clusion(False, {'a': Clusion(False, {'b': Clusion(True, {'c': Clusion(True, {})})})}))

    def test_merging_with_separators(self):
        c = parse_clusions(['a.b.c', 'a.b.d'])
        self.assertEqual(c, Clusion(False, {
            'a': Clusion(False, {'b': Clusion(False, {'c': Clusion(True, {}), 'd': Clusion(True, {})})})}))

    def test_dict(self):
        c = parse_clusions({'a': 'b'})
        self.assertEqual(c, Clusion(False, {'a': Clusion(False, {'b': Clusion(True, {})})}))

    def test_dict_with_separators(self):
        c = parse_clusions({'a.b': 'c'})
        self.assertEqual(c, Clusion(False, {'a': Clusion(False, {'b': Clusion(False, {'c': Clusion(True, {})})})}))

    def test_empty_dict(self):
        c = parse_clusions({})
        self.assertEqual(c, Clusion(True, {}))

    def test_empty_str(self):
        def run():
            parse_clusions('')
        self.assertRaises(Exception, run)

    def test_empty_list(self):
        c = parse_clusions([])
        self.assertEqual(c, Clusion(True, {}))

    def test_none(self):
        c = parse_clusions(None)
        self.assertEqual(c, Clusion(False, {}))
