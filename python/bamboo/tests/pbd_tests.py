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
from unittest import TestCase

from bamboo import from_pbd

from bamboo.tests.test_utils import df_equality


class PBDTests(TestCase):
    def test_example(self):
        file = open(os.path.join(os.path.dirname(__file__), 'data', 'example.pbd'), 'rb')
        example = file.read()  # if you only wanted to read 512 bytes, do .read(512)
        file.close()

        node = from_pbd(io.BytesIO(example))
        df = node.flatten()

        df_equality(self, {'a': [13, 13], 'b': [23, 23], 'c': [33, 33],
                           'd': [-1.3, -1.3], 'e': ['B', 'B'], 'f': [2.3, 3.3]}, df)
