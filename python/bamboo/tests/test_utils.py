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


# we should make failures from these assertions easier to understand (print the differences)
def df_equality(test_case: TestCase, expected, df, float_comparison=True):
    test_case.assertEqual(len(expected), len(df.columns))
    for key, value in expected.items():
        array = df[key]
        test_case.assertEqual(array.size, len(value))
        if float_comparison:
            test_case.assertTrue(np.allclose(array, np.array(value), equal_nan=True))
        else:
            test_case.assertTrue(np.array_equal(array.values, np.array(value)))
