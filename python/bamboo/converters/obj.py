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

from enum import Enum

import numpy as np

from bamboo.nodes import IncompleteNode, ListNode, PrimitiveNode, RecordNode, Converter


class KeyValuePair:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class PythonObjConverter(Converter):
    def __init__(self, dict_as_record=True):
        super(PythonObjConverter, self).__init__(self.type, self.field_names, self.extract_field, self.extract_list)
        self.dict_as_record = dict_as_record

    def is_iterable_dict(self, obj):
        return isinstance(obj, dict) and not self.dict_as_record

    def is_numpy_iterable(self, obj):
        return isinstance(obj, np.ndarray)

    def is_iterable(self, obj):
        return isinstance(obj, list) or isinstance(obj, set) or self.is_iterable_dict(obj) \
               or self.is_numpy_iterable(obj)

    def type(self, obj):
        if obj is None:
            return IncompleteNode
        elif self.is_iterable(obj):
            return ListNode
        elif isinstance(obj, (int, float, bool, str, Enum)):
            return PrimitiveNode
        else:
            return RecordNode

    def field_names(self, obj):
        if isinstance(obj, dict):
            return list(obj.keys())
        else:
            return list(vars(obj).keys())

    def extract_field(self, obj, name):
        if isinstance(obj, dict):
            return obj[name]
        else:
            return vars(obj)[name]

    def extract_list(self, obj):
        if self.is_iterable_dict(obj):
            pairs = list()
            for key, value in obj.items():
                pairs.append(KeyValuePair(key, value))
            return pairs
        elif self.is_numpy_iterable(obj):
            return obj
        return obj
