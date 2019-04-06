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

import numpy as np


class ArrayList:
    def __init__(self, values, growth_rate=1.5):
        self.growth_rate = growth_rate
        self.size = values.size
        self.values = values

    @classmethod
    def create(cls, dtype, growth_rate=1.5):
        dtype_to_use = dtype
        if dtype is str:
            dtype_to_use = object
        return ArrayList(np.ndarray(shape=[0], dtype=dtype_to_use), growth_rate)

    def add(self, value):
        # should check if value is correct type
        self.size += 1
        if self.size > self.values.size:
            new_size = int(self.size * self.growth_rate)
            # this is to avoid a numpy reference counting error due to the debugger holding a reference to the array
            if __debug__:
                self.values.resize([new_size], refcheck=False)
            else:
                self.values.resize([new_size])

        self.values[self.size - 1] = value

    def trim(self):
        self.values.resize([self.size])

    def add_to_item(self, index, value):
        self.values[index] += value

    def array(self):
        return self.values[:self.size]

    def __getitem__(self, index):
        if index >= self.size:
            raise Exception('Index out of bounds')
        return self.values[index]

