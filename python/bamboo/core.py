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

from io import IOBase, BytesIO
from typing import Type, Union

from bamboo.nodes import IncompleteNode, Node
from bamboo.nodes import build
from bamboo.converters.obj import PythonObjConverter
from bamboo.converters.extensions import convert_extension_node

import bamboo_cpp_bind as bamboo_cpp


def from_object(obj, dict_as_record=True) -> Node:
    node = IncompleteNode.create()
    converter = PythonObjConverter(dict_as_record)
    return build(obj, node, converter)


def from_avro(s: Type[IOBase]) -> Node:
    extension_node = bamboo_cpp.convert_avro(s)
    return convert_extension_node(extension_node)


def from_arrow(s: Type[IOBase]) -> Node:
    return convert_extension_node(bamboo_cpp.convert_arrow(s))


def from_pbd(s: Type[IOBase]) -> Node:
    return convert_extension_node(bamboo_cpp.convert_pbd(s))


def from_json(s: Union[str, Type[IOBase]]) -> Node:
    if isinstance(s, str):
        extension_node = bamboo_cpp.convert_json(BytesIO(bytes(s, 'utf8')))
    else:
        extension_node = bamboo_cpp.convert_json(s)
    return convert_extension_node(extension_node)
