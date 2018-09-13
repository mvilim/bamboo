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

import bamboo_cpp_bind as bc

from bamboo.nodes import Node, IncompleteNode, ListNode, PrimitiveNode, RecordNode, RecordField, IndexNullIndicator, \
    OrderedRangeIndex
from bamboo.util import ArrayList


def add_node_reference(node, extension_node):
    # because pybind's reference_internal return value policy does not not appear to work with our zero-copy numpy
    # buffers, we store a reference to the extension node to prevent garbage collection of the node that backs our view
    node.__extension_node = extension_node
    return node


def convert_extension_node(node):
    null_indicator = IndexNullIndicator(ArrayList(node.get_null_indices()), node.get_size())
    if isinstance(node, bc.RecordNode):
        fields = [RecordField(name, convert_extension_node(node.get_field(name))) for name in node.get_fields()]
        return add_node_reference(RecordNode(fields, null_indicator), node)
    elif isinstance(node, bc.ListNode):
        index = OrderedRangeIndex(ArrayList(node.get_index()))
        return add_node_reference(ListNode(convert_extension_node(node.get_list()), index, null_indicator), node)
    elif isinstance(node, bc.PrimitiveNode):
        return add_node_reference(PrimitiveNode(ArrayList(node.get_values()), null_indicator), node)
    elif isinstance(node, bc.IncompleteNode):
        return add_node_reference(IncompleteNode(null_indicator), node)
    else:
        raise RuntimeError('Unexpected node type')
