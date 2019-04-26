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
import pandas as pd
import numpy as np
from bamboo.util import ArrayList


class NullIndicator(object):
    def add_null(self):
        raise NotImplementedError('Must be overridden in subclass')

    def add_not_null(self):
        raise NotImplementedError('Must be overridden in subclass')

    def not_null_indices(self):
        raise NotImplementedError('Must be overridden in subclass')

    def size(self):
        raise NotImplementedError('Must be overridden in subclass')

    def null_size(self):
        raise NotImplementedError('Must be overridden in subclass')


class IndexNullIndicator(NullIndicator):
    def __init__(self, indices, size):
        self._indices = indices
        self._size = size

    def add_not_null(self):
        self._size += 1

    def add_null(self):
        self._indices.add(self._size)
        self._size += 1

    def not_null_indices(self):
        all_indices = np.arange(0, self._size)
        null_indices = self._indices.values[:self._indices.size]
        return np.delete(all_indices, null_indices)

    def size(self):
        return self._size

    def null_size(self):
        return self._indices.size

    @classmethod
    def create(cls):
        indices = ArrayList.create(np.int32)
        return IndexNullIndicator(indices, 0)


def fill_value(dtype):
    if np.issubdtype(dtype, np.integer):
        return 0
    elif np.issubdtype(dtype, np.floating):
        return np.nan
    elif np.issubdtype(dtype, np.bool_):
        return False
    elif np.issubdtype(dtype, np.object_):
        return None


def expand_array_with_nulls(array, nulls):
    if nulls.null_size() == 0:
        return array
    else:
        # inefficient to repeat expanding, should fix
        not_null_indices = nulls.not_null_indices()
        # fill value should be more explicitly handled
        values = np.full(nulls.size(), fill_value(array.dtype))
        values[not_null_indices] = array
        return values


class Index(object):
    def expand(self, values):
        raise NotImplementedError('Must be overridden in subclass')

    def combine_index(self, index):
        raise NotImplementedError('Must be overridden in subclass')

    def add_list(self, length):
        raise NotImplementedError('Must be overridden in subclass')

    def add_to_list(self):
        raise NotImplementedError('Must be overridden in subclass')

    def null_expand(self, nulls):
        raise NotImplementedError('Must be overridden in subclass')


class MixedIndex(Index):
    def expand(self, parent):
        pass

    def __init__(self):
        pass


class OrderedRangeIndex(Index):
    def expand(self, values):
        return values[np.repeat(np.arange(0, self.lengths.size), self.lengths.array().astype(np.int64))]

    def combine_index(self, sub_index):
        if isinstance(sub_index, OrderedRangeIndex):
            # verify that these indices matches
            own_lengths = self.lengths.array()
            if np.sum(own_lengths) != sub_index.lengths.size:
                raise ValueError('Malformed index')
            # need to double check that this performance is full numpy
            new_lengths = np.zeros(shape=[own_lengths.size], dtype=np.uint32)
            indices = np.repeat(np.arange(0, self.lengths.size), own_lengths.astype(np.int64))
            np.add.at(new_lengths, indices, sub_index.lengths.array())
            if new_lengths.size != own_lengths.size:
                raise ValueError('Index comptuation returned unexpected result')
            return OrderedRangeIndex(ArrayList(new_lengths))
        else:
            raise NotImplementedError('Mixed index expansion is not yet supported')

    def __init__(self, lengths):
        self.lengths = lengths

    def add_list(self, length):
        self.lengths.add(length)

    def add_to_list(self):
        self.lengths.add_to_item(self.lengths.size, 1)

    def null_expand(self, nulls):
        return OrderedRangeIndex(ArrayList(expand_array_with_nulls(self.lengths.array(), nulls)))

    @classmethod
    def create(cls):
        lengths = ArrayList.create(np.uint32)
        return OrderedRangeIndex(lengths)


name_separator = '_'


class FlattenStrategy(Enum):
    FLATTEN_EXPLICIT = 1
    FLATTEN_AVAILABLE = 2
    FLATTEN_ALL = 3


class NameStrategy(Enum):
    CONCATENATE_CONFLICTS = 1
    CONCATENATE_CONFLICTS_VERBOSE = 2
    CONCATENATE_ALWAYS = 3
    MULTI_INDEX = 4


class JoinType(Enum):
    INNER = 1
    OUTER = 2


class Indexed(object):
    def __init__(self, index):
        self.index = index


class ResolvedColumnName(object):
    def __init__(self, names):
        self.names = list(names)
        # move at least the final name into the resolved name
        if len(self.names) > 0:
            self.resolved_name = self.names.pop(0)
        else:
            self.resolved_name = None


def column_names(strategy, names):
    resolved_names = _column_names(strategy, names)
    # check that all names are unique
    if len(resolved_names) != len(set(resolved_names)):
        raise ValueError('Resolved names were not unique')
    return resolved_names


def _column_names(strategy, names):
    if strategy is NameStrategy.MULTI_INDEX:
        max_tuple_length = 0
        for name in names:
            max_tuple_length = max(max_tuple_length, len(name))
        return [(tuple(name[::-1]) + (('',) * (max_tuple_length - len(name)))) for name in names]
    elif strategy is NameStrategy.CONCATENATE_ALWAYS:
        return [name_separator.join(name[::-1]) for name in names]
    else:
        if strategy is NameStrategy.CONCATENATE_CONFLICTS:
            resolved_names = _resolve_names(names, False)
        elif strategy is NameStrategy.CONCATENATE_CONFLICTS_VERBOSE:
            resolved_names = _resolve_names(names, True)
        else:
            raise AssertionError('Unrecognized name strategy')
        return resolved_names


def _resolve_names(names, verbose_resolution):
    resolving_names = [ResolvedColumnName(name) for name in names]
    is_resolved = False
    while not is_resolved:
        name_map = dict()
        for name in resolving_names:
            if name.resolved_name not in name_map:
                name_map[name.resolved_name] = list()
            name_map[name.resolved_name].append(name)
        is_resolved = True
        for names in name_map.values():
            # if there are conflicts here
            if len(names) > 1:
                next_prefix = [name.names.pop(0) if len(name.names) > 0 else '' for name in names]
                # need to detect when there is nothing left to differentiate to avoid infinite looping on degeneracy
                all_names_empty = len([name for name in names if len(name.names) > 0]) == 0
                is_resolved = all_names_empty
                if len(set(next_prefix)) > 1 or verbose_resolution:
                    for prefix, name in zip(next_prefix, names):
                        if name.resolved_name == '':
                            name.resolved_name = prefix
                        elif prefix != '':
                            name.resolved_name = prefix + name_separator + name.resolved_name
    return [name.resolved_name for name in resolving_names]


class PartialFlatten(Indexed):
    def __init__(self, columns, index):
        super(PartialFlatten, self).__init__(index)
        self.columns = columns

    def is_empty(self):
        return not self.columns

    def expand_null(self, nulls):
        if self.index is None:
            new_columns = [(name, expand_array_with_nulls(values, nulls)) for name, values in self.columns]
            return PartialFlatten(new_columns, None)
        else:
            return PartialFlatten(self.columns, self.index.null_expand(nulls))

    def column_names(self, strategy):
        names = [name for name, value in self.columns]
        values = [value for name, value in self.columns]
        return dict(zip(column_names(strategy, names), values))


class TextTree(object):
    def __init__(self, text, parent_suffix, subnodes):
        self.text = text
        self.parent_suffix = parent_suffix
        self.subnodes = subnodes

    def render(self, indent=0, indent_step=4, prefix='- '):
        return self._render(indent, indent_step, prefix)

    def _render(self, indent, indent_step, prefix):
        suffix = self._render_suffix()
        if self.text is not None:
            new_indent = indent + indent_step
            own_render = [(' ' * indent) + prefix + self.text + ' ' + suffix]
        else:
            new_indent = indent
            own_render = []
        sub_rendered = [node._render(new_indent, indent_step, prefix) for node in self.subnodes if node is not None]
        nodes = own_render + sub_rendered
        filtered = [node for node in nodes if node != '']
        return '\n'.join(filtered)

    def _render_suffix(self, stop=False):
        if self.text is not None and stop:
            return ''
        else:
            suffixes = [node._render_suffix(True) for node in self.subnodes if node is not None]
            suffix = self.parent_suffix
            if suffix is None:
                suffix = ''
            return suffix + ''.join(suffixes)


class Node(object):
    def flatten(self, flatten_strategy=FlattenStrategy.FLATTEN_ALL,
                name_strategy=NameStrategy.CONCATENATE_CONFLICTS,
                join=JoinType.INNER,
                include=None,
                exclude=None):
        if not include:
            include = set()
        if not exclude:
            exclude = set()
        inclusions = {self._parse_clusion(inc) for inc in include}
        exclusions = {self._parse_clusion(exc) for exc in exclude}
        partial = self._flatten(flatten_strategy, join, inclusions, exclusions, not include)
        resolved = partial.column_names(name_strategy)
        return pd.DataFrame(resolved)

    def _flatten_fields_without_lists(self, fields):
        # this is messy: the expand_null handles creating the new index (which we know will be None), but we
        # throw those indices away because we merge the resulting columns
        columns = [(name, values) for field in fields for name, values in
                   field.expand_null(self._nulls).columns]
        return PartialFlatten(columns, None)

    def _flatten(self, strategy,
                 join,
                 include,
                 exclude,
                 implicit_include):
        if join is JoinType.OUTER:
            raise NotImplementedError('Outer join is not yet implemented')
        # should change this to make a first pass without actual flattening (so that we don't waste computation effort)
        # should compound nulls and indices downwards instead of upwards (to save on repeated work)

        explicit_include = id(self) in include
        explicit_exclude = id(self) in exclude
        if explicit_include and explicit_exclude:
            raise AssertionError('Node is both explicitly included and excluded')
        included = explicit_include or (implicit_include and not explicit_exclude)

        if isinstance(self, RecordNode):
            fields = [field._flatten(strategy, join, include, exclude, included)
                      for field in self._children.values()]
            fields = [field for field in fields if field is not None]
            sub_lists = [field for field in fields if field.index is not None]
            num_lists = len(sub_lists)
            if num_lists > 1:
                if strategy is FlattenStrategy.FLATTEN_AVAILABLE:
                    fields_not_lists = [field for field in fields if field.index is None]
                    return self._flatten_fields_without_lists(fields_not_lists)
                else:
                    # should add something that displays the names of the conflicting lists
                    raise ValueError('Attempted to flatten conflicting lists')
            elif num_lists > 0:
                sub_list = sub_lists[0]
                columns = list()
                index = sub_list.index
                for field in fields:
                    if field.index is None:
                        for name, values in field.columns:
                            columns.append((name, index.expand(values)))
                    else:
                        for name, values in field.columns:
                            columns.append((name, values))
                expanded_index = index.null_expand(self._nulls)
                return PartialFlatten(columns, expanded_index)
            else:
                return self._flatten_fields_without_lists(fields)
        elif isinstance(self, RecordField):
            below = self._value._flatten(strategy, join, include, exclude, included)
            columns = [(name + [self._name], values) for name, values in below.columns]
            return PartialFlatten(columns, below.index)
        elif isinstance(self, ListNode):
            if (strategy is FlattenStrategy.FLATTEN_ALL or strategy is FlattenStrategy.FLATTEN_AVAILABLE or
                    (strategy is FlattenStrategy.FLATTEN_EXPLICIT and explicit_include)):
                sub_flat = self._child._flatten(strategy, join, include, exclude, included)
                if sub_flat.is_empty():
                    return PartialFlatten([], None)
                if sub_flat.index is None:
                    own_index = self._index.null_expand(self._nulls)
                    return PartialFlatten(sub_flat.columns, own_index)
                else:
                    own_index = self._index.null_expand(self._nulls)
                    combined_index = own_index.combine_index(sub_flat.index)
                    return PartialFlatten(sub_flat.columns, combined_index)
            else:
                return PartialFlatten([], None)
        elif isinstance(self, IncompleteNode):
            return PartialFlatten([], None)
        elif isinstance(self, PrimitiveNode):
            if included:
                return PartialFlatten([(list(), expand_array_with_nulls(self._values.array(), self._nulls))], None)
            else:
                return PartialFlatten([], None)

    def _parse_clusion(self, clusion):
        if isinstance(clusion, str):
            splits = clusion.split('.', 1)
            subnode = self._get_subnode(splits[0])
            if len(splits) > 1:
                return subnode._parse_clusion(splits[1])
            else:
                return id(subnode)
        elif isinstance(clusion, Node):
            return id(clusion)

    def _get_subnode(self, name):
        raise NotImplementedError('Must be implemented in subclass')

    def info(self, depth=3, display_mem=False):
        return self._info(depth, display_mem).render()

    def _info(self, depth, display_mem):
        raise NotImplementedError('Must be implemented in subclass')

    def __str__(self):
        return self.info()

    def __getattr__(self, item):
        return self._get_subnode(item)


class Nullable(object):
    def __init__(self, nulls):
        self._nulls = nulls

    def _add_null(self):
        self._nulls.add_null()

    def _add_not_null(self):
        self._nulls.add_not_null()

    def _not_null_indices(self):
        return self._nulls.not_null_indices()

    def _size(self):
        return self._nulls.size()

    def _null_size(self):
        return self._nulls.null_size()


class RecordField(Node):
    def __init__(self, name, value):
        self._name = name
        self._value = value

    def _get_subnode(self, name):
        return self._value._get_subnode(name)

    def _info(self, depth, display_mem):
        return TextTree(self._name, None, [self._value._info(depth, display_mem)])


class RecordNode(Node, Nullable):
    def __init__(self, children, nulls):
        # should check if any of these attribute names conflict

        Nullable.__init__(self, nulls)
        self._children = {c._name: c for c in children}

    def _add_child(self, name):
        child = RecordField(name, IncompleteNode.create())
        num_not_null = self._nulls.size() - self._nulls.null_size()
        for i in range(0, num_not_null):
            child._value._add_null()
        self._children[name] = child

    def _get_subnode(self, name):
        # this is not a node but a record field...
        return self._children[name]

    def _contains(self, name):
        return name in self._children

    def _info(self, depth, display_mem):
        child_info = [child._info(depth - 1, display_mem) for child in self._children.values()]
        return TextTree(None, None, child_info)


class ListNode(Node, Nullable):
    def __init__(self, child, index, nulls):
        Nullable.__init__(self, nulls)
        self._child = child
        self._index = index

    def _get_subnode(self, name):
        return self._child._get_subnode(name)

    def _add_list(self, values):
        self._index.add_list(len(values))

    def _add_to_list(self, values):
        self._index.add_list(len(values))

    def _info(self, depth, display_mem):
        return TextTree(None, '[]', [self._child._info(depth, display_mem)])


class PrimitiveNode(Node, Nullable):
    def __init__(self, values, nulls):
        Nullable.__init__(self, nulls)
        self._values = values

    def __getitem__(self, item):
        return self._values[item]

    def _add(self, value):
        self._add_not_null()
        self._values.add(value)

    def _get_subnode(self, name):
        raise AssertionError('Primitive nodes do not have sub-nodes')

    # should change this so it can append some extra info to the above node (a list indicator)
    def _info(self, depth, display_mem):
        return TextTree(None, str(self._values.values.dtype), [])


class IncompleteNode(Node, Nullable):

    def __init__(self, nulls):
        Nullable.__init__(self, nulls)

    def _get_subnode(self, name):
        raise AssertionError('Incomplete nodes do not have sub-nodes')

    def _info(self, depth, display_mem):
        return TextTree(None, None, [])

    @classmethod
    def create(cls):
        return IncompleteNode(IndexNullIndicator.create())


class Converter(object):
    def __init__(self, type_resolver, field_names,
                 field_extractor, list_extractor):
        self.type_resolver = type_resolver
        self.field_names = field_names
        self.field_extractor = field_extractor
        self.list_extractor = list_extractor


# should we move into node class to avoid protected access warnings?
def build(obj, node, converter):
    obj_type = converter.type_resolver(obj)
    if obj_type is IncompleteNode:
        node._add_null()
        return node
    elif obj_type is RecordNode:
        names = converter.field_names(obj)
        if isinstance(node, IncompleteNode):
            node = RecordNode(list(), node._nulls)
        for name in names:
            if not node._contains(name):
                node._add_child(name)
            field = converter.field_extractor(obj, name)
            record_field = node._children[name]
            sub_node = record_field._value
            node._children[name] = RecordField(name, build(field, sub_node, converter))
        snames = set(names)
        for name in node._children.keys():
            if name not in snames:
                node._children[name]._value._add_null()
        node._add_not_null()
        return node
    elif obj_type is ListNode:
        values = converter.list_extractor(obj)
        if isinstance(node, IncompleteNode):
            node = ListNode(IncompleteNode.create(), OrderedRangeIndex.create(), node._nulls)
        node._add_not_null()
        node._add_list(values)
        for value in values:
            node._child = build(value, node._child, converter)
        return node
    elif obj_type is PrimitiveNode:
        if isinstance(node, IncompleteNode):
            node = PrimitiveNode(ArrayList.create(type(obj)), node._nulls)
        node._add(obj)
        return node
    else:
        raise AssertionError('Unknown node type')
    pass
