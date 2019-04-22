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

import bamboo_cpp_bind as bamboo_cpp

import six


class Clusion(object):
    def __init__(self, explicit, leaves):
        self.explicit = explicit
        self.leaves = leaves

    def __eq__(self, other):
        return isinstance(other, Clusion) and (self.explicit == other.explicit) and (self.leaves == other.leaves)


def recursive_merge(clusions):
    if clusions:
        leaves = dict()
        explicit = False
        dicts = list()
        for clusion in clusions:
            explicit |= clusion.explicit
            dicts.append(clusion.leaves)
        for field in set().union(*dicts):
            field_dicts = list()
            for d in dicts:
                if field in d:
                    field_value = d[field]
                    if field_value:
                        field_dicts.append(field_value)
            merged = recursive_merge(field_dicts)
            leaves[field] = merged
        return Clusion(explicit, leaves)
    else:
        return Clusion(True, {})


def split_str_clusion(clusion):
    splits = clusion.split('.', 1)
    if len(splits) > 1:
        child_dict, final_dict = split_str_clusion(splits[1])
        return Clusion(False, {splits[0]: child_dict}), final_dict
    else:
        field_name = splits[0]
        if field_name == '':
            raise Exception('Empty field names are not allowed')
        d = {field_name: Clusion(True, dict())}
        return Clusion(False, d), d


def parse_clusions(clusions):
    if clusions is None:
        return Clusion(False, {})
    if isinstance(clusions, str):
        return split_str_clusion(clusions)[0]
    elif isinstance(clusions, list):
        return recursive_merge([parse_clusions(clusion) for clusion in clusions])
    elif isinstance(clusions, dict):
        if clusions:
            roots = list()
            for k in clusions:
                if not isinstance(k, str):
                    raise Exception('Clusion dict keys must be strings')
                root, leaf = split_str_clusion(k)
                if len(leaf.keys()) != 1:
                    raise AssertionError('Multi clusion strings should only have one child')
                leaf_key = six.next(six.iterkeys(leaf))
                leaf[leaf_key] = parse_clusions(clusions[k])
                roots.append(root)
            return recursive_merge(roots)
        else:
            return Clusion(True, {})


def convert_clusions(include, exclude):
    inclusions = parse_clusions(include)
    exclusions = parse_clusions(exclude)
    return recurse_clusions(inclusions, exclusions)


def recurse_clusions(included, excluded):
    fields = dict()
    inclusions = included.leaves
    exclusions = excluded.leaves
    for field in set().union(included.leaves.keys(), excluded.leaves.keys()):
        if field in inclusions:
            include_field = inclusions[field]
        else:
            include_field = Clusion(False, {})
        if field in exclusions:
            exclude_field = exclusions[field]
        else:
            exclude_field = Clusion(False, {})
        fields[field] = recurse_clusions(include_field, exclude_field)

    if included.explicit and excluded.explicit:
        raise RuntimeError('Cannot both include and exclude a field')

    return bamboo_cpp.ColumnFilter(included.explicit, excluded.explicit, fields)
