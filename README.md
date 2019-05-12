<!--
Copyright (c) 2019 Michael Vilim

This file is part of the bamboo library. It is currently hosted at
https://github.com/mvilim/bamboo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

## bamboo

[![PyPI Release](https://img.shields.io/pypi/v/bamboo-nested.svg)](https://pypi.org/project/bamboo-nested/)
[![Build Status](https://travis-ci.org/mvilim/bamboo.svg?branch=master)](https://travis-ci.org/mvilim/bamboo)

bamboo is a library for feeding nested data formats into pandas. The space of data representable in nested formats is larger than the space covered by pandas. pandas supports only data representable in a flat table (though things like multi-indexs allows certain types of tree formats to be efficiently projected into a table). Data which supports arbitrary nesting is not in general convertible to a pandas dataframe. In particular, data which contains multiple repetition structures (e.g. JSON arrays) that are not nested within each other will not be flattenable into a table.

The current data formats supported are:
* JSON
* Apache Avro
* Apache Arrow
* Profobuf (via [PBD](https://github.com/mvilim/pbd))

bamboo works by projecting a flattenable portion (a subset of the nested columns) of the data into a pandas dataframe. By projecting various combinations of columns, one can make use of all the relationships implied by the nested structure of the data.

### Installation

To install from PyPI:

```
pip install bamboo-nested
```

### Example

A minimal example of flattening a JSON string:

```
from bamboo import from_json

obj = [{'a': None, 'b': [1, 2], 'c': [5, 6]}, {'a': -1.0, 'b': [3, 4], 'c': [7, 8]}]
node = from_json(json.dumps(obj))
    > - a float64
    > - b []uint64
    > - c []uint64
```

Flattening just the values of column `a`:

```
df_a = node.flatten(include=['a'])
    >      a
    > 0  NaN
    > 1 -1.0
```

Flattening columns `a` and `b` (note that column `a` is repeated to match the corresponding elements of column `b`):

```
df_ab = node.flatten(include=['a', 'b'])
    >      a  b
    > 0  NaN  1
    > 1  NaN  2
    > 2 -1.0  3
    > 3 -1.0  4
```

Trying to flatten two repetition lists at the same level will lead to an error (as this structure is unflattenable without taking a Cartesian product):

```
df_bc = node.flatten(include=['b', 'c'])
    > ValueError: Attempted to flatten conflicting lists
```

### Building

To build this project:

Building from source requires cmake (`pip install cmake`) and Boost.

```
python setup.py
```

### Unit tests

To run the unit tests:

```
python setup.py test
```

or use nose:

```
nosetests python/bamboo/tests
```

### Licensing

This project is licensed under the [Apache 2.0 License](https://github.com/mvilim/bamboo/blob/master/LICENSE). It uses the pybind11, Arrow, Avro, nlohmann JSON, and PBD projects. The licenses can be found in those [projects' directories](https://github.com/mvilim/bamboo/blob/master/cpp/thirdparty).
