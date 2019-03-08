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

cmake_minimum_required(VERSION 3.0)

project(bamboo VERSION ${VERSION_INFO})

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# this must be set here so that it affects avro, allowing us 
# to link its static archive into our runtime loaded module
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# included for debug compatibility with avro library
# should change this to only enable if in debug build
if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_GLIBCXX_DEBUG")
endif ()

# link statically to arrow because of ARROW-4242
set(ARROW_BUILD_STATIC ON)
set(ARROW_IPC ON)

add_subdirectory(thirdparty/arrow/cpp)
add_subdirectory(thirdparty/pybind11)
add_subdirectory(thirdparty/avro/lang/c++)

find_package(Boost 1.38 REQUIRED)

FILE(GLOB bamboo_cpp_src "src/*.cpp")
add_library(bamboo_cpp ${bamboo_cpp_src})
target_include_directories(bamboo_cpp
    PUBLIC src/include thirdparty/json/single_include thirdparty/avro/lang/c++/api thirdparty/arrow/cpp/src ${Boost_INCLUDE_DIR})

target_link_libraries(bamboo_cpp
    PUBLIC avrocpp_s arrow_static z)

pybind11_add_module(bamboo_cpp_bind src/bind/bind.cpp)
target_include_directories(bamboo_cpp_bind
    PUBLIC src/include)
target_link_libraries(bamboo_cpp_bind
    PUBLIC bamboo_cpp)
