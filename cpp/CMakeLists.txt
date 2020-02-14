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

list(APPEND CMAKE_MODULE_PATH thirdparty/pbd/thirdparty/proto-cmake/cmake)

find_package(Protobuf REQUIRED)
find_package(Boost 1.38 REQUIRED)

add_subdirectory(thirdparty/pybind11)

FILE(GLOB bamboo_cpp_src "src/*.cpp")
add_library(bamboo_cpp ${bamboo_cpp_src})

if (NOT EXISTS ${CMAKE_BUILD_TYPE})
    SET(CMAKE_BUILD_TYPE Release)
endif ()

if ($ENV{BAMBOO_USE_INSTALL})
    set(USE_INSTALL ON)
endif ()

if (USE_INSTALL)
    set(USE_INSTALL ON)

    include(ExternalProject)

    set(AVRO_LIB_INSTALL_DIR "/tmp/bamboo_build/avro_lib")
    set(ARROW_LIB_INSTALL_DIR "/tmp/bamboo_build/arrow_lib")

    if(NOT EXISTS ${AVRO_LIB_INSTALL_DIR})
        ExternalProject_Add(avro_lib
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/avro/lang/c++
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${AVRO_LIB_INSTALL_DIR}
                -DCMAKE_POSITION_INDEPENDENT_CODE=ON
                -DCMAKE_CXX_FLAGS=-Wno-deprecated-declarations -Wno-int-in-bool-context
                -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                -DCMAKE_CXX_STANDARD=14
            )
    else ()
        add_custom_target(avro_lib)
    endif ()

    if(NOT EXISTS ${ARROW_LIB_INSTALL_DIR})
        ExternalProject_Add(arrow_lib
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/arrow/cpp
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ARROW_LIB_INSTALL_DIR}
                -DARROW_IPC=ON
                -DARROW_TEST_LINKAGE=shared
                -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            )
    else ()
        add_custom_target(arrow_lib)
    endif ()

    link_directories(${AVRO_LIB_INSTALL_DIR}/lib)
    link_directories(${ARROW_LIB_INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR})

    add_dependencies(bamboo_cpp avro_lib)
    add_dependencies(bamboo_cpp arrow_lib)

    target_link_libraries(bamboo_cpp PUBLIC arrow)
    target_include_directories(bamboo_cpp PUBLIC
        # including this is a hack -- better to use CMAKE_INSTALL_INCLUDEDIR properly, a la https://github.com/apache/arrow/pull/4387
        ${ARROW_LIB_INSTALL_DIR}/include
        ${CMAKE_INSTALL_INCLUDEDIR}
    )

    if (APPLE)
        target_link_libraries(bamboo_cpp PUBLIC avrocpp_s)
    else ()
        target_link_libraries(bamboo_cpp PUBLIC avrocpp)
    endif ()

else ()
    set(ARROW_IPC ON)
    set(ARROW_TEST_LINKAGE static)
    set(ARROW_BUILD_STATIC ON)

    # should suppress deprecation warnings in avro here as well
    add_subdirectory(thirdparty/arrow/cpp)
    add_subdirectory(thirdparty/avro/lang/c++)

    target_link_libraries(bamboo_cpp
        PUBLIC
        avrocpp_s
        arrow_static
    )
endif ()

target_include_directories(bamboo_cpp
    PUBLIC src/include
    thirdparty/json/single_include
    thirdparty/avro/lang/c++/api
    thirdparty/arrow/cpp/src
    thirdparty/pbd/cpp/src/include
    ${Boost_INCLUDE_DIR})

target_link_libraries(bamboo_cpp
    PUBLIC
    protobuf::libprotobuf
    z
)

if (APPLE)
    target_link_libraries(bamboo_cpp PUBLIC boost_iostreams.a)
endif ()

pybind11_add_module(bamboo_cpp_bind src/bind/bind.cpp)
target_include_directories(bamboo_cpp_bind
    PUBLIC src/include)
target_link_libraries(bamboo_cpp_bind
    PUBLIC bamboo_cpp)
