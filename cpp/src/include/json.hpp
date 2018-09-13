// Copyright (c) 2019 Michael Vilim
// 
// This file is part of the bamboo library. It is currently hosted at
// https://github.com/mvilim/bamboo
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//    http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <columns.hpp>
#include <nlohmann/json.hpp>

namespace bamboo {
namespace json {

namespace json = nlohmann;

typedef KeyValueIterator<const string, json::json&> FieldIteratorType;
typedef ValueIterator<json::json&> ListIteratorType;

class FieldIterator final : public FieldIteratorType {
    json::json::iterator it;
    json::json::iterator end;
    bool has_started = false;

   public:
    FieldIterator(json::json& datum) : it(datum.begin()), end(datum.end()){};

    virtual ~FieldIterator() final override = default;

    virtual bool next() final override {
        if (!has_started) {
            has_started = true;
        } else {
            it++;
        }
        return it != end;
    }

    virtual const string key() final override {
        return it.key();
    }

    virtual json::json& value() final override {
        return it.value();
    }
};

class ListIterator final : public ListIteratorType {
    json::json::iterator it;
    json::json::iterator end;
    bool has_started = false;

   public:
    ListIterator(json::json& datum) : it(datum.begin()), end(datum.end()){};

    virtual ~ListIterator() final override = default;

    virtual bool next() override {
        if (!has_started) {
            has_started = true;
        } else {
            it++;
        }
        return it != end;
    };

    virtual json::json& value() override {
        return *it;
    }
};

class JsonConverter final : public virtual Converter<json::json&, FieldIterator, ListIterator> {
   public:
    virtual ObjType type(json::json& datum) final override;

    virtual FieldIterator fields(json::json& datum) final override;

    virtual ListIterator get_list(json::json& datum) final override;

    virtual void add_primitive(PrimitiveNode& v, json::json& datum) final override;

    virtual ~JsonConverter() final override = default;
};

unique_ptr<Node> convert(std::istream& is);

}  // namespace json
}  // namespace bamboo
