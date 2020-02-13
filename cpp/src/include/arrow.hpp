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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>

namespace bamboo {
namespace arrow {

using namespace ::arrow;
using namespace ::arrow::io;

class ArrowInputStream : public virtual ::arrow::io::InputStream {
   private:
    std::istream& stream;
    int64_t pos = 0;

   public:
    virtual ~ArrowInputStream() = default;

    ArrowInputStream(std::istream& stream) : stream(stream) {}

    virtual Status Close() final override {
        return Status::OK();
    };

    virtual bool closed() const final override {
        return false;
    };

    virtual Result<int64_t> Tell() const final override {
        return Result<int64_t>(pos);
    }

    // virtual Status Read(int64_t nbytes, int64_t* bytes_read, void* out) final override {
    //     char* out_char = (char*)out;
    //     stream.read(out_char, nbytes);
    //     *bytes_read = stream.gcount();
    //     pos += *bytes_read;
    //     return Status::OK();
    // }

    virtual arrow::Result<int64_t> Read(int64_t nbytes, void* out) final override
    {
        char* out_char = (char*)out;
        stream.read(out_char, nbytes);
        int64_t bytes_read = stream.gcount();
        pos += bytes_read;
        return Result<int64_t>(bytes_read);
    }

    // virtual Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) final override {
    //     RETURN_NOT_OK(AllocateBuffer(nbytes, out));
    //     stream.read((char*)((*out)->mutable_data()), nbytes);
    //     int64_t bytes_read = stream.gcount();
    //     pos += bytes_read;
    //     return Status::OK();
    // }

    virtual Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) final override {
        // should check status and forward to result
        std::shared_ptr<Buffer> out;
        AllocateBuffer(nbytes, &out);
        stream.read((char*)(out->mutable_data()), nbytes);
        int64_t bytes_read = stream.gcount();
        pos += bytes_read;
        return Result<std::shared_ptr<Buffer>>(out);
    }
};

unique_ptr<Node> convert(std::istream& is);

}  // namespace arrow
}  // namespace bamboo
