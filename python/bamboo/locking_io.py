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

import io
import threading


# this class allows two threads to safely simultaneously write and read from an IO stream
class LockingIO(io.RawIOBase):
    def __init__(self):
        self.is_reading = False
        self.condition = threading.Condition()
        self.read_buffer = None
        self.bytes_read = None
        self.is_eof = False

    def readinto(self, b):
        self.condition.acquire()
        self.read_buffer = b
        self.is_reading = True
        self.condition.notify()
        self.condition.release()

        self.condition.acquire()
        if self.is_reading:
            self.condition.wait()
        if self.is_eof:
            self.bytes_read = 0
        self.condition.release()

        return self.bytes_read

    def set_eof(self):
        self.condition.acquire()
        if not self.is_reading:
            self.condition.wait()
        self.is_eof = True
        self.is_reading = False
        self.condition.notify()
        self.condition.release()
        
    def write(self, b):
        self.condition.acquire()
        if not self.is_reading:
            self.condition.wait()
        n_to_copy = min(b.nbytes, self.read_buffer.nbytes)
        self.read_buffer[0:n_to_copy] = b[0:n_to_copy]
        self.is_reading = False
        self.bytes_read = n_to_copy
        self.condition.notify()
        self.condition.release()
        return self.bytes_read


class Writer(threading.Thread):
    def __init__(self, buf, func):
        threading.Thread.__init__(self)
        self.buf = buf
        self.func = func

    def run(self):
        try:
            self.func(self.buf)
            self.buf.set_eof()
        except Exception as e:
            raise Exception('Error in writer thread') from e
