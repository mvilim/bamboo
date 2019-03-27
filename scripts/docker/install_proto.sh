mkdir protobuf
cd protobuf
curl -L -O https://github.com/protocolbuffers/protobuf/releases/download/v3.7.0rc1/protobuf-cpp-3.7.0.tar.gz
tar -xf protobuf-cpp-3.7.0.tar.gz
cd protobuf-3.7.0
./configure
make install
