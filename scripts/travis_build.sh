set -e -x

CIBUILDWHEEL_VERSION=0.10.1

if [ "$TRAVIS_OS_NAME" = "linux" ]; then
    pip install cibuildwheel==$CIBUILDWHEEL_VERSION
elif [ "$TRAVIS_OS_NAME" = "osx" ]; then
    HOMEBREW_NO_AUTO_UPDATE=1 brew install protobuf
    sudo ln -s /usr/local/opt/openssl/lib/libcrypto.1.1.0.dylib /usr/local/opt/openssl/lib/libcrypto.1.0.0.dylib
    sudo pip2 install cibuildwheel==$CIBUILDWHEEL_VERSION
else
    echo Unrecognized OS
    exit -1
fi

cibuildwheel --output-dir dist

# only create the source distribution from linux, so that we don't try to upload it twice
if [ "$TRAVIS_OS_NAME" = "linux" ]; then
    python setup.py sdist --dist-dir dist
fi
