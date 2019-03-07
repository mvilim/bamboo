set -e -x

pip install --user cibuildwheel==0.10.1
cibuildwheel --output-dir dist

# only create the source distribution from linux, so that we don't try to upload it twice
if [ "$TRAVIS_OS_NAME" = "linux" ]
then
    python setup.py sdist --dist-dir dist
fi
