#!/bin/bash

export PATH=/root/.local/bin/:$PATH
cd /code

# Purpose of this script is to enforce a bash environment when calling make
export PACKAGE_NAME=objectdetector
export PATH=/root/.local/bin/:$PATH
make test
make build-deb

