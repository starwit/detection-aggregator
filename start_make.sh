#!/bin/bash

export PATH=/root/.local/bin/:$PATH

# Purpose of this script is to enforce a bash environment when calling make
export PATH=/root/.local/bin/:$PATH

make test
make build-deb

