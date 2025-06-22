#!/bin/bash

docker build -t starwitorg/detection-aggregator:$(poetry version --short) .