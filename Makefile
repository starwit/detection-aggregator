.PHONY: check-settings install test

export PACKAGE_NAME=objectdetector

default: install

install: check-settings
	poetry install -vvv

check-settings:
	./check_settings.sh

test: install
	poetry run pytest
