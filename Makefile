.PHONY: install test

export PACKAGE_NAME=objectdetector

install: 
	poetry install

check-settings: install
	./check_settings.sh

test: check-settings
	poetry run pytest
