
##
# TravisCI
##

.PHONY: travisci-install
travisci-install:
	pip install -r dev-requirements.txt

.PHONY: travisci-test
travisci-test:
	invoke test
