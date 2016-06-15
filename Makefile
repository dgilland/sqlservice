
##
# TravisCI
##

.PHONY: travisci-install
travisci-install:
	pip install -r requirements-dev.txt --use-mirrors

.PHONY: travisci-test
travisci-test:
	invoke test
