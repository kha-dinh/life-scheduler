init:
	pip install -r requirements.txt

test:
	nosetests tests

.PHONY: test init