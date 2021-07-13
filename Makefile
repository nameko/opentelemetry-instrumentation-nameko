static:
	pre-commit run --all-files

test:
	py.test tests -v

coverage:
	coverage run -m pytest tests -v
	coverage report