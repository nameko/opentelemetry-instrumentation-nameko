static:
	pre-commit run --all-files

test:
	nameko test -v

coverage:
	coverage run -m nameko test -v
	coverage report