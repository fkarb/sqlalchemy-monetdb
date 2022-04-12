
venv/:
	python3 -m venv venv

venv/bin/tox: venv/
	venv/bin/pip install tox

tox: venv/bin/tox
	venv/bin/tox

test: tox
