
venv/:
	python3 -m venv venv

venv/bin/pytest: venv/
	venv/bin/pip install pytest

pytest: venv/bin/pytest
	venv/bin/pytest

venv/bin/tox: venv/
	venv/bin/pip install tox

tox: venv/bin/tox
	venv/bin/tox

setup: venv/
	venv/bin/pip install -e .

test: tox

clean:
	rm -rf .tox/ build/
