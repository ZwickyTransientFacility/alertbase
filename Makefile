virtualenv:
	python -m venv virtualenv
	virtualenv/bin/pip install "pip>=20.3" --upgrade

VENV = virtualenv/bin
$(VENV): virtualenv
$(VENV)/pip: $(VENV)

.git/hooks/pre-commit:
	ln -sf ../../devconfig/pre-commit.sh .git/hooks/pre-commit


# Development tools
.PHONY: dev-setup
dev-setup: .git/hooks/pre-commit $(VENV)/flake8 $(VENV)/black $(VENV)/mypy
deps: $(VENV)
	$(VENV)/pip install -e '.[dev]'

$(VENV)/flake8 $(VENV)/black $(VENV)/mypy $(VENV)/pytest &: $(VENV)/pip
	$(VENV)/pip install -e '.[dev]'

.PHONY: lint check-format format typecheck precommit test
lint: $(VENV)/flake8
	$(VENV)/flake8 ./src
	$(VENV)/flake8 ./test

check-format: $(VENV)/black
	$(VENV)/black \
		--target-version py38 \
		--check \
		./src/alertbase
	$(VENV)/black \
		--target-version py38 \
		--check \
		./test

format: $(VENV)/black
	$(VENV)/black \
		--target-version py38 \
		./src/alertbase
	$(VENV)/black \
		--target-version py38 \
		./test

typecheck: $(VENV)/mypy
	$(VENV)/mypy --strict ./src

test: $(VENV)/pytest
	$(VENV)/pytest .

precommit: check-format typecheck lint test

.PHONY: docs
docs:
	$(MAKE) -C docs html

.PHONY: clean
clean:
	rm -rf virtualenv
