SHELL=/bin/bash

VENV=.venv

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

venv:
	python3 -m venv $(VENV)
	$(MAKE) requirements

requirements:
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/pip install --upgrade -r requirements.txt \

dev-release: .venv
	unset CONDA_PREFIX && \
	cargo fmt && \
	source .venv/bin/activate && maturin develop --release -m Cargo.toml