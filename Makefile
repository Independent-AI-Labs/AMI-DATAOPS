# Makefile for AMI-DATAOPS
# Uses uv workspace to get code quality tools from ami-agents
#
# This project is a uv workspace member of AMI-AGENTS. It must be cloned
# inside the AMI-AGENTS repo at projects/AMI-DATAOPS.

# =============================================================================
# Configuration
# =============================================================================

AGENTS_ROOT := $(abspath ../..)
AGENTS_REPO := git@hf.co:ami-ailabs/AMI-AGENTS
AGENTS_PYPROJECT := $(AGENTS_ROOT)/pyproject.toml
AGENTS_RUFF := $(AGENTS_ROOT)/res/config/ruff.toml
AGENTS_MYPY := $(AGENTS_ROOT)/res/config/mypy.toml
AGENTS_BOOT := $(AGENTS_ROOT)/.boot-linux

# NEVER fallback to system uv - MUST use workspace-bootstrapped uv
# Only system git is allowed (for initial AMI-AGENTS clone)
UV := $(AGENTS_BOOT)/bin/uv

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help: ## Show this help
	@echo "AMI-DATAOPS Makefile"
	@echo ""
	@echo "Installation targets:"
	@echo "  install              Full install: Python + hooks"
	@echo "  install-ci           CI install: Python only, no hooks"
	@echo "  install-package      Install Python dependencies only"
	@echo "  install-hooks        Install pre-commit hooks"
	@echo ""
	@echo "Code quality targets:"
	@echo "  lint                 Run ruff linter"
	@echo "  lint-fix             Run ruff with auto-fix"
	@echo "  type-check           Run mypy"
	@echo "  test                 Run pytest"
	@echo "  test-unit            Run unit tests only"
	@echo "  test-integration     Run integration tests only"
	@echo "  test-cov             Run tests with coverage"
	@echo "  check                Run all checks"
	@echo "  pre-commit           Run pre-commit on all files"
	@echo ""
	@echo "Other targets:"
	@echo "  clean                Remove build artifacts"
	@echo "  clean-venv           Remove virtual environment"

# =============================================================================
# Preflight: Verify AMI-AGENTS workspace is present
# =============================================================================

# Set AUTO_INSTALL=1 to skip the prompt and clone automatically
AUTO_INSTALL ?= 0

.PHONY: preflight
preflight:
	@if [ ! -f "$(AGENTS_PYPROJECT)" ]; then \
		echo ""; \
		echo "ERROR: AMI-AGENTS workspace not found at $(AGENTS_ROOT)"; \
		echo ""; \
		echo "AMI-DATAOPS is a uv workspace member of AMI-AGENTS."; \
		echo "It must be cloned inside the AMI-AGENTS repo at projects/AMI-DATAOPS."; \
		echo ""; \
		if [ "$(AUTO_INSTALL)" = "1" ]; then \
			answer="y"; \
		else \
			printf "Clone AMI-AGENTS now via SSH? [y/N] "; \
			read answer; \
		fi; \
		if [ "$$answer" = "y" ] || [ "$$answer" = "Y" ]; then \
			CLONE_DIR="$$(dirname "$$(pwd)")/AMI-AGENTS"; \
			echo "Cloning AMI-AGENTS to $$CLONE_DIR ..."; \
			git clone $(AGENTS_REPO) "$$CLONE_DIR" && \
			mkdir -p "$$CLONE_DIR/projects" && \
			rm -rf "$$CLONE_DIR/projects/AMI-DATAOPS" && \
			cp -a "$$(pwd)" "$$CLONE_DIR/projects/AMI-DATAOPS" && \
			echo "" && \
			echo "Bootstrapping workspace tools (uv, python, git)..." && \
			$(MAKE) -C "$$CLONE_DIR" bootstrap-core && \
			echo "" && \
			echo "Workspace bootstrapped at $$CLONE_DIR" && \
			echo "AMI-DATAOPS copied into workspace." && \
			echo "" && \
			echo "Continue from the workspace copy:" && \
			echo "  cd $$CLONE_DIR/projects/AMI-DATAOPS" && \
			echo "  make install" && \
			echo ""; \
		else \
			echo ""; \
			echo "To set up manually:"; \
			echo "  git clone $(AGENTS_REPO)"; \
			echo "  cp -a . AMI-AGENTS/projects/AMI-DATAOPS"; \
			echo "  cd AMI-AGENTS/projects/AMI-DATAOPS"; \
			echo "  make install"; \
			echo ""; \
		fi; \
		exit 1; \
	fi
	@if [ ! -f "$(AGENTS_RUFF)" ]; then \
		echo "ERROR: Missing $(AGENTS_RUFF)"; \
		echo "AMI-AGENTS repo appears incomplete. Pull latest and retry."; \
		exit 1; \
	fi
	@if [ ! -f "$(AGENTS_MYPY)" ]; then \
		echo "ERROR: Missing $(AGENTS_MYPY)"; \
		echo "AMI-AGENTS repo appears incomplete. Pull latest and retry."; \
		exit 1; \
	fi
	@if [ ! -x "$(UV)" ]; then \
		echo ""; \
		echo "ERROR: Workspace uv not found at $(UV)"; \
		echo ""; \
		echo "Run 'make bootstrap-core' in AMI-AGENTS root first:"; \
		echo "  cd $(AGENTS_ROOT)"; \
		echo "  make bootstrap-core"; \
		echo ""; \
		exit 1; \
	fi

# =============================================================================
# Installation Targets
# =============================================================================

# Full install targets - use sequential $(MAKE) calls to ensure correct order
# (install-hooks requires pre-commit from install-package)

.PHONY: install
install: ## Full install: Python + hooks
	@$(MAKE) install-package
	@$(MAKE) install-hooks
	@echo ""
	@echo "Installation complete!"

.PHONY: install-ci
install-ci: ## CI install: Python only, no hooks
	@$(MAKE) install-package
	@echo ""
	@echo "CI installation complete!"

.PHONY: install-package
install-package: preflight ## Install Python dependencies
	$(UV) sync --group dev

.PHONY: install-hooks
install-hooks: preflight ## Install pre-commit hooks (requires install-package to have been run)
	$(UV) run pre-commit install
	$(UV) run pre-commit install --hook-type pre-push

# =============================================================================
# Code Quality Targets (uses shared configs from ami-agents)
# =============================================================================

.PHONY: lint
lint: preflight ## Run ruff linter
	$(UV) run ruff check --config $(AGENTS_RUFF) .
	$(UV) run ruff format --config $(AGENTS_RUFF) --check .

.PHONY: lint-fix
lint-fix: preflight ## Run ruff with auto-fix
	$(UV) run ruff check --config $(AGENTS_RUFF) --fix .
	$(UV) run ruff format --config $(AGENTS_RUFF) .

.PHONY: type-check
type-check: preflight ## Run mypy
	$(UV) run mypy --config-file $(AGENTS_MYPY) ami

.PHONY: test
test: preflight ## Run pytest
	$(UV) run pytest

.PHONY: test-unit
test-unit: preflight ## Run unit tests only
	$(UV) run pytest tests/unit -v

.PHONY: test-integration
test-integration: preflight ## Run integration tests only
	$(UV) run pytest tests/integration -v

.PHONY: test-cov
test-cov: preflight ## Run tests with coverage
	$(UV) run pytest --cov=ami --cov-report=term-missing

.PHONY: check
check: lint type-check test ## Run all checks

.PHONY: pre-commit
pre-commit: preflight ## Run pre-commit on all files
	$(UV) run pre-commit run --all-files

# =============================================================================
# Cleanup Targets
# =============================================================================

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

.PHONY: clean-venv
clean-venv: ## Remove virtual environment
	rm -rf .venv
