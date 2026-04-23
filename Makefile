# Makefile for AMI-DATAOPS
# Data operations toolkit: backup, sync, provisioning, monitoring, maintenance.
#
# This project is a uv workspace member of AMI-AGENTS. It must be cloned
# inside the AMI-AGENTS repo at projects/AMI-DATAOPS.

# =============================================================================
# Configuration
# =============================================================================
-include .env
export

AGENTS_ROOT := $(abspath ../..)
AGENTS_REPO := git@hf.co:ami-ailabs/AMI-AGENTS
AGENTS_PYPROJECT := $(AGENTS_ROOT)/pyproject.toml
AGENTS_RUFF := $(AGENTS_ROOT)/res/config/ruff.toml
AGENTS_MYPY := $(AGENTS_ROOT)/res/config/mypy.toml
AGENTS_BOOT := $(AGENTS_ROOT)/.boot-linux
CI_SCRIPTS := $(AGENTS_ROOT)/projects/AMI-CI/scripts
CI_LIB := $(AGENTS_ROOT)/projects/AMI-CI/lib

# Contract compliance
-include $(CI_LIB)/makefile_contract.mk

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
	@echo "  install-hooks        Generate native git hooks"
	@echo ""
	@echo "Code quality targets:"
	@echo "  lint                 Run ruff linter"
	@echo "  lint-fix             Run ruff with auto-fix"
	@echo "  type-check           Run mypy"
	@echo "  test                 Run pytest"
	@echo "  test-cov             Run tests with coverage"
	@echo "  check                Run all checks"
	@echo "  check-hooks          Dry-run hook generation (verify config)"
	@echo ""
	@echo "Compose stack targets:"
	@echo "  compose-deploy       Deploy all services and enable on boot"
	@echo "  compose-stop         Stop compose stack"
	@echo "  compose-restart      Restart compose stack"
	@echo "  compose-status       Show service status"
	@echo "  ensure-profiles      Ensure specific profiles are running (PROFILES=data,secrets)"
	@echo ""
	@echo "Serve (Cloudflare Tunnel) targets:"
	@echo "  serve-deploy         Render + start cloudflared tunnels"
	@echo "  serve-stop           Stop cloudflared tunnels"
	@echo "  serve-restart        Restart cloudflared tunnels"
	@echo "  serve-status         Report tunnel + metrics state"
	@echo "  serve-route-dns      Create Cloudflare CNAMEs for bound hostnames"
	@echo "  serve-logs NAME=<t>  Tail journalctl for ami-serve-<t>.service"
	@echo ""
	@echo "Intake (P2P log receiver) targets:"
	@echo "  intake-deploy        Render config + start ami-intake.service"
	@echo "  intake-stack-deploy  Deploy intake + cloudflared tunnel + DNS from deploy/intake.vars.yml"
	@echo "  intake-stop          Stop ami-intake"
	@echo "  intake-restart       Restart ami-intake"
	@echo "  intake-status        Report unit state + audit.log size"
	@echo "  intake-logs          Tail journalctl for ami-intake.service"
	@echo ""
	@echo "Report (P2P log sender) targets:"
	@echo "  report-send CONFIG=   TUI send of selected logs to a peer"
	@echo "  report-preview CONFIG= List candidate files without sending"
	@echo "  report-peers CONFIG=   List configured peers + env-var state"
	@echo ""
	@echo "Other targets:"
	@echo "  clean                Remove build artifacts"
	@echo "  clean-venv           Remove virtual environment"
	@echo "  cleanup-precommit    Remove legacy pre-commit package traces"

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
	@if [ ! -f "$(AGENTS_ROOT)/projects/AMI-CI/lib/checks.sh" ]; then \
		echo "📦 AMI-CI not found — cloning to $(AGENTS_ROOT)/projects/AMI-CI..."; \
		git clone git@github.com:Independent-AI-Labs/AMI-CI.git "$(AGENTS_ROOT)/projects/AMI-CI"; \
		echo "✅ AMI-CI cloned"; \
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
	$(UV) sync --extra dev

.PHONY: sync
sync: install-package install-hooks ## Sync deps + reinstall hooks (run after AMI-CI changes)

.PHONY: install-hooks
install-hooks: preflight ## Generate native git hooks from .pre-commit-config.yaml
	@bash $(CI_SCRIPTS)/cleanup-precommit 2>/dev/null || true
	bash $(CI_SCRIPTS)/generate-hooks

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

.PHONY: test-cov
test-cov: preflight ## Run tests with coverage
	$(UV) run pytest --cov=ami --cov-report=term-missing

.PHONY: check
check: lint type-check test ## Run all checks

.PHONY: check-hooks
check-hooks: preflight ## Dry-run hook generation (verify config)
	bash $(CI_SCRIPTS)/generate-hooks --dry-run

.PHONY: cleanup-precommit
cleanup-precommit: ## Remove legacy pre-commit package traces
	bash $(CI_SCRIPTS)/cleanup-precommit

# =============================================================================
# Compose Stack Targets (Docker service orchestration)
# =============================================================================

ANSIBLE_PLAYBOOK := $(AGENTS_BOOT)/bin/ansible-playbook
ANSIBLE_COMPOSE := $(ANSIBLE_PLAYBOOK) res/ansible/compose.yml

.PHONY: compose-deploy
compose-deploy: ## Deploy compose stack with all profiles and enable on boot
	$(ANSIBLE_COMPOSE) --tags deploy

.PHONY: ensure-profiles
ensure-profiles: ## Ensure requested profiles are running (PROFILES=data,secrets)
	@if [ -z "$(PROFILES)" ]; then echo "ERROR: Set PROFILES=data,secrets (comma-separated)"; exit 1; fi
	$(ANSIBLE_COMPOSE) --tags ensure -e "{\"ensure_profiles\":[\"$$(echo '$(PROFILES)' | sed 's/,/","/g')\"]}"

.PHONY: compose-stop
compose-stop: ## Stop compose stack
	$(ANSIBLE_COMPOSE) --tags stop

.PHONY: compose-restart
compose-restart: ## Restart compose stack
	$(ANSIBLE_COMPOSE) --tags restart

.PHONY: compose-status
compose-status: ## Show compose stack status
	$(ANSIBLE_COMPOSE) --tags status

# =============================================================================
# Serve (Cloudflare Tunnel) Targets
# =============================================================================

ANSIBLE_SERVE := $(ANSIBLE_PLAYBOOK) res/ansible/serve.yml
INTAKE_VARS := deploy/intake.vars.yml

.PHONY: serve-deploy
serve-deploy: ## Render cloudflared configs + enable/start tunnel units
	$(ANSIBLE_SERVE) --tags deploy

.PHONY: serve-stop
serve-stop: ## Stop all ami-serve tunnel units
	$(ANSIBLE_SERVE) --tags stop

.PHONY: serve-restart
serve-restart: ## Restart all ami-serve tunnel units
	$(ANSIBLE_SERVE) --tags restart

.PHONY: serve-status
serve-status: ## Report systemd + metrics state of tunnels
	$(ANSIBLE_SERVE) --tags status

.PHONY: serve-route-dns
serve-route-dns: ## Create Cloudflare CNAMEs for configured hostnames
	$(ANSIBLE_SERVE) --tags route-dns

.PHONY: serve-logs
serve-logs: ## Tail a single tunnel (usage: make serve-logs NAME=main)
	@if [ -z "$(NAME)" ]; then echo "ERROR: Set NAME=<tunnel>"; exit 1; fi
	journalctl --user -u ami-serve-$(NAME).service -f

# =============================================================================
# Intake (P2P log receiver) Targets
# =============================================================================

ANSIBLE_INTAKE := $(ANSIBLE_PLAYBOOK) res/ansible/intake.yml

.PHONY: intake-deploy
intake-deploy: ## Render intake config + install/start the systemd user unit
	$(ANSIBLE_INTAKE) --tags deploy

.PHONY: intake-stack-deploy
intake-stack-deploy: ## Deploy intake + cloudflared tunnel for reports.ami-remote.work
	@if [ ! -f $(INTAKE_VARS) ]; then \
		echo "ERROR: $(INTAKE_VARS) missing. See docs/runbooks/INTAKE-PRODUCTION.md."; \
		exit 1; \
	fi
	$(ANSIBLE_INTAKE) --tags deploy -e @$(INTAKE_VARS)
	$(ANSIBLE_SERVE) --tags deploy -e @$(INTAKE_VARS)
	@echo "DNS reconciliation: run 'ami-serve bootstrap-cloudflare' to re-assert the CNAME."

.PHONY: intake-stop
intake-stop: ## Stop the ami-intake daemon
	$(ANSIBLE_INTAKE) --tags stop

.PHONY: intake-restart
intake-restart: ## Restart the ami-intake daemon
	$(ANSIBLE_INTAKE) --tags restart

.PHONY: intake-status
intake-status: ## Report ami-intake unit state + audit.log size
	$(ANSIBLE_INTAKE) --tags status

.PHONY: intake-logs
intake-logs: ## Tail the ami-intake journal
	journalctl --user -u ami-intake.service -f

# =============================================================================
# Report (P2P log sender) Targets
# =============================================================================

.PHONY: report-send
report-send: ## Interactive TUI report send (usage: make report-send CONFIG=path)
	@if [ -z "$(CONFIG)" ]; then echo "ERROR: Set CONFIG=<path>"; exit 1; fi
	$(UV) run python -m ami.dataops.report.main send --config $(CONFIG)

.PHONY: report-preview
report-preview: ## List candidate files without sending (usage: make report-preview CONFIG=path)
	@if [ -z "$(CONFIG)" ]; then echo "ERROR: Set CONFIG=<path>"; exit 1; fi
	$(UV) run python -m ami.dataops.report.main preview --config $(CONFIG)

.PHONY: report-peers
report-peers: ## List configured peers + env-var state
	@if [ -z "$(CONFIG)" ]; then echo "ERROR: Set CONFIG=<path>"; exit 1; fi
	$(UV) run python -m ami.dataops.report.main peers --config $(CONFIG)

# =============================================================================
# Volume Backup & Restore
# =============================================================================

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
