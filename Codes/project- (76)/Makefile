# file: Makefile
# Developer shortcuts for common CI/CD tasks

.PHONY: help test-local test-unit test-integration lint deploy-dev rollback

ENVIRONMENT ?= dev
COMMIT_SHA := $(shell git rev-parse --short HEAD)

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt
	pip install pylint cfn-lint bandit pytest boto3 moto

lint: ## Run all linters
	@echo "=== Python Syntax ==="
	python -m py_compile glue_jobs/*.py
	@echo "=== Pylint ==="
	pylint --errors-only glue_jobs/*.py || true
	@echo "=== CloudFormation Lint ==="
	cfn-lint infrastructure/*.yaml
	@echo "=== Security Scan ==="
	bandit -r glue_jobs/ -ll || true

test-unit: ## Run unit tests only (fast, no AWS calls)
	pytest tests/unit/ -v --tb=short

test-integration: ## Run integration tests (requires AWS dev account)
	ENVIRONMENT=dev pytest tests/integration/ -v --tb=short

test-local: lint test-unit ## Lint + unit tests (run before pushing)

deploy-dev: ## Deploy to dev environment
	@echo "Deploying commit $(COMMIT_SHA) to dev..."
	aws s3 sync glue_jobs/ s3://quickcart-artifacts-dev/glue-scripts/$(COMMIT_SHA)/ --sse AES256
	aws cloudformation deploy \
		--template-file infrastructure/glue-jobs.yaml \
		--stack-name quickcart-glue-jobs-dev \
		--parameter-overrides file://infrastructure/parameters/dev.json CommitSha=$(COMMIT_SHA) \
		--capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
		--no-fail-on-empty-changeset

migrate-dev: ## Run Redshift migrations against dev
	python scripts/run_redshift_migrations.py --environment dev

rollback: ## Rollback production to previous version
	python scripts/rollback.py --environment $(ENVIRONMENT)

rollback-verify: ## Verify current deployment state
	python scripts/rollback.py --environment $(ENVIRONMENT) --verify-only

smoke-test: ## Run smoke tests against environment
	ENVIRONMENT=$(ENVIRONMENT) pytest tests/smoke/ -v --tb=short