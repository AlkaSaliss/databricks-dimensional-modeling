.DEFAULT_GOAL := help
DBX_PROFILE := personal
TARGET := dev
BUNDLE_RESOURCE := adventure_works_etl

##@ General

.PHONY: help validate-bundle
help: ## Display this help message
	@echo "Usage:"
	@echo "  make \033[36m<target>\033[0m"
	@awk '/^[a-zA-Z0-9_-]+:/ { target = $$1; sub(/:.*/, "", target); if (match($$0, /##/)) { split($$0, parts, /##/); msg = parts[2]; sub(/^ +/, "", msg); sub(/ +$$/, "", msg); printf "  \033[36m%-20s\033[0m %s\n", target, msg; } else if (helpMessage != "") { printf "  \033[36m%-20s\033[0m %s\n", target, helpMessage; } helpMessage = ""; } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); helpMessage = ""; next; } /^## / { helpMessage = substr($$0, 4); next; } /^##/ { helpMessage = substr($$0, 3); next; } !/^(##|#|[ \t]*$$|[a-zA-Z0-9_-]+:)/ { helpMessage = ""; }' $(MAKEFILE_LIST)

##@ Bundle validation

## Validate bundle target
validate-bundle:
	@echo "Validating bundle $(TARGET) target..."
	@databricks bundle validate --target $(TARGET) --profile $(DBX_PROFILE)

## Show planned resources
plan-bundle:
	@echo "Showing planned resources..."
	@databricks bundle plan --target $(TARGET) --profile $(DBX_PROFILE)

## Deploy Bundle
deploy-bundle:
	@echo "Deploying bundle $(TARGET) target..."
	@databricks bundle deploy --target $(TARGET) --profile $(DBX_PROFILE)

## Run bundle with refresh all:
run-bundle:
	@echo "Running bundle $(TARGET) target..."
	@databricks bundle run $(BUNDLE_RESOURCE) -t $(TARGET) --profile $(DBX_PROFILE)

## Run bundle with FULL refresh all:
run-bundle-full:
	@echo "Running bundle $(TARGET) target..."
	@databricks bundle run $(BUNDLE_RESOURCE) -t $(TARGET) --full-refresh-all --profile $(DBX_PROFILE)

## Run pre-commit hooks
pre-commit:
	@echo "Running pre-commit hooks..."
	@uv run pre-commit run --all-files --show-diff-on-failure
