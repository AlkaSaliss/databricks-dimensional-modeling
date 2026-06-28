.DEFAULT_GOAL := help
TARGET := dev

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
	@databricks bundle validate --target $(TARGET)

## Show planned resources
plan-bundle:
	@echo "Showing planned resources..."
	@databricks bundle plan --target $(TARGET)

## Deploy Bundle
deploy-bundle:
	@echo "Deploying bundle $(TARGET) target..."
	@databricks bundle deploy --target $(TARGET)


## Run pre-commit hooks
pre-commit:
	@echo "Running pre-commit hooks..."
	@uv run pre-commit run --all-files --show-diff-on-failure
