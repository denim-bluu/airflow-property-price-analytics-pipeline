include .env

# ==================================================================================== #
# HELPERS
# ==================================================================================== #
.DEFAULT_GOAL := help
.PHONY: help
help: ## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'
	
.PHONY: wait
wait: ## Wait for 5 seconds
	@echo -n 'Waiting for 5 seconds...' && sleep 5

.PHONY: confirm
confirm: ## Confirm action
	@echo -n 'Are you sure? [y/N] ' && read ans && [ $${ans:-N} = y ]


# ==================================================================================== #
# DEVELOPMENT
# ==================================================================================== #
.PHONY: airflow/cleanup
airflow/cleanup: ## CLeanup airflow
	docker compose down --volumes --rmi all

.PHONY: airflow/init_db
airflow/init_db: ## Initialize airflow database
	docker compose up airflow-init

.PHONY: airflow/up
airflow/up: ## Start airflow
	docker compose up --build