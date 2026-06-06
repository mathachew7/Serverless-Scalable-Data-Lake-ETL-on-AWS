.PHONY: help diagram test test-cov install-dev init plan apply destroy upload-sample fmt validate

TERRAFORM_DIR := terraform

help:
	@echo ""
	@echo "  AWS Serverless Data Lake ETL — Commands"
	@echo ""
	@echo "  Visualization"
	@echo "    make diagram        Generate docs/architecture.png"
	@echo ""
	@echo "  Testing"
	@echo "    make test           Run unit tests"
	@echo "    make test-cov       Run tests with coverage report"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make init           terraform init"
	@echo "    make validate       terraform validate"
	@echo "    make fmt            terraform fmt (auto-format HCL)"
	@echo "    make plan           terraform plan"
	@echo "    make apply          terraform apply"
	@echo "    make destroy        terraform destroy (requires confirmation)"
	@echo ""
	@echo "  Pipeline"
	@echo "    make upload-sample  Upload sample CSV/JSON to raw bucket (after apply)"
	@echo ""

# ── Visualization ─────────────────────────────────────────────────────────────
diagram:
	@pip show diagrams > /dev/null 2>&1 || (echo "Run: brew install graphviz && pip install diagrams" && exit 1)
	python architecture.py
	@echo "Generated: docs/architecture.png"
	open docs/architecture.png 2>/dev/null || xdg-open docs/architecture.png 2>/dev/null || true

# ── Tests ─────────────────────────────────────────────────────────────────────
install-dev:
	pip install -r requirements-dev.txt

test:
	pytest tests/unit/test_handler.py -v

test-etl:
	pytest tests/unit/test_transforms.py -v

test-all:
	pytest tests/unit/ -v

test-cov:
	pytest tests/unit/ -v \
		--cov=lambda/ingestion_trigger \
		--cov=glue \
		--cov-report=term-missing

# ── Terraform ─────────────────────────────────────────────────────────────────
init:
	cd $(TERRAFORM_DIR) && terraform init

validate:
	cd $(TERRAFORM_DIR) && terraform validate

fmt:
	cd $(TERRAFORM_DIR) && terraform fmt -recursive

plan:
	cd $(TERRAFORM_DIR) && terraform plan

apply:
	cd $(TERRAFORM_DIR) && terraform apply

destroy:
	@echo "WARNING: This will destroy ALL AWS resources."
	@echo "Run manually: cd terraform && terraform destroy"

# ── Pipeline ──────────────────────────────────────────────────────────────────
upload-sample:
	python scripts/upload_test_data.py
