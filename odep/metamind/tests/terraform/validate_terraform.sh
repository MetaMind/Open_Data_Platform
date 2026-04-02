#!/bin/bash
# Terraform Validation Script — MetaMind Platform
# File: tests/terraform/validate_terraform.sh
set -euo pipefail

TERRAFORM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../infra/terraform" && pwd)"
echo "=== MetaMind Terraform Validation ==="
echo "Directory: $TERRAFORM_DIR"
cd "$TERRAFORM_DIR"

if ! command -v terraform &>/dev/null; then
    echo "ERROR: terraform not found in PATH"
    exit 1
fi

echo "--- terraform version ---"
terraform version

echo "--- terraform init (no backend) ---"
terraform init -backend=false -no-color

echo "--- terraform validate ---"
terraform validate -no-color

echo "--- terraform fmt check ---"
terraform fmt -check -recursive -no-color

echo "=== All Terraform checks passed ==="
