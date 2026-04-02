#!/usr/bin/env bash
set -euo pipefail

# Deterministic local test bootstrap for MetaMind.
# - Creates .venv if missing
# - Installs runtime + dev dependencies
# - Generates .env.test from template if absent

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

if [[ ! -d "${VENV_DIR}" ]]; then
  python3 -m venv "${VENV_DIR}"
fi

source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip
python -m pip install -r "${ROOT_DIR}/requirements.txt"
python -m pip install -e "${ROOT_DIR}[dev]"

if [[ ! -f "${ROOT_DIR}/.env.test" && -f "${ROOT_DIR}/.env.test.example" ]]; then
  cp "${ROOT_DIR}/.env.test.example" "${ROOT_DIR}/.env.test"
fi

echo "Test environment ready."
echo "Activate with: source .venv/bin/activate"
echo "Run tests with: pytest tests/unit -q"
