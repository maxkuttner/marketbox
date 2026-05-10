#!/usr/bin/env bash
# Install Apache Airflow from PyPI (official guide):
# https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_VERSION="3.2.1"
PYTHON_VERSION="$(python3 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing Airflow ${AIRFLOW_VERSION} for Python ${PYTHON_VERSION}"
echo "Constraints: ${CONSTRAINT_URL}"
echo ""

cd "${SCRIPT_DIR}"

# Create virtual environment if it does not exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Bootstrap pip inside the venv (uv-managed venvs omit pip by default)
if ! .venv/bin/python -m pip --version &>/dev/null; then
    echo "Bootstrapping pip..."
    .venv/bin/python -m ensurepip --upgrade
fi


# upgrade pip
.venv/bin/python -m pip install --upgrade pip

# install uv
.venv/bin/python -m pip install uv

echo "Installing packages (this takes a few minutes)..."
.venv/bin/uv pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "databento>=0.77.0" \
    "psycopg[binary]>=3.0" \
    "python-dotenv>=1.0" \
    --constraint "${CONSTRAINT_URL}"

echo ""
echo "Done. Run ./start_airflow.sh to launch the UI on http://localhost:8091"
