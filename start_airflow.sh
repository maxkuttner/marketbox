#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -f "${SCRIPT_DIR}/.venv/bin/airflow" ]; then
    echo "Airflow not found. Run ./install_airflow.sh first."
    exit 1
fi

# Load .env (DB credentials, API keys)
if [ -f "${SCRIPT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/.env"
    set +a
fi

export PATH="${SCRIPT_DIR}/.venv/bin:${PATH}"
export AIRFLOW_HOME="${SCRIPT_DIR}"
export AIRFLOW__API__PORT=8091
export AIRFLOW__API__BASE_URL="http://localhost:8091"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:8091/execution/"
export AIRFLOW__CORE__DAGS_FOLDER="${SCRIPT_DIR}/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__MP_START_METHOD=fork

echo "Starting Airflow on http://localhost:8091"
echo "AIRFLOW_HOME = ${AIRFLOW_HOME}"
echo ""

exec airflow standalone
