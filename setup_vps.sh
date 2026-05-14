#!/usr/bin/env bash
# One-time VPS bootstrap. Run from your local machine.
# Usage: ./setup_vps.sh user@host
set -euo pipefail

VPS="${1:?Usage: ./setup_vps.sh user@host}"
REMOTE_DIR="/home/max/stack"

echo "Bootstrapping ${VPS} ..."

ssh -t "${VPS}" "sudo dnf install -y python3 python3-pip"

ssh "${VPS}" "mkdir -p '${REMOTE_DIR}'"

rsync -az --exclude='.venv/' --exclude='.git/' --exclude='*.pyc' \
    --exclude='airflow.cfg' --exclude='.airflow/' --exclude='airflow.db*' \
    --exclude='logs/' --exclude='data/' --exclude='.env' \
    ./ "${VPS}:${REMOTE_DIR}/"

echo "Installing Airflow (takes a few minutes) ..."
ssh "${VPS}" "bash '${REMOTE_DIR}/install_airflow.sh'"

ssh -t "${VPS}" "
    sudo cp '${REMOTE_DIR}/airflow.service' /etc/systemd/system/airflow.service
    sudo systemctl daemon-reload
    sudo systemctl enable airflow
"

echo ""
echo "Next: copy your .env, run migrations, then start the service"
echo "  scp .env ${VPS}:${REMOTE_DIR}/.env"
echo "  ssh ${VPS} 'cd ${REMOTE_DIR} && .venv/bin/python migrate.py'"
echo "  ssh ${VPS} 'sudo systemctl start airflow'"
