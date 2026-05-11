# MARKETBOX

Market Data ETL for projects.

## Deployment

Airflow runs as a systemd service on a VPS, accessible over Tailscale.

### First-time setup

```bash
./setup_vps.sh <user>@<server> 
scp .env <user>@<server>:/opt/marketbox/.env
ssh <user>@<server> 'sudo systemctl start airflow'
```

Three GitHub secrets are required for CI:

| Secret | Example |
|---|---|
| `VPS_HOST` | `ubuntu@100.64.1.1` |
| `VPS_SSH_KEY` | contents of your private SSH key |
| `TAILSCALE_AUTHKEY` | ephemeral key from tailscale.com/settings/keys |

### Continuous deployment

Merging a PR to `main` triggers `.github/workflows/deploy.yml`, which runs tests then rsyncs `dags/` and `src/` to the server. Airflow's scheduler picks up changes within 5 minutes — no restart needed.

### Local development

```bash
./install_airflow.sh   # first time only
./start_airflow.sh     # http://localhost:8091
```