# Runbook: deploy `reports.ami-remote.work` intake

One-page operator flow. Assumes this machine is the intake host, the Cloudflare
zone `ami-remote.work` exists in your account, and `cloudflared` is on PATH
(installed by `ami-extra` / the bootstrap installer — check with
`which cloudflared`).

## 1. Create a Cloudflare API token (one-time, 2 min)

In the Cloudflare dashboard → **My Profile** → **API Tokens** → **Create Token**
→ **Custom token** with exactly these two permissions:

| Resource | Permission | Scope |
|---|---|---|
| `Account → Cloudflare Tunnel` | `Edit` | your account |
| `Zone → DNS` | `Edit` | `ami-remote.work` zone only |

Record the token string, your Cloudflare Account ID (in the dashboard URL), and
the Zone ID for `ami-remote.work` (Overview tab → right sidebar).

## 2. Create the secrets file (one-time)

```bash
cd projects/AMI-DATAOPS
cp deploy/intake.env.example deploy/intake.env
chmod 600 deploy/intake.env
```

Edit `deploy/intake.env` and fill in:
- `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_ZONE_ID` — from step 1.
- `AMI_INTAKE_TOKENS__REPORTS` — generate with `openssl rand -hex 32`.
- `AMI_INTAKE_SECRETS__REPORTS` — generate with `openssl rand -hex 32` (different value).

Record the two `AMI_INTAKE_*` values — senders need them to authenticate.

## 3. Bootstrap the tunnel + DNS (one-time, idempotent)

```bash
set -a; source deploy/intake.env; set +a
uv run ami-serve bootstrap-cloudflare \
    --tunnel intake-main \
    --hostname reports.ami-remote.work
```

The command creates the Cloudflare tunnel (or reuses it), writes
`~/.cloudflared/<uuid>.json` at mode 0600, and ensures a CNAME
`reports → <uuid>.cfargotunnel.com` in the zone. It prints JSON like:

```json
{
  "tunnel_id": "c1744f8b-faa1-48a4-9e5c-02ac921467fa",
  "credentials_path": "~/.cloudflared/c1744f8b-....json",
  "cname_fqdn": "reports.ami-remote.work",
  "cname_target": "c1744f8b-....cfargotunnel.com",
  "tunnel_created": true,
  "cname_changed": true
}
```

Paste the `tunnel_id` into `deploy/intake.vars.yml`:

```yaml
dataops_serve_tunnels:
  - name: intake-main
    tunnel_id: c1744f8b-faa1-48a4-9e5c-02ac921467fa            # from bootstrap
    credentials_file: "{{ ansible_env.HOME }}/.cloudflared/c1744f8b-....json"
```

The CLOUDFLARE_API_TOKEN is not needed after this step — rotate or revoke it.

## 4. Deploy intake + tunnel

```bash
make intake-stack-deploy
```

This runs both `ansible-playbook res/ansible/intake.yml --tags deploy` and
`ansible-playbook res/ansible/serve.yml --tags deploy,route-dns` with the
vars file attached. The playbooks are idempotent; rerun any time.

## 5. Verify

```bash
systemctl --user is-active ami-intake.service ami-serve-tunnel@intake-main.service
loginctl show-user "$USER" --property=Linger                 # expect Linger=yes
dig +short reports.ami-remote.work                           # expect <uuid>.cfargotunnel.com
curl -fsS https://reports.ami-remote.work/healthz            # expect HTTP/2 200
```

## 6. Reboot smoke test

```bash
sudo reboot
# wait ~60s after boot, no SSH login required; the linger-backed user manager
# starts both units automatically.
ssh ami@<host> 'systemctl --user is-active ami-intake.service ami-serve-tunnel@intake-main.service'
```

Both should print `active`. If either is `inactive`, check:

```bash
systemctl --user status ami-intake.service
journalctl --user -u ami-intake.service -n 100
systemctl --user status ami-serve-tunnel@intake-main.service
journalctl --user -u ami-serve-tunnel@intake-main.service -n 100
```

## Common follow-ups

- **Rotate sender secrets:** edit `deploy/intake.env`, rerun `make intake-stack-deploy`
  (the playbook copies the env file and restarts the daemon; a full rerun is
  cheap and idempotent). Notify the sender operator so they update their
  `AMI_REPORT_SECRET_REPORTS` / `AMI_REPORT_TOKENS__REPORTS` env vars.
- **Add a new sender:** append to `dataops_intake_config.allowed_senders` in
  `deploy/intake.vars.yml` and add the matching `AMI_INTAKE_TOKENS__<ID>` +
  `AMI_INTAKE_SECRETS__<ID>` pair to `deploy/intake.env`. Rerun
  `make intake-stack-deploy`.
- **Stop intake temporarily:** `make intake-stop` (the tunnel stays up).
  Restart: `make intake-restart`.
- **Tail logs:** `make intake-logs` / `make serve-logs NAME=intake-main`.
