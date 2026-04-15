# Deploying Nyrkiö v3 on the staging Lightsail box

Target layout:

    staging.nyrkio.com/v3/*   →  nginx (in existing docker stack)
                             →  127.0.0.1:8123 (uvicorn, systemd)
                             →  /var/lib/nyrkio-v3/store.json   (persistence)

No Docker for this service itself — it's a single uvicorn process managed
by systemd. The existing dockerized nginx reverse-proxies to it via
`host.docker.internal`.

## 1. Prereqs on the box

```bash
# Install uv (Python package manager + Python installer).
curl -LsSf https://astral.sh/uv/install.sh | sh
# Create a dedicated user.
sudo useradd --system --home-dir /opt/nyrkio-v3 --shell /usr/sbin/nologin nyrkio
```

## 2. Code + data dirs

```bash
sudo mkdir -p /opt/nyrkio-v3 /var/lib/nyrkio-v3 /etc/nyrkio-v3
sudo chown nyrkio:nyrkio /var/lib/nyrkio-v3
sudo -u nyrkio -H bash -c 'cd /opt/nyrkio-v3 && \
  git clone https://github.com/nyrkio/nyrkiov3       nyrkiov3 && \
  git clone https://github.com/nyrkio/AuroraBorealis AuroraBorealis'
# That's all — purejson, extjson, jsonee, benchzoo are pulled from
# GitHub by `uv sync` (see [tool.uv.sources] in pyproject.toml).
```

Install deps (uv resolves against the pyproject's `requires-python=">=3.14"`):

```bash
cd /opt/nyrkio-v3/nyrkiov3
sudo -u nyrkio -H uv sync --python 3.14t
```

## 3. Secrets / env

Register a GitHub OAuth app at https://github.com/settings/applications/new
with callback `https://staging.nyrkio.com/v3/oauth/callback`. Grab the
client id + secret.

```bash
sudo install -o root -g root -m 0600 \
  /opt/nyrkio-v3/nyrkiov3/deploy/env.example /etc/nyrkio-v3/env
sudoedit /etc/nyrkio-v3/env
# Fill in:
#   NYRKIO_GITHUB_CLIENT_ID=...
#   NYRKIO_GITHUB_CLIENT_SECRET=...
#   NYRKIO_SESSION_SECRET=$(openssl rand -hex 32)
#   NYRKIO_APP_GITHUB_PAT=...
```

## 4. Systemd

```bash
sudo install -o root -g root -m 0644 \
  /opt/nyrkio-v3/nyrkiov3/deploy/nyrkio-v3.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nyrkio-v3
sudo systemctl status nyrkio-v3
journalctl -u nyrkio-v3 -f
```

## 5. Nginx

Add the contents of `nginx-v3.snippet` to the `staging.nyrkio.com`
server {} block in `/home/claude/nyrkio/nginx/nginx.conf`. Then
extend the nginx service in `docker-compose.yml` (or its dev variant)
with:

```yaml
services:
  nginx:
    # ...existing config...
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

Reload:

```bash
cd /home/claude/nyrkio
docker compose up -d nginx           # picks up the extra_hosts
docker compose exec nginx nginx -s reload
```

## 6. Smoke test

```bash
curl -sS https://staging.nyrkio.com/v3/api/v3/config
# → {"recent_cp_days": 14, "auth_enabled": true}

curl -sS -X POST -H 'content-type: application/json' \
  -d '{"repo":"unodb-dev/unodb"}' \
  https://staging.nyrkio.com/v3/api/v3/public/connect
# → 202 {"accepted": true, ...}

# Then browse to https://staging.nyrkio.com/v3/  and you should see
# the landing page with the two paths.
```

## Updating

```bash
sudo -u nyrkio -H bash -c 'cd /opt/nyrkio-v3/nyrkiov3 && git pull &&
  cd ../AuroraBorealis && git pull'
sudo systemctl restart nyrkio-v3
```

Static file changes (AuroraBorealis) don't need a restart — the
browser will pick them up on reload.
