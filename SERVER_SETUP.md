# Akamai Cloud (Linode) Server Setup for Space Mambot

Use this checklist to provision and harden an Akamai Cloud Compute (Linode) instance for running the Discord bot.

## 1) Choose a plan and deploy the instance
- Start with **Shared CPU 2 GB (1 vCPU, 50 GB SSD)**; scale to **Shared 4 GB** or **Dedicated 2 vCPU/4–8 GB** if you add heavier tasks (images, larger datasets, more guilds).
- Pick the closest region to your main Discord audience for lower latency.
- Create the instance with the latest **Ubuntu LTS** image and add an SSH key during creation; disable password auth if available.

## 2) Initial OS hardening
- Log in as `root` via SSH and immediately create a non-root sudo user:
  ```bash
  adduser botadmin
  usermod -aG sudo botadmin
  ```
- Enable basic firewall rules with UFW (SSH only to start):
  ```bash
  ufw allow OpenSSH
  ufw enable
  ufw status
  ```
- Set the hostname and time zone (match the bot’s expected rollover zone if desired):
  ```bash
  hostnamectl set-hostname space-mambot
  sudo timedatectl set-timezone America/New_York
  ```
- Install updates and reboot:
  ```bash
  apt-get update && apt-get upgrade -y
  reboot
  ```

## 3) Install runtime dependencies (as non-root)
- SSH in as `botadmin` and install Python and build tools:
  ```bash
  sudo apt-get install -y python3 python3-venv python3-pip git
  ```
- Optional but recommended: `tmux` for shell persistence and `fail2ban` for SSH brute-force protection.

## 4) Fetch the bot code
- Clone the repository (adjust the path as needed):
  ```bash
  git clone https://github.com/<your-org>/space-mambot.git
  cd space-mambot
  ```

## 5) Configure the Python environment
- Create and activate a virtual environment:
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  ```
- Install dependencies:
  ```bash
  pip install --upgrade pip
  pip install -r requirements.txt
  ```

## 6) Set environment variables
- Export secrets and runtime settings in a `.env` file (loaded by your process manager):
  ```bash
  DISCORD_TOKEN=...            # bot token
  DAILY_ROLLOVER_TIME=00:00    # optional override
  DAILY_ROLLOVER_TZ=America/New_York
  ```
- Keep `.env` readable only by the bot user: `chmod 600 .env`.

## 7) Create a systemd service for automatic restarts
- Write the unit file with `sudo tee` (adjust paths if you cloned somewhere else):
   - Why `sudo tee`? Redirects like `>` happen in your current shell, so `sudo echo "..." > /etc/systemd/system/foo.service` still fails without permission. Piping the content into `sudo tee /etc/systemd/system/space-mambot.service` runs the write as root while still showing what was written. Add `> /dev/null` to silence the echoed content if you prefer.
  ```ini
  [Unit]
  Description=Space Mambot Discord bot
  After=network.target

  [Service]
  Type=simple
  User=botadmin
  WorkingDirectory=/home/botadmin/space-mambot
  EnvironmentFile=/home/botadmin/space-mambot/.env
  ExecStart=/home/botadmin/space-mambot/.venv/bin/python bot.py
  Restart=on-failure
  RestartSec=5

  [Install]
  WantedBy=multi-user.target
  ```
- Double-check paths: `WorkingDirectory` must be the repo root, `EnvironmentFile` should point to your `.env`, and `ExecStart`
  must use the virtualenv Python binary.
- Optional hardening: add `RestartPreventExitStatus=SIGTERM` to avoid restarts on manual stops, and consider `MemoryMax` or
  `CPUQuota` limits if you want to cap resource use.
- Reload systemd and start/enable the service:
  ```bash
  sudo systemctl daemon-reload
  sudo systemctl enable --now space-mambot
  sudo systemctl status space-mambot
  ```

## 8) Logging and updates
- View logs with `journalctl -u space-mambot -f`.
- Periodically pull updates and reinstall requirements inside the venv:
  ```bash
  cd /home/botadmin/space-mambot
  git pull
  source .venv/bin/activate
  pip install -r requirements.txt
  sudo systemctl restart space-mambot
  ```

## 9) Backups and monitoring
- Enable Akamai/Linode backups for the instance or back up `/home/botadmin/space-mambot` and your `.env` to object storage.
- Configure email alerts for CPU/memory/disk via the Linode dashboard.

## 10) Optional hardening and access
- Restrict SSH by IP (UFW `allow from <ip> to any port 22`) and disable password logins in `/etc/ssh/sshd_config`.
- Use `fail2ban` defaults to throttle SSH brute-force attempts.
- Rotate bot tokens and regenerate Discord tokens if credentials may have been exposed.