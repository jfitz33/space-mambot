# space-mambot
Discord bot for running Yu-Gi-Oh! custom pack format

Recommended when running space-mambot locally to do so in a virtual python environment.
In the root of the repo, run the following (Windows):

python -m venv .venv
.venv\Scripts\activate

Then make sure dependencies installed:

pip install -r requirements.txt

Now you are free to start the bot

python bot.py

## Configuring the daily rollover time

Daily tasks such as shop sales, gamba chip grants, and starter daily rewards use a shared rollover schedule. By default, the bot rolls over at **00:00 America/New_York**. For local testing, you can override the schedule without touching code:

- `DAILY_ROLLOVER_TIME` — `HH:MM` (24-hour clock) for the rollover time of day (e.g., `12:30` for 12:30 PM).
- `DAILY_ROLLOVER_TZ` — IANA timezone name for the rollover boundary (e.g., `UTC` or `America/Los_Angeles`).

These environment variables are read at startup, so restart the bot after changing them.