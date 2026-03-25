#!/usr/bin/env python3
"""
Lightweight scheduler for GRRR stock data fetching.
Run this as a background process: python3 run_scheduler.py &

Schedule (all times ET):
  - Intraday 5-min fetch: every 6 minutes during market hours (9:30 AM - 4:00 PM ET), Mon-Fri
  - Daily 30-day fetch: once at 5:30 PM ET after market close, Mon-Fri

Handles timezone conversion (ET = US/Eastern) automatically.
"""

import time
import os
import sys
import logging
from datetime import datetime

try:
    import pytz
    ET = pytz.timezone("US/Eastern")
except ImportError:
    ET = None

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

LOG_FILE = os.path.join(SCRIPT_DIR, "data", "scheduler.log")
os.makedirs(os.path.join(SCRIPT_DIR, "data"), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_et_now():
    if ET:
        return datetime.now(ET)
    # Fallback: assume UTC-4 (EDT)
    from datetime import timedelta, timezone
    return datetime.now(timezone(timedelta(hours=-4)))

def is_market_hours(now_et):
    """Check if current time is during market hours (Mon-Fri, 9:30 AM - 4:00 PM ET)."""
    if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    t = now_et.hour * 60 + now_et.minute
    return 570 <= t <= 960  # 9:30 (570 min) to 16:00 (960 min)

def is_post_close(now_et):
    """Check if it's just after market close (5:25-5:35 PM ET window)."""
    if now_et.weekday() >= 5:
        return False
    t = now_et.hour * 60 + now_et.minute
    return 1045 <= t <= 1055  # 17:25 to 17:35

def main():
    logging.info("GRRR Scheduler started. Monitoring market hours...")
    logging.info("  Intraday: every 6 min during 9:30AM-4:00PM ET, Mon-Fri")
    logging.info("  Daily:    once at ~5:30PM ET, Mon-Fri")

    last_intraday = 0
    last_daily = ""

    while True:
        try:
            now_et = get_et_now()
            now_ts = time.time()

            # Intraday fetch: every 6 minutes during market hours
            if is_market_hours(now_et) and (now_ts - last_intraday) >= 360:
                logging.info("Market open - fetching intraday 5-min data...")
                from fetch_intraday import fetch_intraday
                fetch_intraday()
                last_intraday = now_ts

            # Daily fetch + news: once after market close
            today_str = now_et.strftime("%Y-%m-%d")
            if is_post_close(now_et) and last_daily != today_str:
                logging.info("Market closed - fetching daily 30-day data...")
                from fetch_daily import fetch_daily
                fetch_daily()
                logging.info("Fetching news & sentiment...")
                from fetch_news import fetch_news
                fetch_news()
                logging.info("Fetching Reddit sentiment...")
                from fetch_reddit import fetch_reddit
                fetch_reddit()
                last_daily = today_str

        except Exception as e:
            logging.error(f"Scheduler error: {e}")

        time.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    main()
