#!/usr/bin/env python3
"""
Master script: fetches both daily (30-day) and intraday (5-min) data for GRRR.
Designed to be called by cron.
"""

import os
import sys
import logging
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "data", "fetch.log")

os.makedirs(os.path.join(SCRIPT_DIR, "data"), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def main():
    logging.info("=" * 50)
    logging.info("GRRR Stock Data Fetch Started")
    logging.info("=" * 50)

    # Import and run both fetchers
    sys.path.insert(0, SCRIPT_DIR)
    from fetch_daily import fetch_daily
    from fetch_intraday import fetch_intraday

    daily_ok = fetch_daily()
    intraday_ok = fetch_intraday()

    if daily_ok and intraday_ok:
        logging.info("All fetches completed successfully.")
    else:
        logging.warning(f"Some fetches failed. daily={daily_ok}, intraday={intraday_ok}")

    logging.info("Done.\n")

if __name__ == "__main__":
    main()
