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
    from fetch_news import fetch_news
    from fetch_reddit import fetch_reddit
    from fetch_short_interest import fetch_short_interest
    from fetch_insider import fetch_insider
    from fetch_options import fetch_options
    from fetch_sec_filings import fetch_sec_filings
    from technical_alerts import generate_alerts

    results = {}
    results["daily"] = fetch_daily()
    results["intraday"] = fetch_intraday()
    results["news"] = fetch_news()
    results["reddit"] = fetch_reddit()
    results["short_interest"] = fetch_short_interest()
    results["insider"] = fetch_insider()
    results["options"] = fetch_options()
    results["sec_filings"] = fetch_sec_filings()
    # Run technical alerts last (depends on daily + intraday data)
    results["alerts"] = generate_alerts()

    failed = [k for k, v in results.items() if not v]
    if not failed:
        logging.info("All fetches completed successfully.")
    else:
        logging.warning(f"Some fetches failed: {failed}")

    logging.info("Done.\n")

if __name__ == "__main__":
    main()
