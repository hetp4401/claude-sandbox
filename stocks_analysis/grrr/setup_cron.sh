#!/bin/bash
# Sets up cron jobs for GRRR stock data fetching.
#
# Schedule:
#   - Daily data:    Once per day at 5:00 PM ET (21:00 UTC) after market close
#   - Intraday 5min: Every 6 minutes during market hours Mon-Fri (9:30AM-4:00PM ET = 14:30-21:00 UTC)
#
# All times in UTC (cron default). ET = UTC-4 (EDT) or UTC-5 (EST).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$(which python3)"
FETCH_ALL="$SCRIPT_DIR/fetch_all.py"
FETCH_INTRADAY="$SCRIPT_DIR/fetch_intraday.py"
FETCH_DAILY="$SCRIPT_DIR/fetch_daily.py"

echo "Setting up GRRR stock data cron jobs..."
echo "  Script dir:  $SCRIPT_DIR"
echo "  Python:      $PYTHON"

# Remove any existing GRRR cron entries to avoid duplicates
crontab -l 2>/dev/null | grep -v "GRRR" | grep -v "grrr" > /tmp/cron_clean.txt

# Add new cron jobs
cat >> /tmp/cron_clean.txt << EOF

# === GRRR Stock Data Fetcher ===
# Daily 30-day history: run at 9:30 PM UTC (5:30 PM ET) after market close, Mon-Fri
30 21 * * 1-5 $PYTHON $FETCH_DAILY >> $SCRIPT_DIR/data/cron_daily.log 2>&1

# Intraday 5-min bars: every 6 min during market hours (14:30-21:00 UTC), Mon-Fri
*/6 14-20 * * 1-5 $PYTHON $FETCH_INTRADAY >> $SCRIPT_DIR/data/cron_intraday.log 2>&1

# Full refresh (daily + intraday): once at market close 9:05 PM UTC, Mon-Fri
5 21 * * 1-5 $PYTHON $FETCH_ALL >> $SCRIPT_DIR/data/cron_all.log 2>&1
EOF

crontab /tmp/cron_clean.txt
rm /tmp/cron_clean.txt

echo ""
echo "Cron jobs installed. Current crontab:"
crontab -l
echo ""
echo "Done! Data will be saved to: $SCRIPT_DIR/data/"
