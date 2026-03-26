# grrr-rules

Recompute GRRR association rules at all granularities and show the best ones.

## Instructions

Run the GRRR master association rule miner to refresh all trading rules with the latest market data. This fetches fresh data and mines rules at 3 levels:

1. **Daily** - 60-day window, patterns like "mod_down day → next UP 83%"
2. **Intraday 5-min** - ~4,500 bars, patterns like "RSI oversold + deep red → 2h UP 68%"
3. **Sentiment** - News + Reddit sentiment mapped to next session price outcomes

Execute this command and wait for it to complete:

```bash
cd /home/user/claude-sandbox/stocks_analysis/grrr && python3 compute_rules.py
```

This will take several minutes. When it finishes, read and present the master report:

```bash
cat /home/user/claude-sandbox/stocks_analysis/grrr/data/grrr_master_rules_report.txt
```

Present the results to the user organized by tier:
- **Tier S**: 100% confidence, 10+ samples (perfect rules)
- **Tier A**: 80-99% confidence, 7+ samples (near-perfect)
- **Tier B**: 65%+ confidence, 20+ samples (high-volume reliable)
- **Simplest rules**: 1 condition only (least overfit)
- **Best BUY/SELL signals**: Direction predictions only

Also note any changes from the previous run if the user has run this before.

Options the user might specify:
- "just daily" → `python3 compute_rules.py --daily`
- "just intraday" → `python3 compute_rules.py --intraday`
- "just sentiment" → `python3 compute_rules.py --sentiment`
- "just report" → `python3 compute_rules.py --report`
- "skip data fetch" → add `--no-fetch`
