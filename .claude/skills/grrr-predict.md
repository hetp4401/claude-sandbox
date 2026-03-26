# grrr-predict

Run the GRRR Reddit-based next-day price prediction engine.

## Instructions

This is a deterministic prediction system that scores Reddit signals to predict GRRR's next trading day direction. Run it any time -- before market open, night before, or over the weekend.

Execute:

```bash
cd /home/user/claude-sandbox/stocks_analysis/grrr && python3 reddit_predict.py
```

Then read the output and present it to the user. The key things to highlight:

1. **The prediction** (direction + confidence %)
2. **Which factors are driving it** (show the factor breakdown table)
3. **Current Reddit conditions** (buzz level, sentiment, trend)
4. **What the historical rules say** about this specific setup

The 7 scored factors are:
- Weekend buzz (weight 3.0) -- weekend Reddit posts → Monday UP 100% historically
- Buzz volume (weight 1.5) -- post count in last 24 hours
- Avg polarity (weight 1.0) -- sentiment with CONTRARIAN flip (negative = bullish)
- Polarity trend (weight 1.5) -- is sentiment improving or deteriorating
- Day of week (weight 2.0) -- which day the buzz is on predicts next day
- Extreme ratio (weight 1.0) -- % of strongly opinionated posts
- Buzz acceleration (weight 1.0) -- is posting rate growing or shrinking

All thresholds are explicit numbers defined in the script. No ambiguity.
