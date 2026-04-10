#!/usr/bin/env python3
"""
GRRR Reddit-Based Next-Day Predictor.

Deterministic scoring system using Reddit signals to predict tomorrow's price.
Every threshold, weight, and bucket is explicitly defined with numbers.

Factors scored (each with defined numeric thresholds):
  1. Weekend buzz volume (post count in last 48h if weekend)
  2. Weekday buzz volume (post count in last 24h)
  3. Sentiment polarity (avg TextBlob score)
  4. Sentiment trend (current 24h avg vs prior 24h avg)
  5. Posting day-of-week (which day Reddit is buzzing)
  6. Sentiment extremes (% of posts that are strongly positive or negative)
  7. Buzz acceleration (is posting rate increasing or decreasing)

Scoring:
  Each factor contributes a weighted score from -1.0 to +1.0.
  Composite score determines direction + confidence.

Outputs:
  - grrr_reddit_prediction.txt:  Human-readable prediction
  - grrr_reddit_prediction.json: Machine-readable for downstream use
"""

import os
import sys
import json
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import feedparser
import pandas as pd
import numpy as np
from textblob import TextBlob

try:
    import yfinance as yf
except ImportError:
    yf = None

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PREDICTION_FILE = os.path.join(DATA_DIR, "grrr_reddit_prediction.txt")
PREDICTION_JSON = os.path.join(DATA_DIR, "grrr_reddit_prediction.json")

# ================================================================
# THRESHOLDS (all explicitly defined)
# ================================================================

# Buzz volume: post count thresholds
BUZZ_THRESHOLDS = {
    "dead":     (0, 1),      # 0-1 posts in window
    "quiet":    (2, 3),      # 2-3 posts
    "moderate": (4, 7),      # 4-7 posts
    "high":     (8, 14),     # 8-14 posts
    "viral":    (15, 9999),  # 15+ posts
}

# Sentiment polarity buckets (TextBlob: -1.0 to +1.0)
POLARITY_THRESHOLDS = {
    "strong_negative": (-1.0, -0.25),
    "negative":        (-0.25, -0.08),
    "neutral":         (-0.08, 0.08),
    "positive":        (0.08, 0.25),
    "strong_positive": (0.25, 1.0),
}

# Sentiment trend: difference between current 24h avg and prior 24h avg
TREND_THRESHOLDS = {
    "crashing":    (-1.0, -0.15),   # sentiment dropping fast
    "declining":   (-0.15, -0.05),
    "stable":      (-0.05, 0.05),
    "improving":   (0.05, 0.15),
    "surging":     (0.15, 1.0),     # sentiment improving fast
}

# ================================================================
# WEIGHTS (how much each factor matters)
# ================================================================
WEIGHTS = {
    "weekend_buzz":       3.0,   # Strongest signal (100% hit rate historically)
    "mean_reversion":     2.5,   # GRRR is a mean-reversion stock - this matters most after big moves
    "buzz_volume":        1.5,   # More posts = more attention
    "avg_polarity":       1.0,   # What sentiment says
    "polarity_trend":     1.5,   # Direction sentiment is moving
    "day_of_week":        2.0,   # Which day posts are on
    "extreme_ratio":      1.0,   # % of strongly opinionated posts
    "buzz_acceleration":  1.0,   # Is buzz growing or shrinking
}

# ================================================================
# DAY-OF-WEEK SCORES (from association rules)
# Based on: "Reddit posts on day X → next day direction"
# ================================================================
DOW_SCORES = {
    # day_posted: (score, reason)
    # Positive = next day UP, negative = next day DOWN
    "Saturday":  (+1.0, "100% → Monday UP (21/21)"),
    "Sunday":    (+1.0, "100% → Monday UP (14/14)"),
    "Monday":    (-0.4, "63% → Tuesday DOWN (17/27)"),
    "Tuesday":   (-0.7, "82% → Wednesday DOWN (28/34)"),
    "Wednesday": (+0.6, "82% → Thursday UP (27/33)"),
    "Thursday":  (-0.4, "63% → Friday DOWN (17/27)"),
    "Friday":    (+0.3, "71% → Monday UP (15/21)"),
}

# ================================================================
# CONTRARIAN RULES (from news sentiment analysis)
# Negative sentiment → next day UP 86% (18/21)
# ================================================================
CONTRARIAN_BONUS = {
    "strong_negative": +0.4,  # Very negative = strong bounce signal
    "negative":        +0.2,  # Negative = moderate bounce signal
    "neutral":         0.0,
    "positive":        -0.1,  # Positive = slight contrarian drag
    "strong_positive": -0.2,  # Very positive = potential exhaustion
}


def bucket_value(value, thresholds):
    """Assign a value to a named bucket based on thresholds."""
    for name, (low, high) in thresholds.items():
        if low <= value < high:
            return name
    return list(thresholds.keys())[-1]


def scrape_reddit_now():
    """Scrape fresh Reddit data right now. Returns list of post dicts."""
    print("  Scraping fresh Reddit data...")
    posts = []
    seen = set()

    searches = [
        "site:reddit.com GRRR stock",
        "site:reddit.com GRRR Gorilla Technology",
        "site:reddit.com GRRR wallstreetbets OR pennystocks OR stocks",
        'site:reddit.com "GRRR" shares OR trading OR squeeze',
        "GRRR reddit stock",
        "Gorilla Technology GRRR reddit",
    ]
    for sub in ["wallstreetbets", "pennystocks", "stocks", "shortsqueeze", "smallstreetbets"]:
        searches.append(f"site:reddit.com/r/{sub} GRRR")

    for query in searches:
        try:
            url = f"https://news.google.com/rss/search?q={quote_plus(query)}+when:7d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            for entry in feed.entries:
                title = entry.get("title", "")
                if not title.strip():
                    continue

                import hashlib
                key = hashlib.md5(title.lower().encode()).hexdigest()
                if key in seen:
                    continue
                seen.add(key)

                pub_date = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    pub_date = datetime(*entry.published_parsed[:6])

                blob = TextBlob(title)
                posts.append({
                    "title": title,
                    "published": pub_date,
                    "polarity": round(blob.sentiment.polarity, 4),
                    "subjectivity": round(blob.sentiment.subjectivity, 4),
                    "source": entry.get("source", {}).get("title", ""),
                })
        except Exception as e:
            pass

    # Also try yfinance news for Reddit-sourced items
    if yf:
        try:
            t = yf.Ticker(TICKER)
            news = t.news or []
            for item in news:
                title = item.get("title", "")
                link = item.get("link", "")
                publisher = item.get("publisher", "")
                if not title and "content" in item:
                    c = item["content"]
                    title = c.get("title", "")
                    link = c.get("canonicalUrl", {}).get("url", "")
                    publisher = c.get("provider", {}).get("displayName", "")

                combined = (link + publisher + title).lower()
                if "reddit" not in combined:
                    continue

                import hashlib
                key = hashlib.md5(title.lower().encode()).hexdigest()
                if key in seen:
                    continue
                seen.add(key)

                pub_ts = item.get("providerPublishTime", 0)
                pub_date = datetime.fromtimestamp(pub_ts) if pub_ts else None

                blob = TextBlob(title)
                posts.append({
                    "title": title,
                    "published": pub_date,
                    "polarity": round(blob.sentiment.polarity, 4),
                    "subjectivity": round(blob.sentiment.subjectivity, 4),
                    "source": publisher,
                })
        except Exception:
            pass

    print(f"  Scraped {len(posts)} Reddit posts")
    return posts


def load_cached_reddit():
    """Load previously scraped Reddit data as fallback."""
    path = os.path.join(DATA_DIR, "grrr_reddit_posts.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        posts = []
        for _, row in df.iterrows():
            pub = row.get("published", "")
            try:
                pub_date = pd.to_datetime(pub) if pub and str(pub) != "nan" else None
                if hasattr(pub_date, 'to_pydatetime'):
                    pub_date = pub_date.to_pydatetime().replace(tzinfo=None)
            except Exception:
                pub_date = None
            posts.append({
                "title": row.get("title", ""),
                "published": pub_date,
                "polarity": row.get("polarity", 0),
                "subjectivity": row.get("subjectivity", 0),
                "source": row.get("source", ""),
            })
        return posts
    return []


def compute_prediction(posts):
    """Score all factors and produce a prediction."""
    now = datetime.now()
    today_dow = now.strftime("%A")

    scores = {}  # factor_name: (raw_score, weight, detail)

    # Filter to posts with timestamps
    dated = [p for p in posts if p["published"] is not None]
    all_polarities = [p["polarity"] for p in posts]

    # ================================================================
    # FACTOR 1: WEEKEND BUZZ (weight 3.0)
    # ================================================================
    is_weekend = now.weekday() >= 5
    is_sunday_night = now.weekday() == 6 and now.hour >= 18
    is_monday_premarket = now.weekday() == 0 and now.hour < 10

    weekend_start = now - timedelta(days=now.weekday()) if now.weekday() < 5 else now - timedelta(days=now.weekday() - 5)
    if now.weekday() >= 5:
        weekend_start = now - timedelta(days=now.weekday() - 5)
    else:
        # Look at last weekend (Sat-Sun)
        days_since_sat = (now.weekday() + 2) % 7
        weekend_start = now - timedelta(days=days_since_sat)

    weekend_end = weekend_start + timedelta(days=2)
    weekend_posts = [p for p in dated if p["published"] and
                     weekend_start.date() <= p["published"].date() <= weekend_end.date()]
    weekend_count = len(weekend_posts)

    if is_weekend or is_sunday_night or is_monday_premarket:
        # Weekend buzz is ACTIVE right now
        weekend_buzz = bucket_value(weekend_count, BUZZ_THRESHOLDS)
        if weekend_count >= 8:
            raw = +1.0  # Historical: 100% UP with high weekend buzz
        elif weekend_count >= 4:
            raw = +0.7
        elif weekend_count >= 2:
            raw = +0.4
        else:
            raw = 0.0
        detail = f"Weekend posts: {weekend_count} ({weekend_buzz}). Historical: weekend buzz → Monday UP 100% (35/35)"
    else:
        raw = 0.0
        detail = f"Not weekend/Monday pre-market. Weekend posts were: {weekend_count}"

    scores["weekend_buzz"] = (raw, WEIGHTS["weekend_buzz"], detail)

    # ================================================================
    # FACTOR 1B: MEAN REVERSION (weight 2.5)
    # GRRR is a mean-reversion stock. After big drops → bounce.
    # After big rallies → fade. This overrides trend signals.
    # ================================================================
    try:
        ticker_data = yf.Ticker(TICKER)
        hist = ticker_data.history(period="10d", interval="1d")
        if len(hist) >= 2:
            today_close = hist["Close"].iloc[-1]
            today_open = hist["Open"].iloc[-1]
            today_ret = (today_close - today_open) / today_open * 100
            prev_close = hist["Close"].iloc[-2]
            gap_today = (today_open - prev_close) / prev_close * 100

            # EMA5 for fast trend (not SMA10 which lags)
            ema5_series = hist["Close"].ewm(span=5, adjust=False).mean()
            ema5 = ema5_series.iloc[-1]
            ema5_dist = (today_close - ema5) / ema5 * 100

            # Trend breaking detection
            trend_breaking = (today_ret < -3 and abs(ema5_dist) < 2) or today_ret < -5

            # Mean reversion scoring
            if today_ret < -5:
                raw = +0.9  # Huge drop = strong bounce (big_down + wide range → gap UP 80%)
                detail = f"BIG DROP {today_ret:+.1f}% → STRONG bounce signal. Rule: big_down+wide→gap UP 80% (4/5)"
            elif today_ret < -3:
                raw = +0.6  # Large drop = bounce
                detail = f"LARGE DROP {today_ret:+.1f}% → bounce signal. Rule: mod_down→UP 83% (10/12)"
            elif today_ret < -1:
                raw = +0.3  # Moderate drop
                detail = f"Mod drop {today_ret:+.1f}% → mild bounce signal"
            elif today_ret > 5:
                raw = -0.7  # Huge rally = fade
                detail = f"HUGE RALLY {today_ret:+.1f}% → fade signal. Rule: strong_up→DOWN 94% (73/78)"
            elif today_ret > 3:
                raw = -0.4  # Large rally = fade
                detail = f"Large rally {today_ret:+.1f}% → fade signal"
            elif today_ret > 1:
                raw = -0.1
                detail = f"Mild rally {today_ret:+.1f}% → slight fade"
            else:
                raw = 0.0
                detail = f"Flat day {today_ret:+.1f}% → no reversion signal"

            # Override: if trend is breaking, boost bounce signal
            if trend_breaking and raw > 0:
                raw = min(raw + 0.2, 1.0)
                detail += f" [TREND BREAKING: EMA5 dist {ema5_dist:+.1f}%]"

            # Add gap context
            if gap_today > 3:
                raw -= 0.3  # Big gap up today = already bounced, less upside tomorrow
                detail += f" [Gapped up {gap_today:+.1f}% today - some bounce used up]"
            elif gap_today < -2:
                raw += 0.2  # Gap down = more bounce fuel
                detail += f" [Gapped down {gap_today:+.1f}% - extra bounce fuel]"
        else:
            raw = 0.0
            detail = "Not enough price data"
    except Exception as e:
        raw = 0.0
        detail = f"Price data error: {e}"

    scores["mean_reversion"] = (raw, WEIGHTS["mean_reversion"], detail)

    # ================================================================
    # FACTOR 2: BUZZ VOLUME last 24h (weight 1.5)
    # ================================================================
    cutoff_24h = now - timedelta(hours=24)
    recent_24h = [p for p in dated if p["published"] and p["published"] >= cutoff_24h]
    buzz_24h = len(recent_24h)
    buzz_bucket = bucket_value(buzz_24h, BUZZ_THRESHOLDS)

    # More buzz on a downtrend stock = contrarian bullish
    # More buzz on an uptrend stock = potential exhaustion
    if buzz_24h >= 8:
        raw = +0.3  # High buzz → moderate_buzz rule: 70% next day UP
    elif buzz_24h >= 4:
        raw = +0.2
    elif buzz_24h >= 2:
        raw = +0.1
    else:
        raw = -0.1  # Dead quiet = no catalyst

    scores["buzz_volume"] = (raw, WEIGHTS["buzz_volume"],
                              f"Posts in last 24h: {buzz_24h} ({buzz_bucket}). "
                              f"Rule: moderate_buzz → next day UP 70% (30/43)")

    # ================================================================
    # FACTOR 3: AVERAGE POLARITY (weight 1.0)
    # ================================================================
    if recent_24h:
        avg_pol = np.mean([p["polarity"] for p in recent_24h])
    elif all_polarities:
        avg_pol = np.mean(all_polarities)
    else:
        avg_pol = 0.0

    pol_bucket = bucket_value(avg_pol, POLARITY_THRESHOLDS)
    contrarian = CONTRARIAN_BONUS.get(pol_bucket, 0.0)

    # Direct sentiment + contrarian adjustment
    # Negative news → next day UP 86% is our strongest contrarian rule
    raw = contrarian
    scores["avg_polarity"] = (raw, WEIGHTS["avg_polarity"],
                               f"Avg polarity: {avg_pol:.4f} ({pol_bucket}). "
                               f"Contrarian rule: negative → next day UP 86% (18/21). "
                               f"Contrarian bonus: {contrarian:+.1f}")

    # ================================================================
    # FACTOR 4: POLARITY TREND (weight 1.5)
    # ================================================================
    cutoff_48h = now - timedelta(hours=48)
    recent_window = [p for p in dated if p["published"] and p["published"] >= cutoff_24h]
    prior_window = [p for p in dated if p["published"] and cutoff_48h <= p["published"] < cutoff_24h]

    if len(recent_window) >= 2 and len(prior_window) >= 2:
        recent_avg = np.mean([p["polarity"] for p in recent_window])
        prior_avg = np.mean([p["polarity"] for p in prior_window])
        trend_diff = recent_avg - prior_avg
        trend_bucket = bucket_value(trend_diff, TREND_THRESHOLDS)

        # Deteriorating sentiment → next day UP 86% (contrarian)
        if trend_diff < -0.15:
            raw = +0.5  # Crashing sentiment = strong bounce
        elif trend_diff < -0.05:
            raw = +0.3  # Declining = moderate bounce
        elif trend_diff < 0.05:
            raw = 0.0   # Stable
        elif trend_diff < 0.15:
            raw = -0.1  # Improving = less contrarian edge
        else:
            raw = -0.2  # Surging positive = potential exhaustion

        detail = (f"Trend: {trend_diff:+.4f} ({trend_bucket}). "
                  f"Recent 24h avg: {recent_avg:.4f} ({len(recent_window)} posts), "
                  f"Prior 24h avg: {prior_avg:.4f} ({len(prior_window)} posts). "
                  f"Rule: deteriorating → next day UP 86%")
    else:
        raw = 0.0
        trend_diff = 0.0
        trend_bucket = "unknown"
        detail = f"Not enough data for trend (need 2+ posts in each 24h window). Recent: {len(recent_window)}, Prior: {len(prior_window)}"

    scores["polarity_trend"] = (raw, WEIGHTS["polarity_trend"], detail)

    # ================================================================
    # FACTOR 5: DAY OF WEEK (weight 2.0)
    # ================================================================
    # What day is the buzz happening? That determines NEXT day direction
    dow_info = DOW_SCORES.get(today_dow, (0.0, "No data"))
    raw = dow_info[0]
    scores["day_of_week"] = (raw, WEIGHTS["day_of_week"],
                              f"Today is {today_dow}. Score: {raw:+.1f}. {dow_info[1]}")

    # ================================================================
    # FACTOR 6: EXTREME SENTIMENT RATIO (weight 1.0)
    # ================================================================
    if recent_24h:
        strong_neg = sum(1 for p in recent_24h if p["polarity"] < -0.25)
        strong_pos = sum(1 for p in recent_24h if p["polarity"] > 0.25)
        extreme_ratio = (strong_neg + strong_pos) / len(recent_24h)
        neg_ratio = strong_neg / len(recent_24h)

        # High % of strongly negative posts = stronger contrarian bounce
        if neg_ratio > 0.3:
            raw = +0.4
        elif extreme_ratio > 0.3:
            raw = +0.1  # Mixed extremes = volatile
        else:
            raw = 0.0

        detail = (f"Extreme posts: {strong_neg} strong neg, {strong_pos} strong pos "
                  f"out of {len(recent_24h)} ({extreme_ratio*100:.0f}% extreme). "
                  f"Strong neg ratio: {neg_ratio*100:.0f}%")
    else:
        raw = 0.0
        detail = "No recent posts to measure extremes"

    scores["extreme_ratio"] = (raw, WEIGHTS["extreme_ratio"], detail)

    # ================================================================
    # FACTOR 7: BUZZ ACCELERATION (weight 1.0)
    # ================================================================
    cutoff_12h = now - timedelta(hours=12)
    recent_12h = len([p for p in dated if p["published"] and p["published"] >= cutoff_12h])
    prior_12h = len([p for p in dated if p["published"] and cutoff_24h <= p["published"] < cutoff_12h])

    if prior_12h > 0:
        accel = (recent_12h - prior_12h) / prior_12h
    elif recent_12h > 0:
        accel = 1.0
    else:
        accel = 0.0

    if accel > 0.5:
        raw = +0.2  # Buzz growing = attention building
    elif accel < -0.5:
        raw = -0.1  # Buzz dying = fading interest
    else:
        raw = 0.0

    scores["buzz_acceleration"] = (raw, WEIGHTS["buzz_acceleration"],
                                    f"Last 12h: {recent_12h} posts, Prior 12h: {prior_12h} posts. "
                                    f"Acceleration: {accel:+.1%}")

    # ================================================================
    # COMPUTE COMPOSITE
    # ================================================================
    weighted_sum = sum(s[0] * s[1] for s in scores.values())
    total_weight = sum(s[1] for s in scores.values())
    composite = weighted_sum / total_weight if total_weight > 0 else 0

    # Direction + confidence
    if composite > 0.35:
        direction = "STRONG BUY"
        emoji = ">>>"
    elif composite > 0.15:
        direction = "BUY"
        emoji = ">>"
    elif composite > 0.05:
        direction = "LEAN BUY"
        emoji = ">"
    elif composite > -0.05:
        direction = "NEUTRAL"
        emoji = "="
    elif composite > -0.15:
        direction = "LEAN SELL"
        emoji = "<"
    elif composite > -0.35:
        direction = "SELL"
        emoji = "<<"
    else:
        direction = "STRONG SELL"
        emoji = "<<<"

    confidence = min(abs(composite) / 0.5 * 100, 100)

    # Tomorrow's day
    if now.weekday() == 4:  # Friday
        next_day = "Monday"
    elif now.weekday() >= 5:  # Weekend
        next_day = "Monday"
    else:
        next_day = (now + timedelta(days=1)).strftime("%A")

    return {
        "direction": direction,
        "confidence": round(confidence, 1),
        "composite": round(composite, 4),
        "next_day": next_day,
        "today": today_dow,
        "scores": scores,
        "stats": {
            "total_posts": len(posts),
            "posts_24h": buzz_24h,
            "posts_weekend": weekend_count,
            "avg_polarity": round(avg_pol, 4),
            "trend_diff": round(trend_diff, 4) if trend_diff else 0,
            "pol_bucket": pol_bucket,
            "trend_bucket": trend_bucket,
            "buzz_bucket": buzz_bucket,
        }
    }


def format_report(pred):
    """Format prediction as human-readable report."""
    r = []
    r.append("=" * 70)
    r.append("  GRRR REDDIT-BASED NEXT-DAY PREDICTION")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Today: {pred['today']} | Predicting: {pred['next_day']}")
    r.append("=" * 70)
    r.append("")

    r.append(f"  ╔════════════════════════════════════════════════╗")
    r.append(f"  ║  PREDICTION:  {pred['direction']:<20}               ║")
    r.append(f"  ║  CONFIDENCE:  {pred['confidence']:>5.1f}%                          ║")
    r.append(f"  ║  COMPOSITE:   {pred['composite']:>+6.4f}                         ║")
    r.append(f"  ╚════════════════════════════════════════════════╝")
    r.append("")

    # Stats
    s = pred["stats"]
    r.append("REDDIT STATS:")
    r.append(f"  Total posts scraped:    {s['total_posts']}")
    r.append(f"  Posts in last 24h:      {s['posts_24h']} ({s['buzz_bucket']})")
    r.append(f"  Weekend posts:          {s['posts_weekend']}")
    r.append(f"  Avg polarity:           {s['avg_polarity']:+.4f} ({s['pol_bucket']})")
    r.append(f"  Sentiment trend:        {s['trend_diff']:+.4f} ({s['trend_bucket']})")
    r.append("")

    # Factor breakdown
    r.append("FACTOR BREAKDOWN:")
    r.append("-" * 70)
    r.append(f"  {'Factor':<22} {'Raw':>6} {'Weight':>7} {'Score':>7}  Detail")
    r.append("-" * 70)

    total_weighted = 0
    total_weight = 0
    for name, (raw, weight, detail) in pred["scores"].items():
        score = raw * weight
        total_weighted += score
        total_weight += weight
        bar = "+" * int(abs(raw) * 10) if raw > 0 else "-" * int(abs(raw) * 10)
        r.append(f"  {name:<22} {raw:>+5.2f}  {weight:>6.1f}  {score:>+6.3f}  {detail[:45]}")
        if len(detail) > 45:
            r.append(f"  {'':>44} {detail[45:90]}")
            if len(detail) > 90:
                r.append(f"  {'':>44} {detail[90:135]}")
    r.append("-" * 70)
    r.append(f"  {'COMPOSITE':<22} {'':>6}  {total_weight:>6.1f}  {total_weighted:>+6.3f}")
    r.append(f"  {'NORMALIZED':<22} {'':>6}  {'':>6}  {pred['composite']:>+6.4f}")
    r.append("")

    # Threshold reference
    r.append("SCORING THRESHOLDS:")
    r.append(f"  Buzz:      dead(0-1), quiet(2-3), moderate(4-7), high(8-14), viral(15+)")
    r.append(f"  Polarity:  strong_neg(<-0.25), neg(-0.25 to -0.08), neutral(-0.08 to 0.08),")
    r.append(f"             pos(0.08 to 0.25), strong_pos(>0.25)")
    r.append(f"  Trend:     crashing(<-0.15), declining(-0.15 to -0.05), stable(-0.05 to 0.05),")
    r.append(f"             improving(0.05 to 0.15), surging(>0.15)")
    r.append("")

    # Historical basis
    r.append("RULES THIS IS BASED ON:")
    r.append(f"  Weekend posts → Monday UP:           100% (35/35)")
    r.append(f"  Weekend + high buzz → gap up + rip:   100% (23/23)")
    r.append(f"  Negative sentiment → next day UP:     86%  (18/21)")
    r.append(f"  Deteriorating trend → next day UP:    86%  (18/21)")
    r.append(f"  Tuesday posts → next day DOWN:        82%  (28/34)")
    r.append(f"  Wednesday posts → next day UP:        82%  (27/33)")
    r.append(f"  Moderate buzz → next day UP:          70%  (30/43)")
    r.append("")

    r.append("=" * 70)
    r.append("  NOT financial advice. Based on 60-day historical patterns.")
    r.append("=" * 70)

    return "\n".join(r)


def run_prediction():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] GRRR Reddit Prediction Engine")

    # Try fresh scrape first, fall back to cached
    posts = scrape_reddit_now()
    if len(posts) < 3:
        print("  Fresh scrape returned few results, loading cached data...")
        cached = load_cached_reddit()
        posts.extend(cached)
        # Deduplicate
        seen = set()
        unique = []
        for p in posts:
            key = p["title"][:50]
            if key not in seen:
                seen.add(key)
                unique.append(p)
        posts = unique

    print(f"  Total posts for analysis: {len(posts)}")

    # Compute prediction
    pred = compute_prediction(posts)

    # Format and save report
    report = format_report(pred)
    with open(PREDICTION_FILE, "w") as f:
        f.write(report)

    # Save JSON for programmatic use
    json_data = {
        "generated": datetime.now().isoformat(),
        "direction": pred["direction"],
        "confidence": pred["confidence"],
        "composite": pred["composite"],
        "next_day": pred["next_day"],
        "today": pred["today"],
        "stats": pred["stats"],
        "factor_scores": {
            name: {"raw": raw, "weight": weight, "weighted": round(raw * weight, 4)}
            for name, (raw, weight, _) in pred["scores"].items()
        },
    }
    with open(PREDICTION_JSON, "w") as f:
        json.dump(json_data, f, indent=2)

    print(f"\n{report}")
    return True


if __name__ == "__main__":
    run_prediction()
