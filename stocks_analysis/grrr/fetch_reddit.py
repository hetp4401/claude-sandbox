#!/usr/bin/env python3
"""
GRRR (Gorilla Technologies) Reddit Sentiment Scraper.

Monitors Reddit discussions about GRRR across stock subreddits.

Strategy:
  1. Google News RSS with site:reddit.com filter (works without Reddit auth)
  2. Google RSS search for "GRRR" + stock subreddit names
  3. yfinance news (often includes Reddit-sourced content)

Targets:
  r/wallstreetbets, r/stocks, r/pennystocks, r/smallstreetbets,
  r/investing, r/stockmarket, r/spacs, r/shortsqueeze, r/daytrading,
  r/squeezeplays, r/options, r/GRRR

Sentiment via TextBlob on titles and available text.

Outputs:
  - grrr_reddit_posts.csv:    All matched Reddit posts/discussions
  - grrr_reddit_daily.csv:    Daily aggregated sentiment
  - grrr_reddit_report.txt:   Human-readable report
"""

import os
import sys
import re
import time
import hashlib
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import feedparser
import requests
import pandas as pd
from textblob import TextBlob

TICKER = "GRRR"
COMPANY_TERMS = ["GRRR", "Gorilla Technologies", "Gorilla Technology"]

SUBREDDITS = [
    "wallstreetbets", "stocks", "pennystocks", "smallstreetbets",
    "investing", "stockmarket", "spacs", "shortsqueeze",
    "daytrading", "squeezeplays", "options", "GRRR",
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
REDDIT_POSTS = os.path.join(DATA_DIR, "grrr_reddit_posts.csv")
REDDIT_DAILY = os.path.join(DATA_DIR, "grrr_reddit_daily.csv")
REDDIT_REPORT = os.path.join(DATA_DIR, "grrr_reddit_report.txt")

LOOKBACK_DAYS = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def dedupe_key(title):
    clean = re.sub(r"[^a-z0-9 ]", "", title.lower().strip())
    return hashlib.md5(clean.encode()).hexdigest()


def analyze_sentiment(text):
    if not text or not text.strip():
        return 0.0, 0.0, "neutral"
    blob = TextBlob(text[:5000])
    pol = blob.sentiment.polarity
    subj = blob.sentiment.subjectivity
    if pol > 0.1:
        label = "positive"
    elif pol < -0.1:
        label = "negative"
    else:
        label = "neutral"
    return round(pol, 4), round(subj, 4), label


def extract_subreddit(text):
    """Try to extract subreddit name from title or link."""
    match = re.search(r'r/(\w+)', text)
    if match:
        return match.group(1)
    return "unknown"


def google_rss_search(query, days=30):
    """Search Google News RSS for Reddit GRRR results."""
    articles = []
    cutoff = datetime.now() - timedelta(days=days)
    encoded = quote_plus(query)
    url = f"https://news.google.com/rss/search?q={encoded}+when:{days}d&hl=en-US&gl=US&ceid=US:en"

    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            pub_date = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6])

            if pub_date and pub_date < cutoff:
                continue

            title = entry.get("title", "")
            link = entry.get("link", "")
            source = entry.get("source", {}).get("title", "")
            summary = entry.get("summary", "")

            articles.append({
                "title": title,
                "link": link,
                "published": pub_date.strftime("%Y-%m-%d %H:%M") if pub_date else "",
                "source": source,
                "summary": summary,
            })
    except Exception as e:
        print(f"    Google RSS error for '{query}': {e}")

    return articles


def try_reddit_direct():
    """Attempt direct Reddit JSON API. Returns list of posts or empty if blocked."""
    posts = []
    test_url = "https://www.reddit.com/r/stocks/search.json?q=GRRR&sort=new&t=month&limit=5"
    try:
        resp = requests.get(test_url, headers={
            "User-Agent": "GRRR-StockBot/1.0 (stock research)"
        }, timeout=10)
        if resp.status_code != 200:
            return None  # Reddit blocked

        data = resp.json()
        cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

        for sub in SUBREDDITS:
            url = (
                f"https://www.reddit.com/r/{sub}/search.json"
                f"?q={quote_plus(TICKER)}&restrict_sr=on&sort=new&t=month&limit=100"
            )
            try:
                r = requests.get(url, headers={
                    "User-Agent": "GRRR-StockBot/1.0 (stock research)"
                }, timeout=10)
                if r.status_code != 200:
                    continue
                children = r.json().get("data", {}).get("children", [])
                for child in children:
                    p = child.get("data", {})
                    created = datetime.fromtimestamp(p.get("created_utc", 0))
                    if created < cutoff:
                        continue
                    posts.append({
                        "title": p.get("title", ""),
                        "link": f"https://reddit.com{p.get('permalink', '')}",
                        "published": created.strftime("%Y-%m-%d %H:%M"),
                        "source": f"r/{sub}",
                        "subreddit": sub,
                        "summary": p.get("selftext", "")[:500],
                        "score": p.get("score", 0),
                        "num_comments": p.get("num_comments", 0),
                        "upvote_ratio": p.get("upvote_ratio", 0),
                        "author": p.get("author", "[deleted]"),
                        "flair": p.get("link_flair_text", ""),
                    })
                time.sleep(2)
            except Exception:
                continue
        return posts
    except Exception:
        return None


def fetch_reddit():
    """Master: try direct Reddit API first, fall back to Google RSS scraping."""
    os.makedirs(DATA_DIR, exist_ok=True)

    all_posts = []
    seen = set()
    method_used = ""

    print(f"[{datetime.now()}] Scraping Reddit for {TICKER} mentions...")
    print(f"  Lookback: {LOOKBACK_DAYS} days\n")

    # === Method 1: Try direct Reddit API ===
    print("  Trying direct Reddit API...")
    direct_posts = try_reddit_direct()
    if direct_posts is not None and len(direct_posts) > 0:
        method_used = "Reddit JSON API (direct)"
        print(f"  Direct API returned {len(direct_posts)} posts")
        for p in direct_posts:
            key = dedupe_key(p["title"])
            if key not in seen and p["title"].strip():
                seen.add(key)
                all_posts.append(p)
    else:
        print("  Direct Reddit API unavailable, falling back to Google RSS...")
        method_used = "Google News RSS (site:reddit.com)"

        # === Method 2: Google RSS with Reddit filter ===
        searches = [
            f"site:reddit.com GRRR stock",
            f"site:reddit.com GRRR Gorilla Technology",
            f"site:reddit.com GRRR wallstreetbets OR pennystocks OR stocks",
            f"site:reddit.com \"GRRR\" shares OR trading OR squeeze",
            f"GRRR reddit stock",
            f"Gorilla Technology GRRR reddit",
        ]

        # Also search per-subreddit
        for sub in ["wallstreetbets", "pennystocks", "stocks", "shortsqueeze", "smallstreetbets"]:
            searches.append(f"site:reddit.com/r/{sub} GRRR")

        for i, query in enumerate(searches):
            print(f"  [{i+1}/{len(searches)}] Searching: {query[:60]}...")
            results = google_rss_search(query, LOOKBACK_DAYS)
            for art in results:
                key = dedupe_key(art["title"])
                if key not in seen and art["title"].strip():
                    seen.add(key)
                    # Determine subreddit from link or title
                    subreddit = extract_subreddit(art.get("link", "") + " " + art.get("title", ""))
                    art["subreddit"] = subreddit
                    art["score"] = 0
                    art["num_comments"] = 0
                    art["upvote_ratio"] = 0
                    art["author"] = ""
                    art["flair"] = ""
                    all_posts.append(art)
            time.sleep(1)

        # Also pull any Reddit-sourced items from yfinance news
        print(f"  Checking yfinance for Reddit-sourced news...")
        try:
            import yfinance as yf
            t = yf.Ticker(TICKER)
            news = t.news or []
            for item in news:
                title = item.get("title", "")
                link = item.get("link", "")
                publisher = item.get("publisher", "")

                # Check nested content structure
                if not title and "content" in item:
                    c = item["content"]
                    title = c.get("title", "")
                    link = c.get("canonicalUrl", {}).get("url", "")
                    publisher = c.get("provider", {}).get("displayName", "")

                if "reddit" in (link + publisher + title).lower():
                    key = dedupe_key(title)
                    if key not in seen and title.strip():
                        seen.add(key)
                        all_posts.append({
                            "title": title,
                            "link": link,
                            "published": "",
                            "source": publisher,
                            "summary": "",
                            "subreddit": extract_subreddit(link + " " + title),
                            "score": 0,
                            "num_comments": 0,
                            "upvote_ratio": 0,
                            "author": "",
                            "flair": "",
                        })
        except Exception as e:
            print(f"    yfinance check error: {e}")

    print(f"\n  Total unique Reddit items: {len(all_posts)}")
    print(f"  Method: {method_used}")

    if not all_posts:
        print("  No Reddit discussions found. This could mean:")
        print("    - GRRR isn't heavily discussed on Reddit right now")
        print("    - Network restrictions are blocking all sources")
        # Write empty CSV so downstream doesn't break
        pd.DataFrame(columns=[
            "title", "link", "published", "source", "subreddit",
            "polarity", "subjectivity", "sentiment"
        ]).to_csv(REDDIT_POSTS, index=False)
        _write_empty_report()
        return True  # Not a failure, just no data

    # === Sentiment analysis ===
    print("  Analyzing sentiment...")
    for p in all_posts:
        text = f"{p['title']}. {p.get('summary', '')}" if p.get("summary") else p["title"]
        pol, subj, label = analyze_sentiment(text)
        p["polarity"] = pol
        p["subjectivity"] = subj
        p["sentiment"] = label
        p["weighted_polarity"] = round(pol * max(1, abs(p.get("score", 0))), 4)

    # Save posts
    df = pd.DataFrame(all_posts)
    df = df.sort_values("published", ascending=False).reset_index(drop=True)
    df.to_csv(REDDIT_POSTS, index=False)
    print(f"  Saved posts to {REDDIT_POSTS}")

    # === Daily aggregation ===
    df_dated = df[df["published"].str.len() > 0].copy()
    daily = pd.DataFrame()
    if not df_dated.empty:
        df_dated["date"] = pd.to_datetime(df_dated["published"]).dt.strftime("%Y-%m-%d")
        daily = df_dated.groupby("date").agg(
            total_items=("polarity", "count"),
            avg_polarity=("polarity", "mean"),
            weighted_avg=("weighted_polarity", "mean"),
            max_polarity=("polarity", "max"),
            min_polarity=("polarity", "min"),
            positive=("sentiment", lambda x: (x == "positive").sum()),
            negative=("sentiment", lambda x: (x == "negative").sum()),
            neutral=("sentiment", lambda x: (x == "neutral").sum()),
        ).round(4)
        daily["bull_bear_ratio"] = (
            (daily["positive"] - daily["negative"]) / daily["total_items"]
        ).round(4)
        daily = daily.sort_index(ascending=False)
        daily.to_csv(REDDIT_DAILY)
        print(f"  Saved daily summary to {REDDIT_DAILY}")

    # === Subreddit breakdown ===
    sub_stats = {}
    for sub in df["subreddit"].unique():
        sub_df = df[df["subreddit"] == sub]
        sub_stats[sub] = {
            "count": len(sub_df),
            "avg_pol": round(sub_df["polarity"].mean(), 4),
        }

    # === Generate report ===
    _write_report(df, daily, sub_stats, method_used, all_posts)

    return True


def _write_empty_report():
    """Write a report indicating no data found."""
    lines = [
        "=" * 65,
        "  GRRR REDDIT SENTIMENT REPORT",
        f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 65,
        "",
        "  No Reddit discussions found for GRRR in the last 30 days.",
        "  The stock may not be actively discussed on Reddit currently.",
        "",
        "=" * 65,
    ]
    with open(REDDIT_REPORT, "w") as f:
        f.write("\n".join(lines))


def _write_report(df, daily, sub_stats, method, all_posts):
    """Generate the human-readable report."""
    r = []
    r.append("=" * 65)
    r.append("  GRRR REDDIT SENTIMENT REPORT")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Lookback:  {LOOKBACK_DAYS} days")
    r.append(f"  Method:    {method}")
    r.append("=" * 65)
    r.append("")

    total = len(df)
    pos = (df["sentiment"] == "positive").sum()
    neg = (df["sentiment"] == "negative").sum()
    neu = (df["sentiment"] == "neutral").sum()
    avg_pol = df["polarity"].mean()
    wavg = df["weighted_polarity"].mean()

    r.append("OVERALL REDDIT SENTIMENT:")
    r.append(f"  Total discussions: {total}")
    r.append(f"  Positive:          {pos} ({pos/total*100:.1f}%)")
    r.append(f"  Negative:          {neg} ({neg/total*100:.1f}%)")
    r.append(f"  Neutral:           {neu} ({neu/total*100:.1f}%)")
    r.append(f"  Avg polarity:      {avg_pol:.4f}")
    r.append(f"  Weighted avg:      {wavg:.4f}")
    r.append("")

    if wavg > 0.15:
        gauge = "STRONGLY BULLISH"
    elif wavg > 0.05:
        gauge = "MILDLY BULLISH"
    elif wavg > -0.05:
        gauge = "NEUTRAL"
    elif wavg > -0.15:
        gauge = "MILDLY BEARISH"
    else:
        gauge = "STRONGLY BEARISH"
    r.append(f"  >>> REDDIT SENTIMENT GAUGE: {gauge} <<<")
    r.append("")

    # Subreddit breakdown
    if sub_stats:
        r.append("SUBREDDIT BREAKDOWN:")
        r.append("-" * 65)
        r.append(f"{'Subreddit':<25} {'Posts':>6} {'Avg Polarity':>13}")
        r.append("-" * 65)
        for sub, stats in sorted(sub_stats.items(), key=lambda x: -x[1]["count"]):
            r.append(f"r/{sub:<24} {stats['count']:>5} {stats['avg_pol']:>13.4f}")
        r.append("")

    # Daily
    if not daily.empty:
        r.append("DAILY BREAKDOWN:")
        r.append("-" * 65)
        r.append(f"{'Date':<12} {'Items':>6} {'Avg Pol':>9} {'Pos':>4} {'Neg':>4} {'Neu':>4}")
        r.append("-" * 65)
        for date, row in daily.iterrows():
            r.append(
                f"{date:<12} {int(row['total_items']):>6} {row['avg_polarity']:>9.4f} "
                f"{int(row['positive']):>4} {int(row['negative']):>4} {int(row['neutral']):>4}"
            )
        r.append("")

    # Most bullish
    if len(df) >= 3:
        r.append("MOST BULLISH DISCUSSIONS:")
        r.append("-" * 65)
        for _, row in df.nlargest(5, "polarity").iterrows():
            r.append(f"  [{row['polarity']:+.3f}] r/{row['subreddit']} | {row['title'][:60]}")
            r.append(f"           {row['published']}")
        r.append("")

        r.append("MOST BEARISH DISCUSSIONS:")
        r.append("-" * 65)
        for _, row in df.nsmallest(5, "polarity").iterrows():
            r.append(f"  [{row['polarity']:+.3f}] r/{row['subreddit']} | {row['title'][:60]}")
            r.append(f"           {row['published']}")
        r.append("")

    # Buzz trend
    if not daily.empty and len(daily) >= 7:
        recent = daily.head(7)["total_items"].mean()
        older = daily.iloc[7:]["total_items"].mean() if len(daily) > 7 else 0
        if older > 0:
            buzz = ((recent - older) / older) * 100
            r.append("BUZZ TREND (last 7 days vs prior):")
            r.append(f"  Recent avg/day: {recent:.1f}")
            r.append(f"  Prior avg/day:  {older:.1f}")
            r.append(f"  Change:         {buzz:+.1f}%")
            if abs(buzz) > 50:
                r.append(f"  >>> {'BUZZ SURGING' if buzz > 0 else 'BUZZ DYING DOWN'} <<<")
            r.append("")

    r.append("=" * 65)
    r.append("Sentiment: TextBlob (polarity -1 to +1)")
    r.append("=" * 65)

    report_text = "\n".join(r)
    with open(REDDIT_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {REDDIT_REPORT}")
    print(f"\n{report_text}")


if __name__ == "__main__":
    success = fetch_reddit()
    sys.exit(0 if success else 1)
