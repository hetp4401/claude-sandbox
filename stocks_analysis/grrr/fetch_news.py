#!/usr/bin/env python3
"""
GRRR (Gorilla Technologies) News Scraper & Sentiment Analyzer.

Sources:
  1. Yahoo Finance RSS (ticker-specific news)
  2. Google News RSS (search-based)
  3. yfinance built-in news feed

Sentiment analysis via TextBlob (polarity: -1 to +1, subjectivity: 0 to 1).
Outputs:
  - grrr_news_raw.csv:       All scraped articles with sentiment scores
  - grrr_news_summary.csv:   Daily aggregated sentiment
  - grrr_sentiment_report.txt: Human-readable summary
"""

import os
import sys
import re
import json
import hashlib
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import feedparser
import requests
import pandas as pd
from textblob import TextBlob

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

TICKER = "GRRR"
COMPANY = "Gorilla Technologies"
SEARCH_TERMS = [TICKER, COMPANY, "Gorilla Technology Group"]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
NEWS_RAW = os.path.join(DATA_DIR, "grrr_news_raw.csv")
NEWS_SUMMARY = os.path.join(DATA_DIR, "grrr_news_summary.csv")
SENTIMENT_REPORT = os.path.join(DATA_DIR, "grrr_sentiment_report.txt")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

LOOKBACK_DAYS = 30


def dedupe_key(title, source):
    """Create a dedup key from title."""
    clean = re.sub(r"[^a-z0-9 ]", "", title.lower().strip())
    return hashlib.md5(clean.encode()).hexdigest()


def analyze_sentiment(text):
    """Return (polarity, subjectivity, label) for a text string."""
    if not text or not text.strip():
        return 0.0, 0.0, "neutral"
    blob = TextBlob(text)
    pol = blob.sentiment.polarity
    subj = blob.sentiment.subjectivity
    if pol > 0.1:
        label = "positive"
    elif pol < -0.1:
        label = "negative"
    else:
        label = "neutral"
    return round(pol, 4), round(subj, 4), label


def fetch_google_news_rss(query, days=30):
    """Scrape Google News RSS for a search query."""
    articles = []
    cutoff = datetime.now() - timedelta(days=days)

    url = f"https://news.google.com/rss/search?q={quote_plus(query)}+when:{days}d&hl=en-US&gl=US&ceid=US:en"
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

            articles.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": pub_date.strftime("%Y-%m-%d %H:%M") if pub_date else "",
                "source": entry.get("source", {}).get("title", "Google News"),
                "summary": entry.get("summary", ""),
            })
    except Exception as e:
        print(f"  Google News RSS error for '{query}': {e}")

    return articles


def fetch_yahoo_finance_rss(ticker):
    """Scrape Yahoo Finance RSS for ticker news."""
    articles = []
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            pub_date = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6])

            if pub_date and pub_date < cutoff:
                continue

            articles.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": pub_date.strftime("%Y-%m-%d %H:%M") if pub_date else "",
                "source": "Yahoo Finance",
                "summary": entry.get("summary", ""),
            })
    except Exception as e:
        print(f"  Yahoo Finance RSS error: {e}")

    return articles


def fetch_yfinance_news(ticker):
    """Get news from yfinance's built-in news feed."""
    articles = []
    if not HAS_YFINANCE:
        return articles

    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    try:
        t = yf.Ticker(ticker)
        news = t.news
        if not news:
            return articles

        for item in news:
            pub_ts = item.get("providerPublishTime", 0)
            pub_date = datetime.fromtimestamp(pub_ts) if pub_ts else None
            if pub_date and pub_date < cutoff:
                continue

            title = item.get("title", "")
            link = item.get("link", "")
            source = item.get("publisher", "yfinance")

            # Some yfinance versions use 'content' structure
            if not title and "content" in item:
                content = item["content"]
                title = content.get("title", "")
                link = content.get("canonicalUrl", {}).get("url", "")
                source = content.get("provider", {}).get("displayName", "yfinance")
                pub_date_str = content.get("pubDate", "")
                if pub_date_str and not pub_date:
                    try:
                        pub_date = datetime.strptime(pub_date_str[:19], "%Y-%m-%dT%H:%M:%S")
                    except ValueError:
                        pass

            articles.append({
                "title": title,
                "link": link,
                "published": pub_date.strftime("%Y-%m-%d %H:%M") if pub_date else "",
                "source": source,
                "summary": item.get("summary", item.get("content", {}).get("summary", "")),
            })
    except Exception as e:
        print(f"  yfinance news error: {e}")

    return articles


def fetch_news():
    """Master function: scrape all sources, deduplicate, analyze sentiment."""
    os.makedirs(DATA_DIR, exist_ok=True)
    all_articles = []
    seen = set()

    print(f"[{datetime.now()}] Scraping news for {TICKER} ({COMPANY})...")
    print(f"  Lookback: {LOOKBACK_DAYS} days")

    # 1. yfinance news
    print("  [1/3] yfinance news feed...")
    yf_articles = fetch_yfinance_news(TICKER)
    print(f"         Found {len(yf_articles)} articles")
    all_articles.extend(yf_articles)

    # 2. Yahoo Finance RSS
    print("  [2/3] Yahoo Finance RSS...")
    yf_rss = fetch_yahoo_finance_rss(TICKER)
    print(f"         Found {len(yf_rss)} articles")
    all_articles.extend(yf_rss)

    # 3. Google News RSS for each search term
    print("  [3/3] Google News RSS...")
    for term in SEARCH_TERMS:
        gn = fetch_google_news_rss(term, LOOKBACK_DAYS)
        print(f"         '{term}': {len(gn)} articles")
        all_articles.extend(gn)

    # Deduplicate by title
    unique_articles = []
    for art in all_articles:
        key = dedupe_key(art["title"], art["source"])
        if key not in seen and art["title"].strip():
            seen.add(key)
            unique_articles.append(art)

    print(f"\n  Total unique articles: {len(unique_articles)}")

    if not unique_articles:
        print("WARNING: No news articles found.")
        # Write empty files
        pd.DataFrame().to_csv(NEWS_RAW, index=False)
        return False

    # Analyze sentiment for each article
    print("  Analyzing sentiment...")
    for art in unique_articles:
        # Combine title + summary for better sentiment signal
        text = f"{art['title']}. {art['summary']}" if art["summary"] else art["title"]
        pol, subj, label = analyze_sentiment(text)
        art["polarity"] = pol
        art["subjectivity"] = subj
        art["sentiment"] = label

    # Build DataFrame
    df = pd.DataFrame(unique_articles)
    df = df.sort_values("published", ascending=False).reset_index(drop=True)

    # Save raw news
    df.to_csv(NEWS_RAW, index=False)
    print(f"  Saved raw news to {NEWS_RAW}")

    # === Daily sentiment aggregation ===
    df_dated = df[df["published"].str.len() > 0].copy()
    if not df_dated.empty:
        df_dated["date"] = pd.to_datetime(df_dated["published"]).dt.strftime("%Y-%m-%d")
        daily = df_dated.groupby("date").agg(
            article_count=("title", "count"),
            avg_polarity=("polarity", "mean"),
            max_polarity=("polarity", "max"),
            min_polarity=("polarity", "min"),
            avg_subjectivity=("subjectivity", "mean"),
            positive_count=("sentiment", lambda x: (x == "positive").sum()),
            negative_count=("sentiment", lambda x: (x == "negative").sum()),
            neutral_count=("sentiment", lambda x: (x == "neutral").sum()),
        ).round(4)
        daily["sentiment_ratio"] = (
            (daily["positive_count"] - daily["negative_count"]) / daily["article_count"]
        ).round(4)
        daily = daily.sort_index(ascending=False)
        daily.to_csv(NEWS_SUMMARY)
        print(f"  Saved daily summary to {NEWS_SUMMARY}")
    else:
        daily = pd.DataFrame()

    # === Generate human-readable report ===
    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append(f"  GRRR NEWS SENTIMENT REPORT")
    report_lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report_lines.append(f"  Lookback:  {LOOKBACK_DAYS} days")
    report_lines.append("=" * 60)
    report_lines.append("")

    # Overall stats
    total = len(df)
    pos = (df["sentiment"] == "positive").sum()
    neg = (df["sentiment"] == "negative").sum()
    neu = (df["sentiment"] == "neutral").sum()
    avg_pol = df["polarity"].mean()

    report_lines.append(f"OVERALL SENTIMENT:")
    report_lines.append(f"  Total articles:  {total}")
    report_lines.append(f"  Positive:        {pos} ({pos/total*100:.1f}%)")
    report_lines.append(f"  Negative:        {neg} ({neg/total*100:.1f}%)")
    report_lines.append(f"  Neutral:         {neu} ({neu/total*100:.1f}%)")
    report_lines.append(f"  Avg polarity:    {avg_pol:.4f} ({'bullish' if avg_pol > 0.05 else 'bearish' if avg_pol < -0.05 else 'neutral'})")
    report_lines.append(f"  Avg subjectivity:{df['subjectivity'].mean():.4f}")
    report_lines.append("")

    # Sentiment gauge
    if avg_pol > 0.15:
        gauge = "STRONGLY BULLISH"
    elif avg_pol > 0.05:
        gauge = "MILDLY BULLISH"
    elif avg_pol > -0.05:
        gauge = "NEUTRAL"
    elif avg_pol > -0.15:
        gauge = "MILDLY BEARISH"
    else:
        gauge = "STRONGLY BEARISH"
    report_lines.append(f"  >>> SENTIMENT GAUGE: {gauge} <<<")
    report_lines.append("")

    # Daily breakdown
    if not daily.empty:
        report_lines.append("DAILY BREAKDOWN:")
        report_lines.append("-" * 60)
        report_lines.append(f"{'Date':<12} {'Articles':>8} {'Avg Pol':>9} {'Pos':>4} {'Neg':>4} {'Neu':>4} {'Ratio':>7}")
        report_lines.append("-" * 60)
        for date, row in daily.iterrows():
            report_lines.append(
                f"{date:<12} {int(row['article_count']):>8} {row['avg_polarity']:>9.4f} "
                f"{int(row['positive_count']):>4} {int(row['negative_count']):>4} "
                f"{int(row['neutral_count']):>4} {row['sentiment_ratio']:>7.4f}"
            )
        report_lines.append("")

    # Most positive & negative articles
    if len(df) > 0:
        report_lines.append("MOST POSITIVE ARTICLES:")
        report_lines.append("-" * 60)
        top_pos = df.nlargest(5, "polarity")
        for _, row in top_pos.iterrows():
            report_lines.append(f"  [{row['polarity']:+.3f}] {row['title'][:80]}")
            report_lines.append(f"          {row['published']} | {row['source']}")
        report_lines.append("")

        report_lines.append("MOST NEGATIVE ARTICLES:")
        report_lines.append("-" * 60)
        top_neg = df.nsmallest(5, "polarity")
        for _, row in top_neg.iterrows():
            report_lines.append(f"  [{row['polarity']:+.3f}] {row['title'][:80]}")
            report_lines.append(f"          {row['published']} | {row['source']}")
        report_lines.append("")

    report_lines.append("=" * 60)
    report_lines.append("Sources: yfinance, Yahoo Finance RSS, Google News RSS")
    report_lines.append("Sentiment: TextBlob (polarity -1 to +1)")
    report_lines.append("=" * 60)

    report_text = "\n".join(report_lines)

    with open(SENTIMENT_REPORT, "w") as f:
        f.write(report_text)

    print(f"  Saved report to {SENTIMENT_REPORT}")
    print(f"\n{report_text}")

    return True


if __name__ == "__main__":
    success = fetch_news()
    sys.exit(0 if success else 1)
