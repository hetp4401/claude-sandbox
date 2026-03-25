#!/usr/bin/env python3
"""
GRRR Options Flow & Unusual Activity Tracker.

Sources:
  - yfinance: full options chains (calls + puts) for all expirations
  - Derived: put/call ratios, unusual volume, open interest changes,
    max pain, implied move, options volume vs stock volume

Outputs:
  - grrr_options_chain.csv:     Full options chain snapshot
  - grrr_options_flow.csv:      Aggregated flow per expiration
  - grrr_options_report.txt:    Human-readable report
"""

import os
import sys
import numpy as np
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
CHAIN_FILE = os.path.join(DATA_DIR, "grrr_options_chain.csv")
FLOW_FILE = os.path.join(DATA_DIR, "grrr_options_flow.csv")
OPTIONS_REPORT = os.path.join(DATA_DIR, "grrr_options_report.txt")


def fetch_options():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Fetching options data for {TICKER}...")

    ticker = yf.Ticker(TICKER)

    # Get current price
    hist = ticker.history(period="2d")
    current_price = hist["Close"].iloc[-1] if not hist.empty else 0
    print(f"  Current price: ${current_price:.2f}")

    # Get all expiration dates
    try:
        expirations = ticker.options
    except Exception as e:
        print(f"  Could not get options expirations: {e}")
        _write_empty_report()
        return True

    if not expirations:
        print("  No options available for this ticker.")
        _write_empty_report()
        return True

    print(f"  Expirations available: {len(expirations)}")
    print(f"  Dates: {', '.join(expirations[:8])}{'...' if len(expirations) > 8 else ''}")

    all_chains = []
    flow_data = []

    for exp in expirations:
        try:
            chain = ticker.option_chain(exp)
            calls = chain.calls.copy()
            puts = chain.puts.copy()

            calls["type"] = "CALL"
            calls["expiration"] = exp
            puts["type"] = "PUT"
            puts["expiration"] = exp

            combined = pd.concat([calls, puts], ignore_index=True)
            all_chains.append(combined)

            # Aggregate flow for this expiration
            total_call_vol = calls["volume"].sum() if "volume" in calls.columns else 0
            total_put_vol = puts["volume"].sum() if "volume" in puts.columns else 0
            total_call_oi = calls["openInterest"].sum() if "openInterest" in calls.columns else 0
            total_put_oi = puts["openInterest"].sum() if "openInterest" in puts.columns else 0

            # Handle NaN
            total_call_vol = 0 if pd.isna(total_call_vol) else int(total_call_vol)
            total_put_vol = 0 if pd.isna(total_put_vol) else int(total_put_vol)
            total_call_oi = 0 if pd.isna(total_call_oi) else int(total_call_oi)
            total_put_oi = 0 if pd.isna(total_put_oi) else int(total_put_oi)

            pc_vol_ratio = total_put_vol / total_call_vol if total_call_vol > 0 else 0
            pc_oi_ratio = total_put_oi / total_call_oi if total_call_oi > 0 else 0

            # Max pain calculation
            max_pain = _calc_max_pain(calls, puts, current_price)

            # Implied move from ATM options
            implied_move = _calc_implied_move(calls, puts, current_price, exp)

            # Days to expiration
            exp_date = datetime.strptime(exp, "%Y-%m-%d")
            dte = (exp_date - datetime.now()).days

            flow_data.append({
                "expiration": exp,
                "dte": dte,
                "call_volume": total_call_vol,
                "put_volume": total_put_vol,
                "total_volume": total_call_vol + total_put_vol,
                "pc_vol_ratio": round(pc_vol_ratio, 4),
                "call_oi": total_call_oi,
                "put_oi": total_put_oi,
                "total_oi": total_call_oi + total_put_oi,
                "pc_oi_ratio": round(pc_oi_ratio, 4),
                "max_pain": max_pain,
                "implied_move_pct": implied_move,
            })

        except Exception as e:
            print(f"    Error fetching {exp}: {e}")
            continue

    if not all_chains:
        print("  No options chain data retrieved.")
        _write_empty_report()
        return True

    # Save full chain
    df_chain = pd.concat(all_chains, ignore_index=True)
    df_chain.to_csv(CHAIN_FILE, index=False)
    print(f"  Saved {len(df_chain)} options contracts to {CHAIN_FILE}")

    # Save flow
    df_flow = pd.DataFrame(flow_data)
    df_flow.to_csv(FLOW_FILE, index=False)
    print(f"  Saved {len(df_flow)} expirations to {FLOW_FILE}")

    # Find unusual activity
    unusual = _find_unusual_activity(df_chain, current_price)

    # Generate report
    _write_report(df_chain, df_flow, unusual, current_price, expirations)
    return True


def _calc_max_pain(calls, puts, current_price):
    """Calculate max pain strike (where option writers have minimum payout)."""
    try:
        strikes = sorted(set(
            calls["strike"].tolist() + puts["strike"].tolist()
        ))
        if not strikes:
            return 0

        min_pain = float("inf")
        max_pain_strike = 0

        for strike in strikes:
            call_pain = 0
            put_pain = 0

            for _, row in calls.iterrows():
                oi = row.get("openInterest", 0)
                if pd.isna(oi):
                    oi = 0
                if strike > row["strike"]:
                    call_pain += (strike - row["strike"]) * oi * 100

            for _, row in puts.iterrows():
                oi = row.get("openInterest", 0)
                if pd.isna(oi):
                    oi = 0
                if strike < row["strike"]:
                    put_pain += (row["strike"] - strike) * oi * 100

            total = call_pain + put_pain
            if total < min_pain:
                min_pain = total
                max_pain_strike = strike

        return max_pain_strike
    except Exception:
        return 0


def _calc_implied_move(calls, puts, current_price, exp):
    """Estimate implied move from ATM straddle price."""
    try:
        if current_price <= 0:
            return 0

        # Find ATM strike
        all_strikes = calls["strike"].tolist()
        if not all_strikes:
            return 0
        atm_strike = min(all_strikes, key=lambda x: abs(x - current_price))

        atm_call = calls[calls["strike"] == atm_strike]
        atm_put = puts[puts["strike"] == atm_strike]

        call_mid = 0
        put_mid = 0

        if not atm_call.empty:
            ask = atm_call.iloc[0].get("ask", 0) or 0
            bid = atm_call.iloc[0].get("bid", 0) or 0
            last = atm_call.iloc[0].get("lastPrice", 0) or 0
            call_mid = (ask + bid) / 2 if (ask > 0 and bid > 0) else last

        if not atm_put.empty:
            ask = atm_put.iloc[0].get("ask", 0) or 0
            bid = atm_put.iloc[0].get("bid", 0) or 0
            last = atm_put.iloc[0].get("lastPrice", 0) or 0
            put_mid = (ask + bid) / 2 if (ask > 0 and bid > 0) else last

        straddle = call_mid + put_mid
        implied_move = (straddle / current_price) * 100

        return round(implied_move, 2)
    except Exception:
        return 0


def _find_unusual_activity(df, current_price):
    """Identify contracts with unusual volume vs open interest."""
    unusual = []
    try:
        df_valid = df.dropna(subset=["volume", "openInterest"]).copy()
        df_valid = df_valid[df_valid["volume"] > 0]

        if df_valid.empty:
            return unusual

        # Flag: volume > 2x open interest (unusual)
        df_valid["vol_oi_ratio"] = df_valid["volume"] / df_valid["openInterest"].replace(0, 1)
        df_unusual = df_valid[df_valid["vol_oi_ratio"] > 2].copy()
        df_unusual = df_unusual.sort_values("volume", ascending=False)

        for _, row in df_unusual.head(20).iterrows():
            moneyness = "ITM" if (
                (row["type"] == "CALL" and row["strike"] < current_price) or
                (row["type"] == "PUT" and row["strike"] > current_price)
            ) else "OTM"

            unusual.append({
                "type": row["type"],
                "strike": row["strike"],
                "expiration": row["expiration"],
                "volume": int(row["volume"]),
                "open_interest": int(row["openInterest"]),
                "vol_oi_ratio": round(row["vol_oi_ratio"], 2),
                "last_price": row.get("lastPrice", 0),
                "implied_vol": row.get("impliedVolatility", 0),
                "moneyness": moneyness,
            })
    except Exception:
        pass

    return unusual


def _write_empty_report():
    lines = [
        "=" * 65,
        "  GRRR OPTIONS FLOW REPORT",
        f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 65,
        "",
        "  No options data available for GRRR.",
        "=" * 65,
    ]
    with open(OPTIONS_REPORT, "w") as f:
        f.write("\n".join(lines))


def _write_report(df_chain, df_flow, unusual, current_price, expirations):
    r = []
    r.append("=" * 65)
    r.append("  GRRR OPTIONS FLOW REPORT")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Stock Price: ${current_price:.2f}")
    r.append(f"  Expirations: {len(expirations)}")
    r.append("=" * 65)
    r.append("")

    # Overall put/call ratios
    total_call_vol = df_flow["call_volume"].sum()
    total_put_vol = df_flow["put_volume"].sum()
    total_call_oi = df_flow["call_oi"].sum()
    total_put_oi = df_flow["put_oi"].sum()
    pc_vol = total_put_vol / total_call_vol if total_call_vol > 0 else 0
    pc_oi = total_put_oi / total_call_oi if total_call_oi > 0 else 0

    r.append("OVERALL OPTIONS FLOW:")
    r.append(f"  Total call volume:       {total_call_vol:,}")
    r.append(f"  Total put volume:        {total_put_vol:,}")
    r.append(f"  Put/Call volume ratio:   {pc_vol:.3f}")
    r.append(f"  Total call OI:           {total_call_oi:,}")
    r.append(f"  Total put OI:            {total_put_oi:,}")
    r.append(f"  Put/Call OI ratio:       {pc_oi:.3f}")
    r.append("")

    # Sentiment from P/C ratio
    if pc_vol < 0.5:
        sentiment = "STRONGLY BULLISH (heavy call buying)"
    elif pc_vol < 0.7:
        sentiment = "BULLISH"
    elif pc_vol < 1.0:
        sentiment = "SLIGHTLY BULLISH"
    elif pc_vol < 1.3:
        sentiment = "NEUTRAL"
    elif pc_vol < 1.7:
        sentiment = "BEARISH"
    else:
        sentiment = "STRONGLY BEARISH (heavy put buying)"

    r.append(f"  >>> OPTIONS SENTIMENT: {sentiment} <<<")
    r.append("")

    # Per-expiration flow
    r.append("FLOW BY EXPIRATION:")
    r.append("-" * 65)
    r.append(f"{'Expiry':<12} {'DTE':>4} {'C.Vol':>7} {'P.Vol':>7} {'P/C':>5} "
             f"{'C.OI':>8} {'P.OI':>8} {'MaxPain':>8} {'Impl%':>6}")
    r.append("-" * 65)
    for _, row in df_flow.iterrows():
        r.append(
            f"{row['expiration']:<12} {row['dte']:>4} {row['call_volume']:>7,} "
            f"{row['put_volume']:>7,} {row['pc_vol_ratio']:>5.2f} "
            f"{row['call_oi']:>8,} {row['put_oi']:>8,} "
            f"${row['max_pain']:>7.2f} {row['implied_move_pct']:>5.1f}%"
        )
    r.append("")

    # Nearest expiry implied move
    if not df_flow.empty:
        nearest = df_flow.iloc[0]
        r.append(f"NEAREST EXPIRY IMPLIED MOVE ({nearest['expiration']}, {nearest['dte']} DTE):")
        r.append(f"  Implied move: +/-{nearest['implied_move_pct']:.1f}%")
        r.append(f"  Expected range: ${current_price * (1 - nearest['implied_move_pct']/100):.2f} "
                 f"- ${current_price * (1 + nearest['implied_move_pct']/100):.2f}")
        r.append(f"  Max pain: ${nearest['max_pain']:.2f}")
        mp_dist = ((nearest['max_pain'] - current_price) / current_price * 100) if current_price > 0 else 0
        r.append(f"  Max pain distance: {mp_dist:+.1f}% from current")
        r.append("")

    # Unusual activity
    if unusual:
        r.append("UNUSUAL OPTIONS ACTIVITY (volume > 2x open interest):")
        r.append("-" * 65)
        r.append(f"{'Type':<5} {'Strike':>7} {'Expiry':<12} {'Vol':>6} {'OI':>6} "
                 f"{'V/OI':>5} {'IV':>6} {'$':>6} {'Money':>4}")
        r.append("-" * 65)
        for u in unusual:
            iv_str = f"{u['implied_vol']*100:.0f}%" if u['implied_vol'] else "N/A"
            r.append(
                f"{u['type']:<5} ${u['strike']:>6.2f} {u['expiration']:<12} "
                f"{u['volume']:>6,} {u['open_interest']:>6,} "
                f"{u['vol_oi_ratio']:>5.1f} {iv_str:>6} "
                f"${u['last_price']:>5.2f} {u['moneyness']:>4}"
            )
        r.append("")

        # Interpret unusual activity
        call_unusual = [u for u in unusual if u["type"] == "CALL"]
        put_unusual = [u for u in unusual if u["type"] == "PUT"]
        r.append(f"  Unusual calls: {len(call_unusual)}  |  Unusual puts: {len(put_unusual)}")
        if len(call_unusual) > len(put_unusual) * 1.5:
            r.append("  >>> SMART MONEY SIGNAL: BULLISH (unusual call activity) <<<")
        elif len(put_unusual) > len(call_unusual) * 1.5:
            r.append("  >>> SMART MONEY SIGNAL: BEARISH (unusual put activity) <<<")
        else:
            r.append("  >>> SMART MONEY SIGNAL: MIXED <<<")
        r.append("")

    r.append("=" * 65)
    r.append("Sources: yfinance options chain")
    r.append("=" * 65)

    report_text = "\n".join(r)
    with open(OPTIONS_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {OPTIONS_REPORT}")
    print(f"\n{report_text}")


if __name__ == "__main__":
    success = fetch_options()
    sys.exit(0 if success else 1)
