"""
Stock Market Analyzer — Revamped
==================================
Covers all major US sectors via the Yahoo Finance screener.
No slow historical downloads — all metrics derived from screener data.

Outputs to output-YYYY-MM-DD/:
  - growing_sectors.csv   : sectors with positive avg 52-week return
  - growing_stocks.csv    : individual stocks with positive 52-week return
  - money_flow_sectors.csv: sectors ranked by avg daily dollar volume
  - potential_stocks.csv  : stocks with strong momentum + EPS growth + fair valuation

References:
  - yfinance docs: https://ranaroussi.github.io/yfinance
  - GitHub: https://github.com/ranaroussi/yfinance
"""

import yfinance as yf
from yfinance import EquityQuery
import pandas as pd
from datetime import datetime
import os
import sys
import io

# Fix encoding for Windows terminals
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------
TODAY = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = f"output-{TODAY}"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Known Yahoo Finance sector keys (used in EquityQuery filters)
# ---------------------------------------------------------------------------
SECTORS = [
    "Technology",
    "Financial Services",
    "Healthcare",
    "Consumer Cyclical",
    "Industrials",
    "Communication Services",
    "Consumer Defensive",
    "Energy",
    "Basic Materials",
    "Real Estate",
    "Utilities",
]


# ---------------------------------------------------------------------------
# Step 1: Fetch US stocks sector-by-sector so we can tag each stock
# ---------------------------------------------------------------------------

def _fetch_sector(sector: str, min_cap_m: int, page_size: int = 250) -> list:
    """
    Page through the screener for one sector.
    Tags each quote dict with '_sector' for use in the DataFrame.
    """
    results: list = []
    for offset in range(0, 3000, page_size):
        try:
            query = EquityQuery("and", [
                EquityQuery("eq", ["region", "us"]),
                EquityQuery("is-in", ["exchange", "NMS", "NYQ"]),
                EquityQuery("gte", ["intradaymarketcap", min_cap_m * 1_000_000]),
                EquityQuery("eq", ["sector", sector]),
            ])
            resp = yf.screen(
                query,
                sortField="intradaymarketcap",
                sortAsc=False,
                size=page_size,
                offset=offset,
            )
            quotes = resp.get("quotes", [])
            if not quotes:
                break
            for q in quotes:
                q["_sector"] = sector
            results.extend(quotes)
            if len(quotes) < page_size:
                break
        except Exception as exc:
            print(f"  [ERR] sector={sector} offset={offset}: {exc}")
            break
    return results


def fetch_all_us_stocks(max_stocks: int = 4000, min_cap_m: int = 300) -> list:
    """
    Iterate through all known sectors and collect US-listed stocks.
    Each quote dict is tagged with '_sector'.
    Returns a flat list of quote dicts.
    """
    all_quotes: list = []
    print(f"Fetching US stocks by sector (min market cap ${min_cap_m}M)...")

    for sector in SECTORS:
        quotes = _fetch_sector(sector, min_cap_m)
        print(f"  {sector:<30} {len(quotes):>4} stocks")
        all_quotes.extend(quotes)
        if len(all_quotes) >= max_stocks:
            print(f"  Reached cap of {max_stocks} stocks, stopping early.")
            break

    print(f"\n  Total fetched: {len(all_quotes)} stocks across {len(SECTORS)} sectors")
    return all_quotes


def build_stock_df(quotes: list) -> pd.DataFrame:
    """
    Convert screener quote dicts into a clean DataFrame.

    Key field corrections vs the original broken field names:
      - fiftyTwoWeekChangePercent  (already in %, NOT a decimal fraction)
      - epsForward                 (was wrongly called epsForwardTwelveMonths)
      - earningsGrowth / revenueGrowth are NOT in the screener; we compute
        forward EPS growth from epsTrailingTwelveMonths + epsForward instead.
      - trailingAnnualDividendYield is the reliable decimal yield field.
    """
    rows = []
    for q in quotes:
        ticker = q.get("symbol", "")
        if not ticker:
            continue

        eps_ttm     = q.get("epsTrailingTwelveMonths")
        eps_forward = q.get("epsForward")

        # Forward EPS growth estimate: only meaningful when TTM EPS is positive
        if eps_ttm and eps_ttm > 0 and eps_forward is not None:
            eps_growth = (eps_forward - eps_ttm) / eps_ttm
        else:
            eps_growth = None

        # fiftyTwoWeekChangePercent is already a percentage (e.g. 104.3 = +104%)
        w52_pct = q.get("fiftyTwoWeekChangePercent")
        if w52_pct is None:
            w52_pct = 0.0

        rows.append({
            "ticker":               ticker,
            "name":                 q.get("shortName", ""),
            "sector":               q.get("_sector", ""),
            "price":                q.get("regularMarketPrice"),
            "change_1d_pct":        q.get("regularMarketChangePercent"),
            "volume":               q.get("regularMarketVolume"),
            "avg_volume_3m":        q.get("averageDailyVolume3Month"),
            "market_cap_b":         (q.get("marketCap") or 0) / 1e9,
            "52w_change_pct":       round(w52_pct, 2),
            "trailing_pe":          q.get("trailingPE"),
            "forward_pe":           q.get("forwardPE"),
            "eps_ttm":              eps_ttm,
            "eps_forward":          eps_forward,
            "eps_growth_fwd":       eps_growth,       # forward EPS growth fraction
            "price_to_book":        q.get("priceToBook"),
            "dividend_yield_pct":   round((q.get("trailingAnnualDividendYield") or 0) * 100, 4),
            "fifty_day_avg":        q.get("fiftyDayAverage"),
            "two_hundred_day_avg":  q.get("twoHundredDayAverage"),
            "analyst_rating":       q.get("averageAnalystRating", ""),
        })

    df = pd.DataFrame(rows).reset_index(drop=True)

    # Derive above-MA flags (used as momentum confirmation)
    price_s   = pd.to_numeric(df["price"],            errors="coerce")
    ma50_s    = pd.to_numeric(df["fifty_day_avg"],    errors="coerce")
    ma200_s   = pd.to_numeric(df["two_hundred_day_avg"], errors="coerce")
    df["above_50d_ma"]  = price_s > ma50_s
    df["above_200d_ma"] = price_s > ma200_s
    return df


# ---------------------------------------------------------------------------
# Step 3: Generate the four CSVs
# ---------------------------------------------------------------------------


def make_growing_sectors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sectors ranked by average 52-week return (positive only).
    Includes top-3 tickers per sector by 52-week return.
    """
    top3 = (
        df.sort_values("52w_change_pct", ascending=False)
        .groupby("sector")["ticker"]
        .apply(lambda x: ", ".join(x.head(3)))
        .reset_index()
        .rename(columns={"ticker": "top_stocks"})
    )

    grp = (
        df.groupby("sector")
        .agg(
            avg_52w_return_pct=("52w_change_pct", "mean"),
            median_52w_return_pct=("52w_change_pct", "median"),
            best_52w_return_pct=("52w_change_pct", "max"),
            worst_52w_return_pct=("52w_change_pct", "min"),
            avg_1d_change_pct=("change_1d_pct", "mean"),
            stock_count=("ticker", "count"),
            total_market_cap_b=("market_cap_b", "sum"),
            pct_above_50d_ma=("above_50d_ma", "mean"),
            pct_above_200d_ma=("above_200d_ma", "mean"),
        )
        .reset_index()
    )

    grp = grp.merge(top3, on="sector", how="left")
    grp = grp[grp["avg_52w_return_pct"] > 0].sort_values(
        "avg_52w_return_pct", ascending=False
    ).reset_index(drop=True)

    for col in ["avg_52w_return_pct", "median_52w_return_pct", "best_52w_return_pct",
                "worst_52w_return_pct", "avg_1d_change_pct", "total_market_cap_b"]:
        grp[col] = grp[col].round(2)
    grp["pct_above_50d_ma"]  = (grp["pct_above_50d_ma"]  * 100).round(1)
    grp["pct_above_200d_ma"] = (grp["pct_above_200d_ma"] * 100).round(1)

    path = os.path.join(OUTPUT_DIR, "growing_sectors.csv")
    grp.to_csv(path, index=False)
    print(f"  [OK] {path}  ({len(grp)} rows)")
    return grp


def make_growing_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """
    All stocks with a positive 52-week return, sorted best-to-worst.
    """
    growing = df[df["52w_change_pct"] > 0].copy()
    growing = growing.sort_values("52w_change_pct", ascending=False).reset_index(drop=True)

    keep = [
        "ticker", "name", "sector",
        "price", "change_1d_pct", "52w_change_pct",
        "market_cap_b", "volume", "trailing_pe", "forward_pe",
        "eps_ttm", "eps_forward", "eps_growth_fwd",
        "above_50d_ma", "above_200d_ma", "analyst_rating",
    ]
    growing = growing[[c for c in keep if c in growing.columns]]

    for col in ["price", "change_1d_pct", "52w_change_pct", "market_cap_b",
                "trailing_pe", "forward_pe", "eps_ttm", "eps_forward"]:
        if col in growing.columns:
            growing[col] = pd.to_numeric(growing[col], errors="coerce").round(2)

    if "eps_growth_fwd" in growing.columns:
        growing["eps_growth_fwd"] = (
            pd.to_numeric(growing["eps_growth_fwd"], errors="coerce") * 100
        ).round(1)

    path = os.path.join(OUTPUT_DIR, "growing_stocks.csv")
    growing.to_csv(path, index=False)
    print(f"  [OK] {path}  ({len(growing)} rows)")
    return growing


def make_money_flow_sectors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sectors ranked by total average daily dollar volume (3-month).
    dollar_volume_m = avg_volume_3m × price  →  proxy for capital flow.
    """
    tmp = df.copy()
    tmp["dollar_vol_m"] = (
        pd.to_numeric(tmp["avg_volume_3m"], errors="coerce").fillna(0) *
        pd.to_numeric(tmp["price"], errors="coerce").fillna(0)
    ) / 1e6

    top3 = (
        tmp.sort_values("dollar_vol_m", ascending=False)
        .groupby("sector")["ticker"]
        .apply(lambda x: ", ".join(x.head(3)))
        .reset_index()
        .rename(columns={"ticker": "top_flow_stocks"})
    )

    grp = (
        tmp.groupby("sector")
        .agg(
            total_daily_dollar_vol_m=("dollar_vol_m", "sum"),
            avg_stock_dollar_vol_m=("dollar_vol_m", "mean"),
            stock_count=("ticker", "count"),
            avg_52w_return_pct=("52w_change_pct", "mean"),
            total_market_cap_b=("market_cap_b", "sum"),
        )
        .reset_index()
    )

    grp = grp.merge(top3, on="sector", how="left")
    grp = grp.sort_values("total_daily_dollar_vol_m", ascending=False).reset_index(drop=True)

    for col in ["total_daily_dollar_vol_m", "avg_stock_dollar_vol_m",
                "avg_52w_return_pct", "total_market_cap_b"]:
        grp[col] = grp[col].round(2)

    path = os.path.join(OUTPUT_DIR, "money_flow_sectors.csv")
    grp.to_csv(path, index=False)
    print(f"  [OK] {path}  ({len(grp)} rows)")
    return grp


def make_potential_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stocks with strong momentum, projected EPS growth, and fair valuation.

    Criteria:
      - 52w_change_pct  > 10        (clear uptrend over the past year)
      - eps_growth_fwd  > 0.10      (forward EPS growth estimate > 10%)
      - forward_pe      in (0, 50]  (profitable, not extreme multiple)
      - market_cap_b    >= 1        ($1 B+ for adequate liquidity)
      - price > fifty_day_avg       (still in short-term uptrend)

    Ranked by composite score:
      score = 0.4 × 52w_change_pct
            + 0.4 × eps_growth_fwd_pct
            + 0.2 × (100 / forward_pe)
    """
    pot = df.copy()
    for col in ["52w_change_pct", "eps_growth_fwd", "forward_pe", "market_cap_b",
                "price", "fifty_day_avg"]:
        pot[col] = pd.to_numeric(pot[col], errors="coerce")

    mask = (
        (pot["52w_change_pct"]  > 10)   &
        (pot["eps_growth_fwd"]  > 0.10) &
        (pot["forward_pe"]      > 0)    &
        (pot["forward_pe"]      <= 50)  &
        (pot["market_cap_b"]    >= 1)   &
        (pot["price"]           > pot["fifty_day_avg"])
    )
    pot = pot[mask].copy()

    pot["score"] = (
        pot["52w_change_pct"]                          * 0.4 +
        pot["eps_growth_fwd"].clip(upper=5.0) * 100   * 0.4 +
        (100 / pot["forward_pe"])                      * 0.2
    ).round(2)

    pot = pot.sort_values("score", ascending=False).reset_index(drop=True)

    keep = [
        "ticker", "name", "sector",
        "price", "change_1d_pct", "52w_change_pct",
        "market_cap_b", "trailing_pe", "forward_pe",
        "eps_ttm", "eps_forward", "eps_growth_fwd",
        "price_to_book", "dividend_yield_pct",
        "above_50d_ma", "above_200d_ma",
        "analyst_rating", "score",
    ]
    pot = pot[[c for c in keep if c in pot.columns]]

    for col in ["price", "change_1d_pct", "52w_change_pct", "market_cap_b",
                "trailing_pe", "forward_pe", "eps_ttm", "eps_forward", "price_to_book"]:
        if col in pot.columns:
            pot[col] = pd.to_numeric(pot[col], errors="coerce").round(2)

    if "eps_growth_fwd" in pot.columns:
        pot["eps_growth_fwd"] = (
            pd.to_numeric(pot["eps_growth_fwd"], errors="coerce") * 100
        ).round(1)

    path = os.path.join(OUTPUT_DIR, "potential_stocks.csv")
    pot.to_csv(path, index=False)
    print(f"  [OK] {path}  ({len(pot)} rows)")
    return pot


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 65)
    print("  STOCK MARKET ANALYZER  —  powered by yfinance")
    print(f"  Run date : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Output   : {OUTPUT_DIR}/")
    print("=" * 65)

    # --- Fetch ---
    quotes = fetch_all_us_stocks(max_stocks=4000, min_cap_m=300)
    if not quotes:
        print("[FATAL] No stocks returned from screener. Exiting.")
        return

    # --- Build master DataFrame ---
    print("\nBuilding master DataFrame...")
    df = build_stock_df(quotes)
    n_sectors = df["sector"].nunique()
    print(f"  {len(df)} stocks  |  {n_sectors} sectors")
    print(f"  52w_change_pct range: {df['52w_change_pct'].min():.1f}% to {df['52w_change_pct'].max():.1f}%")
    print(f"  Stocks with positive 52w return: {(df['52w_change_pct'] > 0).sum()}")
    print(f"  Stocks with forward EPS data:    {df['eps_growth_fwd'].notna().sum()}")

    # --- Generate CSVs ---
    print(f"\nGenerating CSVs in {OUTPUT_DIR}/...")
    growing_sectors = make_growing_sectors(df)
    growing_stocks  = make_growing_stocks(df)
    money_flow      = make_money_flow_sectors(df)
    potential       = make_potential_stocks(df)

    # --- Console summary ---
    print("\n" + "=" * 65)
    print("  SUMMARY")
    print("=" * 65)
    print(f"  Growing sectors    : {len(growing_sectors)} with positive avg 52-week return")
    print(f"  Growing stocks     : {len(growing_stocks)} stocks with positive 52-week return")
    print(f"  Money flow table   : {len(money_flow)} sectors ranked by daily $ volume")
    print(f"  Potential stocks   : {len(potential)} stocks meeting growth + value criteria")

    if not growing_sectors.empty:
        top = growing_sectors.iloc[0]
        print(f"\n  Top growing sector   : {top['sector']}  "
              f"(avg 52w {top['avg_52w_return_pct']:+.1f}%)  "
              f"leaders: {top['top_stocks']}")

    if not money_flow.empty:
        top = money_flow.iloc[0]
        print(f"  Most money flowing   : {top['sector']}  "
              f"(${top['total_daily_dollar_vol_m']:,.0f}M avg daily vol)")

    if not potential.empty:
        row = potential.iloc[0]
        print(f"  Top potential stock  : {row['ticker']} — {row['name']}  "
              f"(score {row['score']:.1f})")
        print(f"\n  Top 10 potential stocks:")
        for _, r in potential.head(10).iterrows():
            print(f"    {r['ticker']:<6}  {r['name']:<35}  "
                  f"52w:{r['52w_change_pct']:>+7.1f}%  "
                  f"EPS-g:{r['eps_growth_fwd']:>+6.1f}%  "
                  f"fwdPE:{r.get('forward_pe', float('nan')):>5.1f}  "
                  f"score:{r['score']:>6.1f}")

    print(f"\n  All CSVs saved to: {OUTPUT_DIR}/")
    print("=" * 65)


if __name__ == "__main__":
    main()
