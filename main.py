
"""
Stock Market Analyzer
=====================
Fetches stock & sector data from Yahoo Finance via yfinance,
extracts useful information, and identifies the best-performing
stocks and sectors.

References:
  - yfinance docs: https://ranaroussi.github.io/yfinance
  - GitHub: https://github.com/ranaroussi/yfinance
"""

import yfinance as yf
from yfinance import EquityQuery
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate

import sys
import io

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.float_format", "{:.2f}".format)

# Fix encoding for Windows terminals
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SECTOR_KEYS = [
    "technology",
    "healthcare",
    "financial-services",
    "consumer-cyclical",
    "communication-services",
    "industrials",
    "consumer-defensive",
    "energy",
    "real-estate",
    "basic-materials",
    "utilities",
]

# Sector display names mapped to EquityQuery sector values
SECTOR_NAMES = {
    "technology": "Technology",
    "healthcare": "Healthcare",
    "financial-services": "Financial Services",
    "consumer-cyclical": "Consumer Cyclical",
    "communication-services": "Communication Services",
    "industrials": "Industrials",
    "consumer-defensive": "Consumer Defensive",
    "energy": "Energy",
    "real-estate": "Real Estate",
    "basic-materials": "Basic Materials",
    "utilities": "Utilities",
}

# EquityQuery uses these exact sector labels
_SCREENER_SECTOR_NAMES = {
    "technology": "Technology",
    "healthcare": "Healthcare",
    "financial-services": "Financial Services",
    "consumer-cyclical": "Consumer Cyclical",
    "communication-services": "Communication Services",
    "industrials": "Industrials",
    "consumer-defensive": "Consumer Defensive",
    "energy": "Energy",
    "real-estate": "Real Estate",
    "basic-materials": "Basic Materials",
    "utilities": "Utilities",
}


def fetch_sector_tickers(max_per_sector=250):
    """Dynamically fetch all stock tickers for each sector via yf.screen."""
    print("\n" + "=" * 70)
    print("  FETCHING TICKERS FOR ALL SECTORS")
    print("=" * 70)

    sector_tickers = {}
    for key, display_name in SECTOR_NAMES.items():
        screener_name = _SCREENER_SECTOR_NAMES[key]
        try:
            query = EquityQuery("and", [
                EquityQuery("eq", ["sector", screener_name]),
                EquityQuery("is-in", ["exchange", "NMS", "NYQ"]),
                EquityQuery("gte", ["intradaymarketcap", 1_000_000_000]),
            ])
            response = yf.screen(
                query,
                sortField="intradaymarketcap",
                sortAsc=False,
                size=max_per_sector,
            )
            quotes = response.get("quotes", [])
            tickers = [q["symbol"] for q in quotes if "symbol" in q]
            sector_tickers[display_name] = tickers
            print(f"  [OK] {display_name}: {len(tickers)} stocks")
        except Exception as e:
            print(f"  [ERR] {display_name}: {e}")
            sector_tickers[display_name] = []

    total = sum(len(v) for v in sector_tickers.values())
    print(f"\n  Total unique tickers: {len({t for v in sector_tickers.values() for t in v})}"
          f"  (total across sectors: {total})")
    return sector_tickers


# =====================================================================
# 1. FETCH STOCK INFORMATION
# =====================================================================
def fetch_sector_overview():
    """Fetch overview data for every GICS sector using yf.Sector."""
    print("\n" + "=" * 70)
    print("  SECTOR OVERVIEW (from Yahoo Finance)")
    print("=" * 70)

    sector_rows = []
    for key in SECTOR_KEYS:
        try:
            sec = yf.Sector(key)
            overview = sec.overview
            sector_rows.append({
                "Sector": sec.name,
                "Symbol": sec.symbol,
                "Key": key,
            })
            print(f"  [OK] {sec.name}")
        except Exception as e:
            print(f"  [ERR] {key}: {e}")

    if sector_rows:
        df = pd.DataFrame(sector_rows)
        print("\n" + tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
    return sector_rows


def fetch_top_companies_per_sector():
    """Retrieve top companies for each sector from yf.Sector."""
    print("\n" + "=" * 70)
    print("  TOP COMPANIES PER SECTOR")
    print("=" * 70)

    all_companies = {}
    for key in SECTOR_KEYS:
        try:
            sec = yf.Sector(key)
            top = sec.top_companies
            if top is not None and not top.empty:
                all_companies[sec.name] = top
                print(f"\n--- {sec.name} ---")
                print(tabulate(
                    top.head(5).reset_index(),
                    headers="keys",
                    tablefmt="simple",
                    showindex=False,
                ))
        except Exception as e:
            print(f"  [ERR] {key}: {e}")

    return all_companies


# =====================================================================
# 2. EXTRACT USEFUL INFORMATION
# =====================================================================
def download_price_data(sector_tickers, period="6mo"):
    """Download historical price data for all tracked tickers."""
    all_tickers = sorted({t for tlist in sector_tickers.values() for t in tlist})
    print(f"\nDownloading price data for {len(all_tickers)} tickers (period={period})...")

    data = yf.download(all_tickers, period=period, group_by="ticker", progress=True)
    return data


def compute_stock_metrics(data, sector_tickers):
    """Compute performance metrics for each stock from downloaded data."""
    print("\n" + "=" * 70)
    print("  STOCK PERFORMANCE METRICS")
    print("=" * 70)

    records = []
    all_tickers = sorted({t for tlist in sector_tickers.values() for t in tlist})

    for ticker in all_tickers:
        try:
            # Handle both single and multi-level column indices
            if isinstance(data.columns, pd.MultiIndex):
                close = data[(ticker, "Close")].dropna()
            else:
                close = data["Close"].dropna()

            if len(close) < 2:
                continue

            current_price = close.iloc[-1]
            start_price = close.iloc[0]
            total_return = (current_price / start_price - 1) * 100

            # Daily returns
            daily_ret = close.pct_change().dropna()
            avg_daily_return = daily_ret.mean() * 100
            volatility = daily_ret.std() * (252 ** 0.5) * 100  # annualised
            sharpe = (daily_ret.mean() / daily_ret.std()) * (252 ** 0.5) if daily_ret.std() > 0 else 0

            # Drawdown
            cummax = close.cummax()
            drawdown = ((close - cummax) / cummax).min() * 100

            # 52-week high / low (approximate from data)
            high_52 = close.max()
            low_52 = close.min()
            pct_from_high = (current_price / high_52 - 1) * 100

            records.append({
                "Ticker": ticker,
                "Price": current_price,
                "Total Return %": total_return,
                "Avg Daily Ret %": avg_daily_return,
                "Volatility %": volatility,
                "Sharpe Ratio": sharpe,
                "Max Drawdown %": drawdown,
                "High": high_52,
                "Low": low_52,
                "% From High": pct_from_high,
            })
        except Exception:
            continue

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("Total Return %", ascending=False).reset_index(drop=True)
        print("\n" + tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
    return df


def extract_ticker_info(tickers_list):
    """Extract fundamental info for a list of tickers."""
    print("\n" + "=" * 70)
    print("  KEY FUNDAMENTAL DATA (sample)")
    print("=" * 70)

    rows = []
    for sym in tickers_list[:15]:  # limit to avoid rate limiting
        try:
            t = yf.Ticker(sym)
            info = t.info
            rows.append({
                "Ticker": sym,
                "Name": info.get("shortName", "N/A"),
                "Market Cap": info.get("marketCap", 0),
                "P/E": info.get("trailingPE", None),
                "Fwd P/E": info.get("forwardPE", None),
                "EPS": info.get("trailingEps", None),
                "Div Yield %": (info.get("dividendYield") or 0) * 100,
                "52w Change %": (info.get("52WeekChange") or 0) * 100,
                "Beta": info.get("beta", None),
                "Sector": info.get("sector", "N/A"),
            })
            print(f"  [OK] {sym}")
        except Exception as e:
            print(f"  [ERR] {sym}: {e}")

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("52w Change %", ascending=False).reset_index(drop=True)
        print("\n" + tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
    return df


# =====================================================================
# 3. COMPARE SECTORS & FIND BEST PERFORMERS
# =====================================================================
def analyse_sectors(stock_metrics, sector_tickers):
    """Aggregate stock metrics by sector and rank sectors."""
    print("\n" + "=" * 70)
    print("  SECTOR PERFORMANCE RANKING")
    print("=" * 70)

    sector_records = []
    for sector, tickers in sector_tickers.items():
        subset = stock_metrics[stock_metrics["Ticker"].isin(tickers)]
        if subset.empty:
            continue
        sector_records.append({
            "Sector": sector,
            "Avg Return %": subset["Total Return %"].mean(),
            "Median Return %": subset["Total Return %"].median(),
            "Best Stock": subset.iloc[0]["Ticker"] if not subset.empty else "N/A",
            "Best Return %": subset["Total Return %"].max(),
            "Worst Return %": subset["Total Return %"].min(),
            "Avg Volatility %": subset["Volatility %"].mean(),
            "Avg Sharpe": subset["Sharpe Ratio"].mean(),
            "Avg Drawdown %": subset["Max Drawdown %"].mean(),
        })

    df = pd.DataFrame(sector_records)
    if not df.empty:
        df = df.sort_values("Avg Return %", ascending=False).reset_index(drop=True)
        print("\n" + tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
    return df


def best_stocks_per_sector(stock_metrics, sector_tickers, top_n=3):
    """Show the top N performing stocks within each sector."""
    print("\n" + "=" * 70)
    print(f"  TOP {top_n} STOCKS PER SECTOR")
    print("=" * 70)

    for sector, tickers in sector_tickers.items():
        subset = stock_metrics[stock_metrics["Ticker"].isin(tickers)].copy()
        if subset.empty:
            continue
        top = subset.nlargest(top_n, "Total Return %")[
            ["Ticker", "Price", "Total Return %", "Volatility %", "Sharpe Ratio", "Max Drawdown %"]
        ]
        print(f"\n--- {sector} ---")
        print(tabulate(top, headers="keys", tablefmt="simple", showindex=False))


def run_screeners():
    """Run Yahoo Finance predefined screeners for useful stock lists."""
    print("\n" + "=" * 70)
    print("  YAHOO FINANCE SCREENERS")
    print("=" * 70)

    screener_names = [
        "day_gainers",
        "day_losers",
        "most_actives",
        "growth_technology_stocks",
        "undervalued_growth_stocks",
        "undervalued_large_caps",
    ]

    results = {}
    for name in screener_names:
        try:
            response = yf.screen(name, count=10)
            quotes = response.get("quotes", [])
            if quotes:
                df = pd.DataFrame(quotes)
                cols = [c for c in [
                    "symbol", "shortName", "regularMarketPrice",
                    "regularMarketChangePercent", "marketCap",
                    "averageDailyVolume3Month",
                ] if c in df.columns]
                df = df[cols] if cols else df.iloc[:, :6]
                results[name] = df
                print(f"\n--- {name.replace('_', ' ').title()} ---")
                print(tabulate(df.head(10), headers="keys", tablefmt="simple", showindex=False))
            else:
                print(f"  [{name}] No results")
        except Exception as e:
            print(f"  [{name}] Error: {e}")

    return results


def run_custom_screen():
    """Run a custom screener: high-growth, large-cap US stocks across all sectors."""
    print("\n" + "=" * 70)
    print("  CUSTOM SCREEN: High Growth Large-Cap US Stocks")
    print("=" * 70)

    try:
        query = EquityQuery("and", [
            EquityQuery("eq", ["region", "us"]),
            EquityQuery("gte", ["intradaymarketcap", 10_000_000_000]),
            EquityQuery("gte", ["epsgrowth.lasttwelvemonths", 20]),
        ])
        response = yf.screen(query, sortField="epsgrowth.lasttwelvemonths", sortAsc=False, size=25)
        quotes = response.get("quotes", [])
        if quotes:
            df = pd.DataFrame(quotes)
            cols = [c for c in [
                "symbol", "shortName", "regularMarketPrice",
                "regularMarketChangePercent", "marketCap",
                "epsCurrentYear", "epsTrailingTwelveMonths",
            ] if c in df.columns]
            df = df[cols] if cols else df.iloc[:, :6]
            print("\n" + tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
            return df
    except Exception as e:
        print(f"  Error: {e}")
    return pd.DataFrame()


def print_summary(sector_ranking, stock_metrics):
    """Print a concise summary of findings."""
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)

    if not sector_ranking.empty:
        best_sector = sector_ranking.iloc[0]
        worst_sector = sector_ranking.iloc[-1]
        print(f"\n  Best Sector:  {best_sector['Sector']}  "
              f"(Avg Return: {best_sector['Avg Return %']:.2f}%,  Sharpe: {best_sector['Avg Sharpe']:.2f})")
        print(f"  Worst Sector: {worst_sector['Sector']}  "
              f"(Avg Return: {worst_sector['Avg Return %']:.2f}%,  Sharpe: {worst_sector['Avg Sharpe']:.2f})")

    if not stock_metrics.empty:
        best = stock_metrics.iloc[0]
        print(f"\n  Best Overall Stock: {best['Ticker']}  "
              f"(Return: {best['Total Return %']:.2f}%,  Sharpe: {best['Sharpe Ratio']:.2f})")

        print(f"\n  Top 10 Stocks by Total Return:")
        for _, row in stock_metrics.head(10).iterrows():
            print(f"    {row['Ticker']:>6s}  {row['Total Return %']:+8.2f}%   "
                  f"Vol: {row['Volatility %']:5.1f}%   Sharpe: {row['Sharpe Ratio']:.2f}")

    print("\n" + "=" * 70)


# =====================================================================
# MAIN
# =====================================================================
def main():
    print("=" * 70)
    print("  STOCK MARKET ANALYZER  —  powered by yfinance")
    print(f"  Run date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    # --- Step 0: Dynamically fetch all tickers per sector ---
    sector_tickers = fetch_sector_tickers(max_per_sector=250)

    # --- Step 1: Fetch sector-level information ---
    fetch_sector_overview()
    fetch_top_companies_per_sector()

    # --- Step 2: Download price data & compute metrics ---
    price_data = download_price_data(sector_tickers, period="6mo")
    stock_metrics = compute_stock_metrics(price_data, sector_tickers)

    # Extract fundamental info for the top movers
    if not stock_metrics.empty:
        top_tickers = stock_metrics.head(15)["Ticker"].tolist()
        extract_ticker_info(top_tickers)

    # --- Step 3: Sector comparison & best performers ---
    sector_ranking = analyse_sectors(stock_metrics, sector_tickers)
    best_stocks_per_sector(stock_metrics, sector_tickers, top_n=3)

    # --- Screeners ---
    run_screeners()
    run_custom_screen()

    # --- Summary ---
    print_summary(sector_ranking, stock_metrics)


if __name__ == "__main__":
    main()
