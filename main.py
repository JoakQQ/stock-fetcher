import yfinance as yf
import pandas as pd
import signal
import sys
from tqdm import tqdm

def _handle_sigint(sig, frame):
    # Raise KeyboardInterrupt so the loop can break and save partial results
    raise KeyboardInterrupt


_PAGE_SIZE = 250  # Yahoo Finance maximum per request


def get_tickers_by_market_cap(min_cap: int = 1_000_000_000,
                               max_results: int = 500,
                               sort_by: str = 'intradaymarketcap',
                               sort_asc: bool = False) -> list:
    """
    Return up to max_results tickers with market cap >= min_cap.
    Paginates through screen() in chunks of 250 (Yahoo max per call).
    sort_by: screener field, e.g. 'intradaymarketcap', 'percentchange', 'dayvolume'
    """
    query = yf.EquityQuery('and', [
        yf.EquityQuery('gte', ['intradaymarketcap', min_cap]),
        yf.EquityQuery('eq', ['region', 'us']),
    ])
    tickers = []
    offset = 0
    while len(tickers) < max_results:
        fetch = min(_PAGE_SIZE, max_results - len(tickers))
        try:
            result = yf.screen(query, sortField=sort_by, sortAsc=sort_asc,
                               size=fetch, offset=offset)
        except Exception as e:
            print(f"Screen request failed at offset {offset}: {e}")
            break
        quotes = result.get('quotes', [])
        if not quotes:
            break
        tickers.extend(q['symbol'] for q in quotes if 'symbol' in q)
        offset += len(quotes)
        if offset >= result.get('total', 0):
            break
    return tickers


data_list = []


def main():
    signal.signal(signal.SIGINT, _handle_sigint)
    tickers = get_tickers_by_market_cap(min_cap=2_000_000_000, max_results=5_000)

    print(f"Screening {len(tickers)} tickers with market cap >= $2B...\n")
    try:
        for symbol in tqdm(tickers, desc="Fetching", unit="ticker"):
            try:
                stock = yf.Ticker(symbol)
                info = stock.info

                # 1. Contextualize Volume: Calculate Relative Volume (Current Vol / Avg 10-Day Vol)
                current_vol = info.get("volume") or 0
                avg_vol_10d = info.get("averageDailyVolume10Day") or 1
                rel_volume = current_vol / avg_vol_10d if current_vol else 0

                # 2. Extract and format data
                row = {
                    "Ticker": symbol,
                    "Sector": info.get("sector", "N/A"),
                    "Industry": info.get("industry", "N/A"),
                    "Close": info.get("currentPrice"),
                    "Change %": f"{((info.get('currentPrice') or 0) - (info.get('open') or 0)) / (info.get('open') or 1) * 100:.2f}%",
                    # > 1 means higher than usual activity
                    "Rel Vol": f"{rel_volume:.2f}x",
                    "Forward PE": info.get("forwardPE"),
                    "PEG Ratio": info.get("pegRatio"),
                    "Debt/Equity": info.get("debtToEquity"),
                    "ROE": f"{(info.get('returnOnEquity', 0) * 100):.2f}%" if info.get('returnOnEquity') else "N/A",
                    "Profit Margin": f"{(info.get('profitMargins', 0) * 100):.2f}%" if info.get('profitMargins') else "N/A",
                    "Short Float %": f"{(info.get('shortPercentOfFloat', 0) * 100):.2f}%" if info.get('shortPercentOfFloat') else "N/A"
                }
                data_list.append(row)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                tqdm.write(f"Could not fetch {symbol}: {e}")
    except KeyboardInterrupt:
        tqdm.write("\nInterrupted — saving partial results...")

    # Create DataFrame and save
    df = pd.DataFrame(data_list)
    output_txt = "output.txt"
    output_pkl = "output.pkl"
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write(df.to_string(index=False))
    df.to_pickle(output_pkl)
    print(f"\nSaved {len(df)} rows to {output_txt} and {output_pkl}")


if __name__ == '__main__':
    main()
