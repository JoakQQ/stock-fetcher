import yfinance as yf
import pandas as pd
import signal
import time
import datetime
from pathlib import Path
from tqdm import tqdm
import random
import requests
import json
from yfinance import data

YF_PAGE_SIZE = 250
YF_PAGE_SIZE=250
TV_SCANNER_URL = "https://scanner.tradingview.com/global/scan"
EXCHANGE_MAPPING = {
    'NMS': 'NASDAQ',
    'NCM': 'NASDAQ',
    'NYQ': 'NYSE',
    'ASE': 'AMEX',
    'PCX': 'AMEX',
    'PNK': 'OTC',
    'NGM': 'NASDAQ',
}
TV_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
]
TV_COLUMNS = [
    "name",
    "sector",
    "price_earnings_growth_ttm",
    "change",
    "premarket_change",
    "postmarket_change",
    "premarket_gap",
    "close",
    "high",
    "low",
    "debt_to_equity_fq",
    "profit_margin_ttm",
    "return_on_equity_fq",
    "short_percentage_of_float",
    "average_volume_10d_calc",
    "average_volume_30d_calc",
    "volume",
]

def get_tickers(min_cap: int = 2_000_000_000,
                max_results: int = 5_000,
                sort_by: str = 'intradaymarketcap',
                sort_asc: bool = False) -> list[str]:
    query = yf.EquityQuery('and', [
        yf.EquityQuery('gte', ['intradaymarketcap', min_cap]),
        yf.EquityQuery('eq', ['region', 'us']),
        yf.EquityQuery('gte', ['avgdailyvol3m', 1_000_000]),
    ])
    tickers = []
    offset = 0
    while len(tickers) < max_results:
        fetch = min(YF_PAGE_SIZE, max_results - len(tickers))
        try:
            result = yf.screen(query, sortField=sort_by, sortAsc=sort_asc, size=fetch, offset=offset)
        except Exception as e:
            print(f"YF screen request failed at offset {offset}: {e}")
            break
        quotes = result.get('quotes', [])
        if not quotes:
            break
        tickers.extend(f"{EXCHANGE_MAPPING.get(q['exchange'])}:{q['symbol'].replace('-', '.')}" for q in quotes if 'symbol' in q and 'exchange' in q and q['exchange'] in EXCHANGE_MAPPING)
        offset += len(quotes)
        if offset >= result.get('total', 0):
            break
    return tickers

def get_ticker_infos(tickers: list[str], batch_size: int = 500) -> list:
    results = []
    for i in tqdm(range(0, len(tickers), batch_size), desc="Fetching batches", unit="batch"):
        batch = tickers[i:i + batch_size]
        tv_payload = {
            "symbols": {
                "tickers": batch,
                "query": {"types": []}
            },
            "columns": TV_COLUMNS
        }
        tv_headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(TV_USER_AGENTS)
        }
        try:
            tv_response = requests.post(TV_SCANNER_URL, headers=tv_headers, data=json.dumps(tv_payload))
            tv_response.raise_for_status()
            tv_data = tv_response.json()
        except Exception as e:
            print(f"TV scanner request failed for batch {i // batch_size + 1}: {e}")
            continue
        for _item in tv_data.get('data', []):
            item = _item['d']
            if item[TV_COLUMNS.index("change")] is None or item[TV_COLUMNS.index("change")] <= 0:
                continue
            def _pct(key):
                val = item[TV_COLUMNS.index(key)]
                return f"{val:.2f}%" if val is not None else "N/A"
            row = {
                "Ticker": item[TV_COLUMNS.index("name")],
                "Sector": item[TV_COLUMNS.index("sector")],
                "Close": item[TV_COLUMNS.index("close")],
                "Change": _pct("change"),
                "Premarket Change": _pct("premarket_change"),
                "Postmarket Change": _pct("postmarket_change"),
                "Premarket Gap": _pct("premarket_gap"),
                "PEG Ratio": item[TV_COLUMNS.index("price_earnings_growth_ttm")],
                "Debt/Equity": item[TV_COLUMNS.index("debt_to_equity_fq")],
                "Profit Margin": item[TV_COLUMNS.index("profit_margin_ttm")],
                "ROE": item[TV_COLUMNS.index("return_on_equity_fq")],
                "Short Float %": _pct("short_percentage_of_float"),
                "Avg Vol (10d)": item[TV_COLUMNS.index("average_volume_10d_calc")],
                "Avg Vol (30d)": item[TV_COLUMNS.index("average_volume_30d_calc")],
                "Volume": item[TV_COLUMNS.index("volume")],
            }
            results.append(row)
        time.sleep(0.25)
    return results

def main():
    print(f"Getting tickers with market cap >= $2B...\n")
    tickers = get_tickers(min_cap=2_000_000_000, max_results=2_000)

    print(f"Screening {len(tickers)} tickers...\n")
    data_list = get_ticker_infos(tickers)

    df = pd.DataFrame(data_list)
    out_dir = Path(f"output-{datetime.date.today().isoformat()}")
    out_dir.mkdir(exist_ok=True)
    output_txt = out_dir / "output.txt"
    output_csv = out_dir / "output.csv"
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write(df.to_string(index=False))
    df.to_csv(output_csv, index=False)

    print()
    print(f"Fetched {len(tickers)} tickers, screened down to {len(df)} rows.\n")
    print(f"Saved {len(df)} rows to {output_txt} and {output_csv}")

if __name__ == '__main__':
    main()

