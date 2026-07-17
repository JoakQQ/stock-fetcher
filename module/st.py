from module.cache import CacheManager
import yfinance as yf
import time
from tqdm import tqdm
import random
import requests
import json
from datetime import datetime
import holidays
from zoneinfo import ZoneInfo


YF_PAGE_SIZE = 250
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
    "price_earnings_growth_forward",
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
CLOSE_TV_COLUMNS = [
    "name",
    "change",
    "premarket_gap",
    "close",
    "high",
    "low",
    "volume",
]


def get_tickers(min_cap: int = 2_000_000_000,
                max_results: int = 5_000,
                avg_daily_vol: int = 2_000_000,
                sort_by: str = 'intradaymarketcap',
                sort_asc: bool = False) -> list[dict]:
    query = yf.EquityQuery('and', [
        yf.EquityQuery('gte', ['intradaymarketcap', min_cap]),
        yf.EquityQuery('eq', ['region', 'us']),
        yf.EquityQuery('gte', ['avgdailyvol3m', avg_daily_vol]),
    ])
    tickers = []
    offset = 0
    while len(tickers) < max_results:
        fetch = min(YF_PAGE_SIZE, max_results - len(tickers))
        try:
            result = yf.screen(query, sortField=sort_by, sortAsc=sort_asc,
                               size=fetch, offset=offset)
        except Exception as e:
            print(f"YF screen request failed at offset {offset}: {e}")
            break
        quotes = result.get('quotes', [])
        if not quotes:
            break
        tickers.extend({"yf_ticker": q["symbol"], "tv_ticker": f"{EXCHANGE_MAPPING.get(q['exchange'])}:{q['symbol'].replace('-', '.')}"}
                       for q in quotes if 'symbol' in q and 'exchange' in q and q['exchange'] in EXCHANGE_MAPPING)
        offset += len(quotes)
        if offset >= result.get('total', 0):
            break
    return tickers


def get_ticker_infos(cache_manager: CacheManager, tickers: list[dict], batch_size: int = 500) -> list:
    results = []
    yf_results = yf.Tickers(
        " ".join([t["yf_ticker"] for t in tickers]))
    for i in tqdm(range(0, len(tickers), batch_size), desc="Fetching batches", unit="batch"):
        batch = tickers[i:i + batch_size]
        tv_payload = {
            "symbols": {
                "tickers": [t["tv_ticker"] for t in batch],
                "query": {"types": []}
            },
            "columns": TV_COLUMNS
        }
        tv_headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(TV_USER_AGENTS)
        }
        try:
            tv_response = requests.post(
                TV_SCANNER_URL, headers=tv_headers, data=json.dumps(tv_payload))
            tv_response.raise_for_status()
            tv_data = tv_response.json()
        except Exception as e:
            print(
                f"TV scanner request failed for batch {i // batch_size + 1}: {e}")
            continue
        for _item in tqdm(tv_data.get('data', []), desc=f" > Fetching batch {i // batch_size + 1}", unit="ticker", leave=False):
            item = _item['d']
            symbol = _item['s']

            def _pct(key):
                val = item[TV_COLUMNS.index(key)]
                return f"{val:.2f}%" if val is not None else "N/A"
            yf_ticker = next((t["yf_ticker"]
                             for t in batch if t["tv_ticker"] == symbol), None)
            bsummary = cache_manager.get_bsummary(yf_ticker)
            if bsummary is None:
                bsummary = yf_results.tickers.get(
                    yf_ticker).info.get("longBusinessSummary")
                cache_manager.append(yf_ticker, bsummary)
            row = {
                "Ticker": item[TV_COLUMNS.index("name")],
                "Sector": item[TV_COLUMNS.index("sector")],
                "Close": item[TV_COLUMNS.index("close")],
                "Change": _pct("change"),
                "Premarket Change": _pct("premarket_change"),
                "Postmarket Change": _pct("postmarket_change"),
                "Premarket Gap": _pct("premarket_gap"),
                "PEG Ratio (TTM)": item[TV_COLUMNS.index("price_earnings_growth_ttm")],
                "PEG Ratio (Forward)": item[TV_COLUMNS.index("price_earnings_growth_forward")],
                "Debt/Equity": item[TV_COLUMNS.index("debt_to_equity_fq")],
                "Profit Margin": item[TV_COLUMNS.index("profit_margin_ttm")],
                "ROE": item[TV_COLUMNS.index("return_on_equity_fq")],
                "Short Float %": _pct("short_percentage_of_float"),
                "Avg Vol (10d)": item[TV_COLUMNS.index("average_volume_10d_calc")],
                "Avg Vol (30d)": item[TV_COLUMNS.index("average_volume_30d_calc")],
                "Volume": item[TV_COLUMNS.index("volume")],
                "Business Summary": bsummary if bsummary is not None else "N/A",
            }
            results.append(row)
        time.sleep(0.25)
    return results

def get_close_infos(tickers: list[dict], batch_size: int = 500) -> list:
    results = []
    for i in tqdm(range(0, len(tickers), batch_size), desc="Fetching batches", unit="batch"):
        batch = tickers[i:i + batch_size]
        tv_payload = {
            "symbols": {
                "tickers": [t["tv_ticker"] for t in batch],
                "query": {"types": []}
            },
            "columns": CLOSE_TV_COLUMNS
        }
        tv_headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(TV_USER_AGENTS)
        }
        try:
            tv_response = requests.post(
                TV_SCANNER_URL, headers=tv_headers, data=json.dumps(tv_payload))
            tv_response.raise_for_status()
            tv_data = tv_response.json()
        except Exception as e:
            print(
                f"TV scanner request failed for batch {i // batch_size + 1}: {e}")
            continue
        for _item in tqdm(tv_data.get('data', []), desc=f" > Fetching batch {i // batch_size + 1}", unit="ticker", leave=False):
            item = _item['d']

            row = {
                "ticker": item[CLOSE_TV_COLUMNS.index("name")],
                "change": item[CLOSE_TV_COLUMNS.index("change")],
                "premarket_gap": item[CLOSE_TV_COLUMNS.index("premarket_gap")],
                "close": item[CLOSE_TV_COLUMNS.index("close")],
                "high": item[CLOSE_TV_COLUMNS.index("high")],
                "low": item[CLOSE_TV_COLUMNS.index("low")],
                "volume": item[CLOSE_TV_COLUMNS.index("volume")],
            }
            results.append(row)
        time.sleep(0.25)
    return results

def is_trading_day() -> bool:
    now = datetime.now()
    now = now.astimezone(ZoneInfo("America/New_York"))
    if now.weekday() >= 5:
        return False
    nyse_holidays = holidays.financial_holidays("NYSE", years=now.year)
    if now.date() in nyse_holidays:
        return False
    return True
