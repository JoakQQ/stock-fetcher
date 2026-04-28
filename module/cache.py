from module.file import read_csv, write_csv
from pathlib import Path
import pandas as pd
import datetime
import os

LOCAL_CACHE_DIR = "cache"
IS_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"

class CacheManager:
    cache_dir: Path
    cache: pd.DataFrame
    cache_path: Path

    def __init__(self, cache_dir: str = LOCAL_CACHE_DIR):
        self.cache_dir = Path(cache_dir)
        self.cache = pd.DataFrame()
        self.cache_path = self.cache_dir / f"cache-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        os.makedirs(self.cache_dir, exist_ok=True)

    def load(self):
        if not IS_GITHUB:
            cache_files = os.listdir(self.cache_dir)
            cache_files.sort(reverse=True)
            latest_cache_files = [self.cache_dir / f for f in cache_files if f.endswith(".csv")]
            if latest_cache_files and latest_cache_files[0].exists():
                self.cache_path = latest_cache_files[0]
        df = read_csv(file_path=self.cache_path, parse_dates=["timestamp"], fileId=os.getenv("CACHE_FILE_ID"))
        df = df[df["timestamp"] > datetime.datetime.now() - datetime.timedelta(days=30)]
        self.cache = df

    def get_bsummary(self, ticker: str | None) -> str | None:
        if self.cache.empty or ticker is None:
            return None
        row = self.cache[self.cache["ticker"] == ticker]
        if not row.empty:
            return row.iloc[0]["business_summary"]
        return None

    def append(self, ticker: str | None, bsummary: str | None):
        if ticker is None or bsummary is None:
            return
        new_row = pd.DataFrame(
            [{"ticker": ticker, "business_summary": bsummary, "timestamp": pd.to_datetime(datetime.datetime.now())}])
        self.cache = pd.concat([self.cache, new_row], ignore_index=True)

    def save(self):
        write_csv(file_path=self.cache_path, df=self.cache, fileId=os.getenv("CACHE_FILE_ID"))

