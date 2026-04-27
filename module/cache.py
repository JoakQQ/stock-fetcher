import pandas as pd
import datetime
from pathlib import Path
import os
import io
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

LOCAL_CACHE_DIR = "cache"
IS_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"

class CacheManager:
    cache: pd.DataFrame
    cache_path: Path

    def __init__(self, cache_dir: str = LOCAL_CACHE_DIR):
        self.cache_dir = cache_dir
        self.cache = pd.DataFrame()
        self.cache_path = Path(os.path.join(
            cache_dir, f"cache-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv"))
        os.makedirs(self.cache_dir, exist_ok=True)

    def load(self):
        if IS_GITHUB:
            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
            service = build("drive", "v3", credentials=creds)
            request = service.files().get_media(fileId=os.getenv("CACHE_FILE_ID"))
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
            fh.seek(0) 
            df = pd.read_csv(fh, parse_dates=["timestamp"])
            self.cache = df[df["timestamp"] > datetime.datetime.now() - datetime.timedelta(days=30)]
        else:
            cache_files = os.listdir(self.cache_dir)
            cache_files.sort(reverse=True)
            latest_cache_files = [os.path.join(
                self.cache_dir, f) for f in cache_files if f.endswith(".csv")]
            if latest_cache_files:
                latest_cache_path = Path(latest_cache_files[0])
                if latest_cache_path.exists():
                    self.cache_path = latest_cache_path
                    df = pd.read_csv(latest_cache_path, parse_dates=["timestamp"])
                    self.cache = df[df["timestamp"] > datetime.datetime.now() - datetime.timedelta(days=30)]

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
        self.cache.to_csv(self.cache_path, index=False)
        if IS_GITHUB:
            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
            service = build("drive", "v3", credentials=creds)
            request = service.files().get_media(fileId=os.getenv("CACHE_FILE_ID"))
            media = MediaFileUpload(self.cache_path, resumable=True)
            service.files().update(
                fileId=os.getenv("OUTPUT_FILE_ID"),
                body={"name": f"cache-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv" },
                media_body=media,
                fields="id, name"
            ).execute()

