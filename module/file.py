from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import google.auth
import pandas as pd
import io
import os

IS_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"

def read_csv(file_path: Path | None=None, parse_dates: bool | list[int] | list[str] | None=None, fileId: str | None = None) -> pd.DataFrame:
    if IS_GITHUB:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
        service = build("drive", "v3", credentials=creds)
        request = service.files().get_media(fileId=fileId)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return pd.read_csv(fh, parse_dates=parse_dates)
    else:
        return pd.read_csv(file_path, parse_dates=parse_dates)

def write_csv(file_path: Path, df: pd.DataFrame, fileId: str | None = None):
    df.to_csv(file_path, index=False)
    if IS_GITHUB:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
        service = build("drive", "v3", credentials=creds)
        media = MediaFileUpload(file_path, resumable=True)
        service.files().update(
            fileId=fileId,
            body={"name": file_path.name},
            media_body=media,
            fields="id"
        ).execute()
        os.remove(file_path)
