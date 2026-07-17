import os
from pathlib import Path
from sqlite3 import connect, Connection
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import google.auth

IS_GITHUB = os.getenv("GITHUB_ACTIONS") == "true"

def open_sqlite_connection(db_path: str = "cache/data.db", fileId: str | None = None) -> Connection:
    if IS_GITHUB:
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive"])
        service = build("drive", "v3", credentials=creds)
        request = service.files().get_media(fileId=fileId)
        with open(db_path, "wb") as local_file:
            downloader = MediaIoBaseDownload(local_file, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
    return connect(db_path)

def close_sqlite_connection(conn: Connection, db_path: str = "cache/data.db", fileId: str | None = None):
    conn.close()
    if IS_GITHUB:
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive"])
        service = build("drive", "v3", credentials=creds)
        media = MediaFileUpload(db_path, resumable=True)
        service.files().update(
            fileId=fileId,
            body={"name": Path(db_path).name},
            media_body=media,
            fields="id"
        ).execute()
        os.remove(db_path)
