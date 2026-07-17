import pandas as pd
import os
from module.st import get_tickers, is_trading_day, get_close_infos
from module.db import open_sqlite_connection, close_sqlite_connection
from sqlite3 import Connection
from datetime import datetime

def is_fetched(conn: Connection, table_name: str = "st") -> bool:
    return pd.read_sql_query(f"select max(date) as latest_date from {table_name}", conn).iloc[0].latest_date == datetime.now().strftime("%Y-%m-%d")

def fetch_ticker_names(conn: Connection, table_name: str = "t"):
    print("fetching ticker names...")
    fetched_tickers = get_tickers(min_cap=2_000_000_000, max_results=2_000, avg_daily_vol=2_000_000)
    fetched_tickers = pd.DataFrame(fetched_tickers)
    old_tickers = pd.read_sql_query(f"select * from {table_name}", conn)
    new_tickers = fetched_tickers[~fetched_tickers['tv_ticker'].isin(old_tickers['tv_ticker'])]
    new_tickers.to_sql(table_name, conn, if_exists="append", index=False)

def fetch_ticker_infos(conn: Connection, ticker_table_name: str = "t", info_table_name: str = "st"):
    print("fetching ticker infos...")
    tickers = pd.read_sql_query(f"select * from {ticker_table_name}", conn)
    tickers = tickers.to_dict(orient='records')
    ticker_infos = get_close_infos(tickers=tickers)
    ticker_infos = pd.DataFrame(ticker_infos)
    ticker_infos['date'] = datetime.now().strftime("%Y-%m-%d")
    ticker_infos.to_sql(info_table_name, conn, if_exists="append", index=False)

def main():
    os.makedirs("cache", exist_ok=True)
    conn = open_sqlite_connection(db_path="cache/data.db")
    if not is_trading_day():
        print("Today is not a trading day. Exiting...")
        close_sqlite_connection(conn, db_path="cache/data.db")
        return
    elif is_fetched(conn):
        print("Data already fetched for today. Exiting...")
        close_sqlite_connection(conn, db_path="cache/data.db")
        return
    fetch_ticker_names(conn)
    fetch_ticker_infos(conn)
    close_sqlite_connection(conn, db_path="cache/data.db")
    print("Data fetched and saved to SQLite database successfully.")

if __name__ == '__main__':
    main()
