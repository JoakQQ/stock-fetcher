from pathlib import Path
from module.cache import CacheManager
from module.st import get_tickers, get_ticker_infos
from module.file import write_csv
import pandas as pd
import datetime
import os


def main():
    print("Loading cache...\n")
    cache_manager = CacheManager()
    cache_manager.load()

    print(f"Getting tickers with market cap >= $2B...\n")
    tickers = get_tickers(min_cap=2_000_000_000, max_results=2_000)

    print(f"Screening {len(tickers)} tickers...\n")
    data_list = get_ticker_infos(cache_manager=cache_manager, tickers=tickers)

    df = pd.DataFrame(data_list)
    out_dir = Path(
        f"/output-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}")
    out_dir.mkdir(exist_ok=True)
    output_csv = out_dir / "output.csv"
    write_csv(file_path=output_csv, df=df, fileId=os.getenv("OUTPUT_FILE_ID"))

    print()
    print(f"Fetched {len(tickers)} tickers.\n")
    print(f"Saved {len(df)} rows to {output_csv}")

    print("Saving cache...\n")
    cache_manager.save()
    print("Saved cache to disk.\n")


if __name__ == '__main__':
    main()
