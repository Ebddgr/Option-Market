from datetime import date, time
import os
import requests
import concurrent.futures
import asyncio
import aiohttp
from date_conversion import generate_jalali_date_range
from jdatetime import date, timedelta
import os
import time
import concurrent.futures
import pandas as pd


start_date = "1401-01-01"
end_date = date.today().isoformat()
# Download the datebase
tic = time.time()

# start_date = (date.today() - timedelta(days=30)).isoformat()  # type: ignore
# end_date = date.today().isoformat()
download_dates = generate_jalali_date_range(start_date, end_date)


def download_and_save_tsetmc(jalali_date: str, output_directory="./tsetmcdata"):
    base_url = "https://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d="
    url = f"{base_url}{jalali_date}"
    filename = os.path.join(output_directory, f"{jalali_date}.xlsx")

    if os.path.exists(filename):
        print(f"File {jalali_date}.xlsx already exists. Skipping download.")
        return

    try:
        print(f"Requesting URL: {url}")
        response = requests.get(url)
        if response.status_code == 200:
            content = response.content
            if len(content) / 1024 > 5:
                with open(filename, "wb") as f:
                    f.write(content)
                print(f"CSV data for {jalali_date} saved to {filename}")
            else:
                print(f"{jalali_date} is not a trading day.")
        else:
            print(
                f"Failed to download CSV for {jalali_date}. Status code:",
                response.status_code,
            )
    except Exception as e:
        print(f"Error while downloading CSV for {jalali_date}:", str(e))


def main():
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=1
    ) as executor:  # Adjust max_workers as needed
        futures = [
            executor.submit(download_and_save_tsetmc, date) for date in download_dates
        ]
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()

toc = time.time()
print(toc - toc)
