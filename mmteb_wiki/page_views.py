import requests
import os
import gzip
import json
import math
import sys
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from datasets import load_dataset
import pandas as pd
import random
import glob

def download_pageviews(year):
    base_url = "https://dumps.wikimedia.org/other/pageviews/"
    start_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 1)
    current_date = start_date
    while current_date < end_date:
        year_str = current_date.strftime('%Y')
        month_str = current_date.strftime('%m')
        day_str = current_date.strftime('%d')
        hour_str = f"{random.randint(0, 23):02}"
        url = f"{base_url}{year_str}/{year_str}-{month_str}/pageviews-{year_str}{month_str}{day_str}-{hour_str}0000.gz"
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            dir_path = f"data/pageviews/{year_str}/{year_str}-{month_str}"
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            file_path = f"{dir_path}/pageviews-{year_str}{month_str}{day_str}-{hour_str}0000.gz"
            with open(file_path, 'wb') as f:
                f.write(response.raw.read())
            print(f"Downloaded {file_path}")
        else:
            print(f"Failed to download data for {url}")
        current_date += timedelta(days=1)

def process_pageviews(file_paths):
    title_views = {}
    for filepath in file_paths:
        with gzip.open(filepath, "rt") as fIn:
            for line in fIn:
                splits = line.strip().split()
                if len(splits) == 4:
                    lang, title, views, _ = splits
                    if lang == '""':
                        continue
                    lang = lang.lower()
                    if lang.endswith(".m"):
                        lang = lang[:-2]
                    if lang.count(".") > 0:
                        continue
                    if lang not in title_views:
                        title_views[lang] = {}
                    if title not in title_views[lang]:
                        title_views[lang][title] = 0.0
                    title_views[lang][title] += math.log(int(views) + 1)
    return title_views

def save_results(title_views):
    dir_path = "data/pageviews_summary"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    for lang, views in title_views.items():
        with open(f"{dir_path}/{lang}.json", "w") as fOut:
            fOut.write(json.dumps(views))

def main():
    for year in range(2015, 2025):
        print("STARTING TO DOWNLOAD YEAR: ", year)
        download_pageviews(year)
    
    file_paths = []
    for year in range(2015, 2025):
        for month in range(1, 13):
            folder_path = f"data/pageviews/{year}/{year}-{month:02}"
            file_paths.extend(glob.glob(f"{folder_path}/*.gz"))


    title_views = process_pageviews(file_paths)
    save_results(title_views)

if __name__ == "__main__":
    main()