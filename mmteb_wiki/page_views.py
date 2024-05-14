import time
from tqdm import tqdm
import pickle
from multiprocessing import Pool, Manager
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
        print("PROCESSING: ", filepath)
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
                    try:
                        title_views[lang][title] += math.log(int(views) + 1)
                    except:
                        print("ERROR PROCESSING: ", filepath)
    return title_views

def save_results(title_views, year, lang="de"):
    dir_path = "data/pageviews_summary"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    # for lang, views in title_views.items():
    #     with open(f"{dir_path}/{lang}.json", "w") as fOut:
    #         fOut.write(json.dumps(views))

    # breakpoint()
    # views = title_views[lang]
    with open(f"{dir_path}/{lang}_{year}.json", "w") as fOut:
        fOut.write(json.dumps(title_views))

def combine_results(shared_dict, result, year, languages=["de", "it", "pt", "ru", "uk", "nl", "cs", "ro", "bg", "sr", "fi", "fa", "bn", "hi"]):
    # for lang, titles in result.items():

    for lang in languages:
        titles = result[lang]
        if lang not in shared_dict:
            # shared_dict[lang] = Manager().dict()
            shared_dict[lang] = {}
        for title, views in titles.items():
            print(f"COMBINING YEAR {year} for {lang}:", title)
            if title not in shared_dict[lang]:
                shared_dict[lang][title] = views
            else:
                shared_dict[lang][title] += views

    return shared_dict

def worker(file_paths):
    return process_pageviews(file_paths)

def main():
    num_processes = 16  # Adjust based on your CPU

    years = range(2015, 2025)
    # years = range(2015, 2016)
    # years = range(2015, 2017)
    for year in years:
        file_paths = []
        for month in range(1, 13):
        # for month in range(5, 6):
            folder_path = f"data/pageviews/{year}/{year}-{month:02}"
            file_paths.extend(glob.glob(f"{folder_path}/*.gz"))


        # Split file_paths into chunks for each process
        chunks = [file_paths[i::num_processes] for i in range(num_processes)]

        # Create a multiprocessing pool
        # with Pool(processes=num_processes) as pool:
        #     results = pool.map(worker, chunks)

        print("LOADING PICKLE FOR", year)
        with open(f"data/pageviews/gathered_views_{year}.pkl", "rb") as f:
            results = pickle.load(f)

        # Pickle results
        # print(f"PICKLING RESULTS FOR {year}")
        # with open(f"data/pageviews/gathered_views_{year}.pkl", "wb") as f:
        #     pickle.dump(results, f)

        # Create a manager dictionary to store combined results
        # manager = Manager()
        # combined_title_views = manager.dict()
        # print("DICT: ", combined_title_views)
        # breakpoint()

        tick = time.time()
        # Use a pool to combine results in parallel
        # with Pool(processes=num_processes) as pool:
        #     combined_title_views = pool.starmap(combine_results, [(combined_title_views, result, year) for result in results])

        
        combined_title_views = {}
        for result in tqdm(results):
            combined_title_views = combine_results(combined_title_views, result, year)

        tock = time.time()
        # print("DICT: ", combined_title_views)
        print("TIME FOR COMBINING: ", tock - tick)

        # # Convert DictProxy to a regular dictionary
        # if isinstance(combined_title_views, Manager().dict().__class__):
        #     combined_title_views = dict(combined_title_views)

        # print(f"PICKLING COMBINED RESULTS FOR {year}")
        # with open(f"data/pageviews/combined_views_{year}.pkl", "wb") as f:
        #     pickle.dump(combined_title_views, f)

        print("SAVING RESULTS FOR", year)
        save_results(combined_title_views, year)

        # Save results in parallel
        # print("SAVING RESULTS")
        # with Pool(processes=num_processes) as pool:
        #     pool.starmap(save_results, [(views,) for views in combined_title_views.values()])

if __name__ == "__main__":
    main()