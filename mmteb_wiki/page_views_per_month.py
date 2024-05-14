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


def combine_results(shared_dict, result, year, languages=["de", "it", "pt", "ru", "uk", "nl", "cs", "ro", "bg", "sr", "fi", "fa", "bn", "hi"]):
    for lang in languages:
        titles = result[lang]
        if lang not in shared_dict:
            # shared_dict[lang] = Manager().dict()
            shared_dict[lang] = {}
        for title, views in titles.items():
            # print(f"COMBINING YEAR {year} for {lang}:", title)
            if title not in shared_dict[lang]:
                shared_dict[lang][title] = views
            else:
                shared_dict[lang][title] += views

    return shared_dict

def worker(file_paths):
    return process_pageviews(file_paths)

def save_results(title_views, year, month):
    dir_path = f"data/pageviews_summary/{year}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(f"{dir_path}/{month:02}.json", "w") as fOut:
        fOut.write(json.dumps(title_views))

def main():
    num_processes = 16  # Adjust based on your CPU

    years = range(2015, 2025)
    for year in years:
        for month in range(1, 13):
            tick = time.time()
            # Gather file paths for the downloaded files
            folder_path = f"data/pageviews/{year}/{year}-{month:02}"
            file_paths = glob.glob(f"{folder_path}/*.gz")
            if len(file_paths) == 0:
                continue

            # Split file_paths into chunks for each process
            chunks = [file_paths[i::num_processes] for i in range(num_processes)]

            # Process the pageviews in parallel
            with Pool(processes=num_processes) as pool:
                results = pool.map(worker, chunks)

            # Combine results
            combined_title_views = {}
            for result in tqdm(results):
                combined_title_views = combine_results(combined_title_views, result, year)

            for lang, titles in combined_title_views.items():
                print(f"{year} {month} for {lang}: {len(titles)} titles")
            # Save combined results for the month
            save_results(combined_title_views, year, month)
            tock = time.time()
            print(f"Time taken for {year}-{month:02}: {tock-tick} seconds")

if __name__ == "__main__":
    main()