import time
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

def save_results(title_views, lang="de"):
    dir_path = "data/pageviews_summary"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    # for lang, views in title_views.items():
    #     with open(f"{dir_path}/{lang}.json", "w") as fOut:
    #         fOut.write(json.dumps(views))

    breakpoint()
    views = title_views[lang]
    with open(f"{dir_path}/{lang}.json", "w") as fOut:
        fOut.write(json.dumps(views))

# def main():
#     # for year in range(2015, 2025):
#     #     print("STARTING TO DOWNLOAD YEAR: ", year)
#     #     download_pageviews(year)
    
#     file_paths = []
#     for year in range(2015, 2025):
#         for month in range(1, 13):
#             folder_path = f"data/pageviews/{year}/{year}-{month:02}"
#             file_paths.extend(glob.glob(f"{folder_path}/*.gz"))


#     title_views = process_pageviews(file_paths)
#     save_results(title_views)

# if __name__ == "__main__":
#     main()

def combine_results(shared_dict, result, lang="de"):
    # for lang, titles in result.items():

    titles = result[lang]
    if lang not in shared_dict:
        # shared_dict[lang] = Manager().dict()
        shared_dict[lang] = {}
    for title, views in titles.items():
        print("COMBINING: ", title)
        if title not in shared_dict[lang]:
            shared_dict[lang][title] = views
        else:
            shared_dict[lang][title] += views

def worker(file_paths):
    return process_pageviews(file_paths)

def main():
    file_paths = []
    # years = range(2015, 2025)
    years = range(2015, 2016)
    for year in years:
        # for month in range(1, 13):
        for month in range(5, 6):
            folder_path = f"data/pageviews/{year}/{year}-{month:02}"
            file_paths.extend(glob.glob(f"{folder_path}/*.gz"))

    # Number of processes to use
    num_processes = 16  # Adjust based on your CPU

    # Split file_paths into chunks for each process
    chunks = [file_paths[i::num_processes] for i in range(num_processes)]

    # Create a multiprocessing pool
    # with Pool(processes=num_processes) as pool:
    #     results = pool.map(worker, chunks)

    with open("data/pageviews/gathered_views.pkl", "rb") as f:
        results = pickle.load(f)

    # Pickle results
    # print("PICKLING RESULTS")
    # with open("data/pageviews/gathered_views.pkl", "wb") as f:
    #     pickle.dump(results, f)

    # # Combine results from all processes
    # print("COMBINING RESULTS")
    # combined_title_views = {}
    # for result in results:
    #     for lang in result:
    #         if lang not in combined_title_views:
    #             combined_title_views[lang] = {}
    #         for title, views in result[lang].items():
    #             print("COMBINING: ", title)
    #             if title not in combined_title_views[lang]:
    #                 combined_title_views[lang][title] = views
    #             else:
    #                 combined_title_views[lang][title] += views

    # Create a manager dictionary to store combined results
    manager = Manager()
    combined_title_views = manager.dict()

    tick = time.time()
    # Use a pool to combine results in parallel
    with Pool(processes=num_processes) as pool:
        pool.starmap(combine_results, [(combined_title_views, result) for result in results])
    tock = time.time()
    print("TIME FOR COMBINING: ", tock - tick)

    print("PICKLING COMBINED RESULTS")
    with open("data/pageviews/combined_views.pkl", "wb") as f:
        pickle.dump(combined_title_views, f)

    # Save results in parallel
    # print("SAVING RESULTS")
    # with Pool(processes=num_processes) as pool:
    #     pool.starmap(save_results, [(views,) for views in combined_title_views.values()])

if __name__ == "__main__":
    main()