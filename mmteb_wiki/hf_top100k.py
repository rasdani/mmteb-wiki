from datasets import load_dataset, Dataset
import json
import random

# Set a seed for reproducibility
seed = int.from_bytes(b"ellamind") % 1337
random.seed(seed)

def add_views(entry, ds_pageviews):
    return {"views": ds_pageviews[entry["title"]] if entry["title"] in ds_pageviews else 0}

def select_random_window(group):
    if len(group) >= 9:
        start_index = random.randint(0, len(group) - 9)
        return group.iloc[start_index:start_index + 9]
    return None

# languages = ["de", "it", "pt", "ru", "uk", "nl", "cs", "ro", "bg", "sr", "fi", "fa", "bn", "hi"]
languages = ["de"]

for lang in languages:
    with open(f"data/pageviews_summary/final_total_views.json") as fIn:
        pageviews = json.load(fIn)
        breakpoint()
        pageviews_lang = [{"title": key, "views": value} for key, value in pageviews[lang].items()]
        ds_pageviews = Dataset.from_list(pageviews_lang).sort("views", reverse=True)
        ds_pageviews_top100k = ds_pageviews.select(range(100000))

    ds = load_dataset(f"rasdani/cohere-wikipedia-2023-11-{lang}", split="train")
    ds = ds.filter(lambda x: x["title"] in set(ds_pageviews_top100k["title"]))
    df = ds.to_pandas()

    grouped_df = df.groupby('title', sort=False).apply(select_random_window).dropna()
    filtered_ds = Dataset.from_pandas(grouped_df.reset_index(drop=True))
    ds_views = ds.map(lambda x: add_views(x, ds_pageviews_top100k))
    ds_views = ds_views.filter(lambda x: x["views"] > 0)

    ds_views_sorted = ds_views.sort("views", reverse=True)
    ds_views_sorted.push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}-top100k-views")

    # ds_views_top100k = ds_views_sorted.select(range(100000))
    # ds_views_top100k.push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}-top100k-views")