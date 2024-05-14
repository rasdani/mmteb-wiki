from datasets import load_dataset, Dataset
import json
import random

# Set a seed for reproducibility
seed = int.from_bytes(b"ellamind") % 1337 # == 1114
print(f"SEED: {seed}")
random.seed(seed)

def add_views(entry, pageviews_top100k_dict):
    return {"views": pageviews_top100k_dict.get(entry["title"], 0)}

def select_random_window(group):
    if len(group) >= 9:
        start_index = random.randint(0, len(group) - 9)
        return group.iloc[start_index:start_index + 9]
    return None

# Define a function to select a window of 9 consecutive paragraphs and assign scores
def assign_scores(group):
    if len(group) >= 9:
        scores = [0.5, 0.5, 0.5, 0.5, 1.0, 0.5, 0.5, 0.5, 0.5]  # Scores for the paragraphs
        group['score'] = scores
        return group
    return None

# languages = ["de", "it", "pt", "ru", "uk", "nl", "cs", "ro", "bg", "sr", "fi", "fa", "bn", "hi"]
# languages = ["de"]
# languages = ["bn"]
# languages = ["de", "bn"]
languages = ["it", "pt", "ru", "uk", "nl", "cs", "ro", "bg", "sr", "fi", "fa", "hi"]

print(f"LOADING PAGEVIEWS")
with open(f"data/pageviews_summary/final_total_views.json") as fIn:
    pageviews = json.load(fIn)
        
for lang in languages:
    try:
        print(f"KEEPING PAGEVIEWS FOR {lang}")
        pageviews_lang = [{"title": key, "views": value} for key, value in pageviews[lang].items()]
        ds_pageviews = Dataset.from_list(pageviews_lang).sort("views", reverse=True)
        ds_pageviews_top100k = ds_pageviews.select(range(100000))
        pageviews_top100k_dict = {entry["title"]: entry["views"] for entry in ds_pageviews_top100k}

        ds = load_dataset(f"rasdani/cohere-wikipedia-2023-11-{lang}", split="train")
        titles_set = set(pageviews_top100k_dict.keys())
        ds = ds.filter(lambda x: x["title"] in titles_set)
        df = ds.to_pandas()

        # Filter out articles with less than 10 paragraphs/occurrences
        df_filtered = df.groupby('title').filter(lambda x: len(x) >= 9)
        unique_titles = df_filtered['title'].unique()
        print(f"SAMPLING 2000 TITLES FROM {len(unique_titles)} TITLES")
        sampled_titles = random.sample(list(unique_titles), 1500)
        df_sampled = df_filtered[df_filtered['title'].isin(sampled_titles)]

        df_grouped = df_sampled.groupby('title', sort=False).apply(select_random_window).dropna()
        df_grouped = df_grouped.reset_index(drop=True)
        df_grouped = df_grouped.groupby('title', sort=False).apply(assign_scores).dropna()
        ds_filtered = Dataset.from_pandas(df_grouped.reset_index(drop=True))
        # breakpoint()
        ds_views = ds_filtered.map(lambda x: add_views(x, pageviews_top100k_dict))
        ds_views = ds_views.filter(lambda x: x["views"] > 0)

        ds_views_sorted = ds_views.sort("views", reverse=True)
        # ds_views_sorted.push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}-top100k-views")
        # ds_views_sorted.push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}-2k-articles")
        ds_views_sorted.push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}-1.5k-articles")
        ds_positives = ds_views_sorted.filter(lambda x: x["score"] == 1.0)
        ds_positives.push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}-1.5k-articles-positives")
        # ds_positives.select(range(20)).push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}-1.5k-articles-positives-debug")

    except Exception as e:
        print(f"ERROR WITH LANGUAGE {lang}: {e}")
