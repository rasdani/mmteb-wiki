from datasets import load_dataset
import json

def add_views(entry):
    return {"views": pageviews_de.get(entry["title"], 0)}

with open("data/pageviews_summary/final_total_views.json") as fIn:
    pageviews = json.load(fIn)
    pageviews_de = pageviews["de"]

ds = load_dataset("rasdani/cohere-wikipedia-2023-11-de", split="train")
breakpoint()

# ds_views = ds.map(lambda x: {"views": pageviews_de.get(x["title"], 0)})
ds_views = ds.map(add_views)

ds_views_sorted = ds_views.sort("views", reverse=True)

ds_views_top100k = ds_views_sorted.select(range(100000))

ds_views_top100k.push_to_hub("rasdani/cohere-wikipedia-2023-11-de-top100k-views")
