from datasets import load_dataset
import urllib.parse
import json
import random
from dotenv import load_dotenv

load_dotenv(override=True)

seed = int.from_bytes(b"ellamind")

ds = load_dataset("wikimedia/wikipedia", "20231101.de", split="train")


with open("url_to_rank.json") as f:
    url_to_rank = json.load(f)


def normalize_url(url):
    # Parse the URL to components
    parsed_url = urllib.parse.urlparse(url)
    # Normalize the path component
    normalized_path = urllib.parse.quote(urllib.parse.unquote(parsed_url.path))
    # Reconstruct the URL with the normalized path
    normalized_url = urllib.parse.urlunparse(
        (parsed_url.scheme, parsed_url.netloc, normalized_path, parsed_url.params, parsed_url.query, parsed_url.fragment)
    )
    return normalized_url

def filter_url(url):
    is_in_rank = url in url_to_rank.keys()
    return is_in_rank

def map_rank(url):
    qid_and_rank = url_to_rank.get(url, None)
    if qid_and_rank is None:
        return {"qid": None, "rank": None}
    qid = qid_and_rank["qid"]
    rank = qid_and_rank["rank"]
    return {"qid": qid, "rank": rank}


# breakpoint()
ds = ds.map(lambda example: {**example, "url": normalize_url(example["url"])})
ds = ds.filter(lambda example: filter_url(example["url"]))
ds = ds.map(lambda example: {**example, **map_rank(example["url"])})
ds = ds.sort("rank", reverse=True)
ds = ds.select(range(10000))
ds = ds.shuffle(seed=seed).select(range(2500))
ds.push_to_hub("ellamind/wikipedia-filtered-de", private=True)
