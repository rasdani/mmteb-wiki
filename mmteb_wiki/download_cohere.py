from functools import partial
from datasets import load_dataset, Dataset

# languages = ["it", "pt", "ru", "uk", "nl", "cs", "ro", "bg", "sr", "fi", "fa", "bn", "hi"]
# languages = ["hi"]
languages = ["it", "pt", "ru", "uk", "nl", "cs", "ro", "bg", "sr", "fi", "fa"][::-1]

def gen_from_iterable_dataset(iterable_ds):
    yield from iterable_ds

for lang in languages:
    try:
        print("DOWNLOADING", lang)
        docs = load_dataset("Cohere/wikipedia-2023-11-embed-multilingual-v3-int8-binary", lang, split="train", streaming=True)
        docs = docs.remove_columns(['emb_int8', 'emb_ubinary'])

        ds = Dataset.from_generator(partial(gen_from_iterable_dataset, docs), features=docs.features)
        ds.push_to_hub(f"rasdani/cohere-wikipedia-2023-11-{lang}")
        print("UPLOADED", lang)
    except Exception as e:
        print("ERROR", lang, e)
