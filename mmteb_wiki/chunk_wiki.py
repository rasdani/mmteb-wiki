from llama_index.core.schema import NodeRelationship
from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter
from datasets import Dataset, load_dataset
from transformers import AutoTokenizer
import spacy
from dotenv import load_dotenv

load_dotenv(override=True)

embedding_id = "intfloat/multilingual-e5-small" # model with smallest token limit so far
tokenizer = AutoTokenizer.from_pretrained(embedding_id)
if not spacy.util.is_package("de_core_news_lg"):
    spacy.cli.download("de_core_news_lg")

nlp = spacy.load("de_core_news_lg")
def sentence_tokenizer(text):
    # Use the model to tokenize the text into sentences
    doc = nlp(text)
    sentences = [sentence.text for sentence in doc.sents]
    return sentences

ds = load_dataset("rasdani/wikipedia-filtered-de", split="train")
# ds.push_to_hub("rasdani/wikipedia-filtered-de")
# breakpoint()

#     features: ['id', 'url', 'title', 'text', 'qid', 'rank'],
docs = [Document(id_=i, text=example["text"], metadata={"url": example["url"], "id": example["id"], "title": example["title"], "qid": example["qid"], "rank": example["rank"]}) for i, example in enumerate(ds)]

embedding_id = "intfloat/multilingual-e5-small" # model with smallest token limit so far
tokenizer = AutoTokenizer.from_pretrained(embedding_id)
chunk_size = 512
# splitter = SentenceSplitter(chunk_size=chunk_size, chunk_overlap=0, tokenizer=lambda x: tokenizer(x)["input_ids"], chunking_tokenizer_fn=sentence_tokenizer)
splitter = SentenceSplitter(chunk_size=chunk_size, chunk_overlap=0, tokenizer=lambda x: tokenizer(x)["input_ids"], paragraph_separator="\n\n")

nodes = splitter.get_nodes_from_documents(docs)
nodes = nodes[:4000]

# by default, the node ids are set to random uuids. To ensure same id's per run, we manually set them.
for idx, node in enumerate(nodes):
    # node.id_ = f"node_{idx}"
    node.id_ = idx

node_ids = [node.id_ for node in nodes]
doc_ids = [node.relationships[NodeRelationship.SOURCE].node_id for node in nodes]
ids = [node.metadata["id"] for node in nodes]
urls = [node.metadata["url"] for node in nodes]
texts = [node.text for node in nodes]
ranks = [node.metadata["rank"] for node in nodes]
titles = [node.metadata["title"] for node in nodes]
qids = [node.metadata["qid"] for node in nodes]
data_dict = {"node_id": node_ids, "doc_id": doc_ids, "url": urls, "titles": titles, "text": texts, "rank": ranks, "qid": qids}

ds_chunks = Dataset.from_dict(data_dict)
ds_chunks = ds_chunks.filter(lambda x: len(x["text"]) > 256)
ds_chunks.push_to_hub("rasdani/wikipedia-chunks-de")
