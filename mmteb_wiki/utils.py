from datasets import Dataset, load_dataset
from llama_index.core import Document, Settings
from llama_index.core.evaluation import RetrieverEvaluator, EmbeddingQAFinetuneDataset
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from typing import Dict, List



def load_and_prepare_datasets():
    ds1 = load_dataset("rasdani/germanrag-positives", split="train")
    ds2 = load_dataset("rasdani/germanrag-positives-queries", split="train")
    return ds1, ds2


def create_documents_from_dataset(ds):
    return [
        Document(id_=i, text=example["contexts"][example["positive_ctx_idx"]])
        for i, example in enumerate(ds)
    ]


def prepare_embedding_qa_dataset(
    ds, query_key="question"
) -> EmbeddingQAFinetuneDataset:
    queries: Dict[str, str] = {}
    corpus: Dict[str, str] = {}
    relevant_docs: Dict[str, List[str]] = {}

    for i, entry in enumerate(ds):
        query_id = str(i)
        doc_id = str(i)
        query = entry[query_key]
        doc = entry["contexts"][entry["positive_ctx_idx"]]

        queries[query_id] = query
        corpus[doc_id] = doc
        relevant_docs.setdefault(query_id, []).append(doc_id)

    return EmbeddingQAFinetuneDataset(
        queries=queries, corpus=corpus, relevant_docs=relevant_docs, mode="text"
    )


def setup_embedding_model(model_id, config):
    Settings.embed_model = HuggingFaceEmbedding(
        model_name=model_id,
        embed_batch_size=4,
        query_instruction=config["query_instruction"],
        text_instruction=config["text_instruction"],
        trust_remote_code=config["trust_remote_code"],
    )
