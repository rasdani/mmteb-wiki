import pickle
import asyncio
import os
import pandas as pd
from transformers import AutoTokenizer
from dotenv import load_dotenv
from llama_index.core import Document, Settings, VectorStoreIndex
from llama_index.core.node_parser import TokenTextSplitter
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.evaluation import RetrieverEvaluator, EmbeddingQAFinetuneDataset
from typing import Dict, List

from mmteb_wiki.config import EMBEDDING_MODELS
from mmteb_wiki.utils import (
    load_and_prepare_datasets,
    create_documents_from_dataset,
    prepare_embedding_qa_dataset,
)


# Load environment variables
load_dotenv(override=True)


# tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_ID)


async def evaluate_datasets(ds1, ds2):
    eval_dataset1 = prepare_embedding_qa_dataset(ds1, query_key="question")
    eval_dataset2 = prepare_embedding_qa_dataset(ds2, query_key="query")

    vector_index1 = VectorStoreIndex(
        create_documents_from_dataset(ds1), show_progress=True
    )
    vector_index2 = VectorStoreIndex(
        create_documents_from_dataset(ds2), show_progress=True
    )

    retriever1 = vector_index1.as_retriever(similarity_top_k=10)
    retriever2 = vector_index2.as_retriever(similarity_top_k=10)

    retrieval_evaluator1 = RetrieverEvaluator.from_metric_names(
        metric_names=["mrr"], retriever=retriever1
    )
    retrieval_evaluator2 = RetrieverEvaluator.from_metric_names(
        metric_names=["mrr"], retriever=retriever2
    )

    results1 = await retrieval_evaluator1.aevaluate_dataset(eval_dataset1)
    results2 = await retrieval_evaluator2.aevaluate_dataset(eval_dataset2)

    return results1, results2


def save_retrieval_results_with_pickle(results, embedding_id):
    dir_path = "results/retrieval"
    file_path = f"{dir_path}/retrieval_results_{embedding_id.replace('/', '_')}.pkl"

    os.makedirs(dir_path, exist_ok=True)
    with open(file_path, "wb") as file:
        pickle.dump(results, file)

    print(f"Results saved to {file_path}")


def save_results_to_dataframe(avg_mrr1, avg_mrr2, embedding_id):
    dir_path = "retrieval/mrr"
    file_path = f"{dir_path}/cumulative_results_{embedding_id.replace('/', '_')}.csv"

    os.makedirs(dir_path, exist_ok=True)

    new_data = pd.DataFrame(
        {
            "Model ID": [embedding_id],
            "MRR Gold Dataset": [avg_mrr1],
            "MRR Synthetic Queries": [avg_mrr2],
        }
    )

    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path)
        updated_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        updated_data = new_data

    updated_data.to_csv(file_path, index=False)
    print(f"Results saved to {file_path}")


def calculate_mrr(results1, results2):
    scores1 = [result.metric_dict["mrr"].score for result in results1]
    scores2 = [result.metric_dict["mrr"].score for result in results2]
    avg_mrr1 = sum(scores1) / len(scores1)
    avg_mrr2 = sum(scores2) / len(scores2)
    print(f"Average MRR Score Gold Dataset: {avg_mrr1}")
    print(f"Average MRR Score Synthetic Queries: {avg_mrr2}")


async def main():
    # ds1, ds2 = load_and_prepare_datasets()
    # setup_embedding_model()
    # results1, results2 = await evaluate_datasets(ds1, ds2)
    # calculate_mrr(results1, results2)
    ds1, ds2 = load_and_prepare_datasets()
    for embedding_id in EMBEDDING_MODELS:
        try:
            results1, results2 = await evaluate_datasets(ds1, ds2, embedding_id)
            calculate_mrr(results1, results2)
            save_results((results1, results2), embedding_id)
        except Exception as e:
            print(f"Error processing {embedding_id}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
