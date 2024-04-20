EMBEDDING_MODELS = [
    "intfloat/multilingual-e5-small",
    "intfloat/multilingual-e5-base",
    "intfloat/multilingual-e5-large",
    "thenlper/gte-small",
    "thenlper/gte-base",
    "thenlper/gte-large",
    "jinaai/jina-embeddings-v2-base-de",
    "deepset/gbert-large",
    "deutsche-telekom/gbert-large-paraphrase-cosine",
    "mixedbread-ai/mxbai-embed-large-v1",
]

EMBEDDING_MODEL_CONFIGS = {
    "intfloat/multilingual-e5-small": {
        "query_instruction": "query: ",
        "text_instruction": "passage: ",
        "pooling": "mean",
        "trust_remote_code": False,
    },
    "intfloat/multilingual-e5-base": {
        "query_instruction": "query: ",
        "text_instruction": "passage: ",
        "pooling": "mean",
        "trust_remote_code": False,
    },
    "intfloat/multilingual-e5-large": {
        "query_instruction": "query: ",
        "text_instruction": "passage: ",
        "pooling": "mean",
        "trust_remote_code": False,
    },
    "jinaai/jina-embeddings-v2-base-de": {
        "query_instruction": None,
        "text_instruction": None,
        "pooling": "mean",
        "trust_remote_code": True,
    },
    "deepset/gbert-large": {
        "query_instruction": None,
        "text_instruction": None,
        "pooling": "mean",
        "trust_remote_code": False,
    },
    "deutsche-telekom/gbert-large-paraphrase-cosine": {
        "query_instruction": None,
        "text_instruction": None,
        "pooling": "mean",
        "trust_remote_code": False,
    },
    "mixedbread-ai/mxbai-embed-large-v1": {
        "query_instruction": "Represent this sentence for searching relevant passages: ",
        "text_instruction": None,
        "pooling": "cls",
        "trust_remote_code": False,
    },
    "thenlper/gte-small": {
        "query_instruction": None,
        "text_instruction": None,
        "pooling": "mean",
        "trust_remote_code": False,
    },
    "thenlper/gte-base": {
        "query_instruction": None,
        "text_instruction": None,
        "pooling": "mean",
        "trust_remote_code": False,
    },
    "thenlper/gte-large": {
        "query_instruction": None,
        "text_instruction": None,
        "pooling": "mean",
        "trust_remote_code": False,
    },
}
