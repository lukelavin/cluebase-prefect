import time

import faiss
import pandas as pd
from datasets import load_dataset
from prefect import flow, task
from sentence_transformers import SentenceTransformer
from sentence_transformers.quantization import quantize_embeddings
from usearch.index import Index

from src.io_utils import get_s3_bucket


@flow(log_prints=True)
def add_relevant_links():
    document = {
        "_id": "000009-1-0-0",
        "clueText": "Te Papa, the National Museum of New Zealand, is in this city, the capital",
        "solution": "Wellington",
        "category": "NEW ZEALAND",
        "difficulty": 0,
        "airDate": "2004-09-16T00:00:00.000+0000",
        "gameId": "9",
        "roundNumber": 1,
        "categoryIndex": 0,
        "createdAt": "2024-11-03T00:16:18.598+0000",
        "updatedAt": "2024-11-03T00:16:18.598+0000",
    }

    download_indices()

    title_text_dataset, model, int8_view, binary_index = load_model_and_indices()

    print(
        search(
            'In an attempt to come to grips with Shakespeare\'s "Richard III", this actor directed "Looking for Richard": Al Pacino',
            title_text_dataset,
            model,
            binary_index,
            int8_view,
        )
    )


@task
def download_indices(bucket_name="cluebase"):
    # Download the model and indexes
    bucket = get_s3_bucket(bucket_name)

    bucket.download_object_to_path(
        "models/wikipedia_int8_usearch_1m.index", "wikipedia_int8_usearch_1m.index"
    )

    bucket.download_object_to_path(
        "models/wikipedia_ubinary_faiss_1m.index", "wikipedia_ubinary_faiss_1m.index"
    )


@task
def load_model_and_indices():

    # Load titles and texts
    title_text_dataset = load_dataset(
        "mixedbread-ai/wikipedia-data-en-2023-11", split="train", num_proc=4
    ).select_columns(["title", "text"])

    model = SentenceTransformer(
        "mixedbread-ai/mxbai-embed-large-v1",
        prompts={
            "retrieval": "Represent this sentence for searching relevant passages: ",
        },
        default_prompt_name="retrieval",
    )

    # Load the int8 and binary indices. Int8 is loaded as a view to save memory, as we never actually perform search with it.
    int8_view = Index.restore("wikipedia_int8_usearch_1m.index", view=True)
    binary_index: faiss.IndexBinaryFlat = faiss.read_index_binary(
        "wikipedia_ubinary_faiss_1m.index"
    )
    return title_text_dataset, model, int8_view, binary_index


def search(
    query,
    title_text_dataset,
    model,
    int8_view,
    binary_index,
    top_k: int = 100,
    rescore_multiplier: int = 1,
):
    # 1. Embed the query as float32
    start_time = time.time()
    query_embedding = model.encode(query)
    embed_time = time.time() - start_time

    # 2. Quantize the query to ubinary
    start_time = time.time()
    query_embedding_ubinary = quantize_embeddings(
        query_embedding.reshape(1, -1), "ubinary"
    )
    quantize_time = time.time() - start_time

    # 3. Search the binary index (either exact or approximate)
    index = binary_index
    start_time = time.time()
    _scores, binary_ids = index.search(
        query_embedding_ubinary, top_k * rescore_multiplier
    )
    binary_ids = binary_ids[0]
    search_time = time.time() - start_time

    # 4. Load the corresponding int8 embeddings
    start_time = time.time()
    int8_embeddings = int8_view[binary_ids].astype(int)
    load_time = time.time() - start_time

    # 5. Rescore the top_k * rescore_multiplier using the float32 query embedding and the int8 document embeddings
    start_time = time.time()
    scores = query_embedding @ int8_embeddings.T
    rescore_time = time.time() - start_time

    # 6. Sort the scores and return the top_k
    start_time = time.time()
    indices = scores.argsort()[::-1][:top_k]
    top_k_indices = binary_ids[indices]
    top_k_scores = scores[indices]
    top_k_titles, top_k_texts = zip(
        *[
            (title_text_dataset[idx]["title"], title_text_dataset[idx]["text"])
            for idx in top_k_indices.tolist()
        ]
    )
    df = pd.DataFrame(
        {
            "Score": [round(value, 2) for value in top_k_scores],
            "Title": top_k_titles,
            "Text": top_k_texts,
        }
    )
    sort_time = time.time() - start_time

    return df, {
        "Embed Time": f"{embed_time:.4f} s",
        "Quantize Time": f"{quantize_time:.4f} s",
        "Search Time": f"{search_time:.4f} s",
        "Load Time": f"{load_time:.4f} s",
        "Rescore Time": f"{rescore_time:.4f} s",
        "Sort Time": f"{sort_time:.4f} s",
        "Total Retrieval Time": f"{quantize_time + search_time + load_time + rescore_time + sort_time:.4f} s",
    }


if __name__ == "__main__":
    add_relevant_links.serve(name="local-generate-links")
