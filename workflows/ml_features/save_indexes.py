import contextlib
from contextvars import ContextVar

import numpy as np
from datasets import load_dataset
from datasets.utils.tqdm import disable_progress_bars
from faiss import IndexBinaryFlat, write_index_binary
from sentence_transformers.quantization import quantize_embeddings
from tqdm import tqdm
from usearch.index import Index

disable_progress_bars()

BATCH_SIZE = 10000
TOTAL_RECORDS = 41_500_000


def save_int8_index():
    dataset = load_dataset("mixedbread-ai/wikipedia-embed-en-2023-11", split="train")
    embeddings = np.array(dataset["emb"], dtype=np.float32)

    int8_embeddings = quantize_embeddings(embeddings, "int8")
    index = Index(ndim=1024, metric="ip", dtype="i8")
    index.add(np.arange(len(int8_embeddings)), int8_embeddings)
    index.save("wikipedia_int8_usearch_1m.index")


def save_binary_index():
    index = IndexBinaryFlat(1024)

    print("Loading batches into arrays for quantization")
    print(f"Total batches: {TOTAL_RECORDS // BATCH_SIZE + 1}")
    for i in tqdm(range(TOTAL_RECORDS // BATCH_SIZE + 1)):
        dataset = load_dataset(
            "mixedbread-ai/wikipedia-embed-en-2023-11",
            split=f"train[{i * BATCH_SIZE}:{(i+1)*10000}]",
        )
        print(f"Loaded dataset batch {i}")
        embeddings = np.array(dataset["emb"], dtype=np.float32)
        print(f"Embedding batch is in np array")
        ubinary_embeddings = quantize_embeddings(embeddings, "ubinary")
        index.add(ubinary_embeddings)

    write_index_binary(index, "wikipedia_ubinary_faiss_1m.index")


if __name__ == "__main__":
    save_binary_index()
