"""RAG over your documents with Docling for IBM watsonx and watsonx.ai.

Script version of rag.ipynb:

  1. convert the documents on Docling for IBM watsonx,
  2. chunk them locally with the HybridChunker,
  3. embed the chunks with watsonx.ai (granite-embedding-278m-multilingual),
  4. keep the vectors in memory (NumPy, cosine similarity),
  5. answer questions with a watsonx.ai chat model, citing the passages.

    uv run rag.py
    uv run rag.py --question "How many pages does DocLayNet contain?"
    uv run rag.py --source https://arxiv.org/pdf/2408.09869 --source my.pdf
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

DEFAULT_SOURCES = [
    "https://arxiv.org/pdf/2408.09869",  # Docling technical report
    "https://arxiv.org/pdf/2206.01062",  # DocLayNet
]
DEFAULT_QUESTIONS = [
    "How many pages does DocLayNet contain, and how were they annotated?",
    "Which AI models does Docling use for layout analysis and table structure?",
    "What is the capital of France?",  # not in the documents: the model should say so
]

EMBEDDING_MODEL = "ibm/granite-embedding-278m-multilingual"
EMBEDDING_TOKENIZER = "ibm-granite/granite-embedding-278m-multilingual"
EMBEDDING_MAX_TOKENS = 512
CHAT_MODEL = "ibm/granite-4-h-small"

SYSTEM_PROMPT = (
    "You answer questions using only the numbered passages provided. "
    "Cite the passages you use as [1], [2], ... "
    "If the passages do not contain the answer, say that you cannot answer from the documents."
)


def load_env() -> None:
    """Load .env files from this folder up to the repository root (the nearest one wins)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    here = Path(__file__).resolve().parent
    for folder in (here, *here.parents[:2]):
        if (folder / ".env").is_file():
            load_dotenv(folder / ".env", override=False)


def require_env(*names: str) -> str:
    """Return the first variable that is set among `names`, or exit with a message."""
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    sys.exit(f"Set {names[0]} (environment or .env).")


# --- 1. Convert ---------------------------------------------------------------


def convert(sources: list[str]) -> dict:
    from docling.service_client import DoclingServiceClient

    documents = {}
    with DoclingServiceClient(
        url=require_env("DOCLING_SERVICE_URL"),
        api_key=require_env("DOCLING_SERVICE_API_KEY"),
    ) as client:
        for source in sources:
            src = source if source.startswith(("http://", "https://")) else Path(source)
            result = client.convert(source=src)
            documents[source] = result.document
            print(f"{result.status.value:10s} {source}")
    return documents


# --- 2. Chunk -----------------------------------------------------------------


def chunk(documents: dict) -> list[dict]:
    from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
    from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer

    # Use the tokenizer of the embedding model and its input limit,
    # otherwise watsonx.ai truncates long chunks.
    tokenizer = HuggingFaceTokenizer.from_pretrained(
        model_name=EMBEDDING_TOKENIZER,
        max_tokens=EMBEDDING_MAX_TOKENS,
    )
    chunker = HybridChunker(tokenizer=tokenizer)

    chunks = []
    for source, doc in documents.items():
        for ch in chunker.chunk(dl_doc=doc):
            pages = sorted({prov.page_no for item in ch.meta.doc_items for prov in item.prov})
            chunks.append(
                {
                    "source": source,
                    "headings": ch.meta.headings or [],
                    "pages": pages,
                    # The contextualized text starts with the chunk's headings.
                    # It embeds better than the bare chunk text.
                    "text": chunker.contextualize(chunk=ch),
                }
            )
    print(f"{len(chunks)} chunks")
    return chunks


# --- 3 + 4. Embed and index -----------------------------------------------------


class Index:
    """In-memory vector index: a normalized NumPy matrix and a dot product."""

    def __init__(self, chunks: list[dict], credentials, project_id: str):
        import numpy as np
        from ibm_watsonx_ai.foundation_models import Embeddings
        from ibm_watsonx_ai.metanames import EmbedTextParamsMetaNames

        self.np = np
        self.chunks = chunks
        self.embedder = Embeddings(
            model_id=EMBEDDING_MODEL,
            credentials=credentials,
            project_id=project_id,
            # Docling and watsonx.ai count tokens slightly differently: truncate
            # instead of failing on a chunk at the limit.
            params={EmbedTextParamsMetaNames.TRUNCATE_INPUT_TOKENS: EMBEDDING_MAX_TOKENS},
        )
        vectors = np.array(self.embedder.embed_documents([c["text"] for c in chunks]))
        self.vectors = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
        print(f"Index: {self.vectors.shape}")  # (number of chunks, 768)

    def retrieve(self, question: str, k: int = 5) -> list[dict]:
        """Return the k chunks closest to the question."""
        np = self.np
        query = np.array(self.embedder.embed_query(question))
        query /= np.linalg.norm(query)
        scores = self.vectors @ query
        return [self.chunks[i] for i in np.argsort(-scores)[:k]]


# --- 5. Answer ------------------------------------------------------------------


def ask(llm, index: Index, question: str, k: int = 5) -> str:
    hits = index.retrieve(question, k=k)
    context = "\n\n".join(f"[{i}] {hit['text']}" for i, hit in enumerate(hits, start=1))
    response = llm.chat(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Passages:\n{context}\n\nQuestion: {question}"},
        ],
        params={"temperature": 0, "max_tokens": 512},
    )
    answer = response["choices"][0]["message"]["content"]

    print(f"\nQ: {question}\n")
    print(f"A: {answer}\n")
    for i, hit in enumerate(hits, start=1):
        section = " > ".join(hit["headings"]) or "(no heading)"
        print(f"  [{i}] {hit['source']}, {section}, pages {hit['pages']}")
    return answer


def main() -> int:
    parser = argparse.ArgumentParser(
        description="RAG over documents with Docling for IBM watsonx and watsonx.ai."
    )
    parser.add_argument(
        "--source",
        action="append",
        help="URL or local file to index (repeatable). Default: two arXiv papers.",
    )
    parser.add_argument(
        "--question",
        action="append",
        help="Question to ask (repeatable). Default: three sample questions.",
    )
    parser.add_argument("-k", type=int, default=5, help="Passages per question (default: 5)")
    args = parser.parse_args()

    load_env()
    from ibm_watsonx_ai import Credentials
    from ibm_watsonx_ai.foundation_models import ModelInference

    credentials = Credentials(
        url=require_env("WATSONX_URL"),
        api_key=require_env("WATSONX_APIKEY", "WATSONX_API_KEY"),
    )
    project_id = require_env("WATSONX_PROJECT_ID")

    documents = convert(args.source or DEFAULT_SOURCES)
    chunks = chunk(documents)
    index = Index(chunks, credentials, project_id)
    llm = ModelInference(model_id=CHAT_MODEL, credentials=credentials, project_id=project_id)

    for question in args.question or DEFAULT_QUESTIONS:
        ask(llm, index, question, k=args.k)
    return 0


if __name__ == "__main__":
    sys.exit(main())
