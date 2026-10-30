"""Convert, chunk and index documents into AstraDB with one Docling for IBM watsonx batch request.

AstraDB embeds every chunk itself (vectorize). The script creates the collection if it
does not exist yet: the service writes into an existing collection and does not create one.

    uv run ingest.py                                     # three public sample documents
    uv run ingest.py https://example.com/a.pdf https://example.com/b.pdf
"""

import os
import sys
from pathlib import Path

from astrapy import DataAPIClient
from astrapy.info import CollectionDefinition
from docling.datamodel.service.chunking import HybridChunkerOptions
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.requests import AnyHttpSourceRequest
from docling.service_client import DoclingServiceClient, GenericTargetRequest
from dotenv import load_dotenv

# Load .env files from this folder up to the repository root (the nearest value wins).
for folder in (Path.cwd(), *Path.cwd().parents):
    load_dotenv(folder / ".env", override=False)

urls = sys.argv[1:] or [
    "https://arxiv.org/pdf/2408.09869",  # Docling technical report
    "https://arxiv.org/pdf/2206.01062",  # DocLayNet
    "https://arxiv.org/pdf/2501.17887",  # Docling: an efficient open-source toolkit
]

COLLECTION = os.environ.get("ASTRA_DB_COLLECTION", "docling_chunks")
VECTORIZE_PROVIDER = "nvidia"
VECTORIZE_MODEL = "nvidia/nv-embedqa-e5-v5"

# Create the collection with server-side embeddings (vectorize), unless it already exists.
database = DataAPIClient().get_database(
    os.environ["ASTRA_DB_API_ENDPOINT"], token=os.environ["ASTRA_DB_TOKEN"]
)
if COLLECTION not in database.list_collection_names():
    database.create_collection(
        COLLECTION,
        definition=CollectionDefinition.builder()
        .set_vector_service(VECTORIZE_PROVIDER, VECTORIZE_MODEL)
        .build(),
    )
    print(f"Created the AstraDB collection {COLLECTION}")

target = GenericTargetRequest(
    kind="astradb_chunks",
    api_endpoint=os.environ["ASTRA_DB_API_ENDPOINT"],
    token=os.environ["ASTRA_DB_TOKEN"],
    collection_name=COLLECTION,
    vectorize_provider=VECTORIZE_PROVIDER,
    vectorize_model=VECTORIZE_MODEL,
    headings_field="headings",
    page_field="page_numbers",
)

# Always set chunker="hybrid" explicitly: the service rejects chunking options without it.
# The tokenizer matches the embedding model. max_tokens stays below the model's 512-token
# limit: the headings are added to each chunk, and AstraDB rejects chunks that are too long.
options = ConvertDocumentsOptions(
    chunking_options=HybridChunkerOptions(
        chunker="hybrid",
        tokenizer="intfloat/e5-large-unsupervised",
        max_tokens=384,
    )
)

with DoclingServiceClient(
    url=os.environ["DOCLING_SERVICE_URL"],
    api_key=os.environ["DOCLING_SERVICE_API_KEY"],
) as client:
    job = client.submit_batch(
        sources=[AnyHttpSourceRequest(url=url) for url in urls],
        target=target,
        options=options,
    )
    print(f"Task {job.task_id}: converting {len(urls)} document(s) ...")
    result = job.result(timeout=3600)

print(f"{result.num_succeeded}/{result.num_converted} documents indexed")
if result.num_failed:
    sys.exit(f"{result.num_failed} document(s) failed. Check the URLs and the AstraDB endpoint and token.")
print('Next: uv run query.py "your question"')
