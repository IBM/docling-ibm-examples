"""Send documents to Docling for IBM watsonx and stream their chunks to Confluent Cloud.

One batch request: the service converts every URL, chunks it, and writes one
Kafka message per chunk to the `docling_chunks` topic.

    uv run send.py                                   # the Docling technical report
    uv run send.py https://arxiv.org/pdf/2206.01062 https://example.com/report.pdf
"""

import os
import sys
from pathlib import Path

from docling.datamodel.service.chunking import HybridChunkerOptions
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.requests import AnyHttpSourceRequest
from docling.service_client import DoclingServiceClient, GenericTargetRequest
from dotenv import load_dotenv

# Load .env files from this folder up to the repository root (the nearest value wins).
for folder in (Path.cwd(), *Path.cwd().parents):
    load_dotenv(folder / ".env", override=False)

urls = sys.argv[1:] or ["https://arxiv.org/pdf/2408.09869"]

target = GenericTargetRequest(
    kind="kafka_chunks",
    bootstrap_servers=[os.environ["KAFKA_BOOTSTRAP_SERVER"]],
    topic=os.environ.get("KAFKA_TOPIC", "docling_chunks"),
    auth={
        "kind": "sasl",
        "mechanism": "PLAIN",
        "username": os.environ["KAFKA_API_KEY"],
        "password": os.environ["KAFKA_API_SECRET"],
    },
    headings_field="headings",
    page_field="page_numbers",
)

# Always set chunker="hybrid" explicitly: the service rejects chunking options without it.
options = ConvertDocumentsOptions(
    chunking_options=HybridChunkerOptions(chunker="hybrid", max_tokens=512)
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
    result = job.result(timeout=1800)

print(f"{result.num_succeeded}/{result.num_converted} documents streamed to Kafka")
