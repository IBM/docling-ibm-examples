# 03 · Ingest documents into a vector database

Convert, chunk and index documents into **AstraDB** with **one** Docling for IBM
watsonx batch request. The service downloads each document, converts it, chunks
it along its structure and writes every chunk, with its headings and page
numbers, into an AstraDB collection. AstraDB embeds the chunks itself, so no
code runs in between.

```
public URLs ──▶ Docling for IBM watsonx ──▶ AstraDB collection ──▶ vector search
                convert + hybrid chunk      astradb_chunks,          $vectorize
                                            server-side embeddings
```

| File | What it does |
|---|---|
| [`python/ingest.py`](python/ingest.py) | Creates the collection if needed, then one batch request: URL sources, `astradb_chunks` target, `"chunker": "hybrid"` |
| [`python/query.py`](python/query.py) | Vector search over the chunks; AstraDB embeds the question |

This folder accompanies the [Ingest a Document Library into a Vector Database](https://developer.dcls.saas.ibm.com/tutorials/vector-db-ingestion)
tutorial of the [Docling for IBM watsonx documentation](https://developer.dcls.saas.ibm.com/).

## Prerequisites

- **Docling for IBM watsonx**: the Service URL and an API key (Workbench, **API examples**).
- **An AstraDB database** (Serverless, vector) and an application token
  (`AstraCS:...`) with write access. `ingest.py` creates the collection on the first run.
- [`uv`](https://docs.astral.sh/uv/) (or Python 3.10+ and `pip install docling-client astrapy python-dotenv`).

## Run

```bash
git clone https://github.com/IBM/docling-ibm-examples
cd docling-ibm-examples
cp .env.example .env                       # DOCLING_SERVICE_URL, DOCLING_SERVICE_API_KEY
cp 03-vector-db-ingestion/.env.example 03-vector-db-ingestion/.env   # ASTRA_DB_*
cd 03-vector-db-ingestion/python

uv run ingest.py                           # three public papers
uv run query.py "How many pages does DocLayNet contain?"
```

`ingest.py` takes your own URLs as arguments. `query.py` prints the closest
chunks with their document, section headings, page numbers and the start of
their text: everything a RAG prompt needs to cite its sources.

Running `ingest.py` again does not duplicate chunks: every chunk has a
deterministic ID and is overwritten in place.

## Next steps

- **Your own document library.** Replace the URL sources with a connector, for
  example an S3 bucket (`S3SourceRequest` with endpoint, bucket, key prefix and
  credentials), SharePoint or Box. The service then reads every document under
  the prefix or folder. The [connector pages](https://developer.dcls.saas.ibm.com/connectors) of the documentation list every
  source.
- **Answer questions.** Pass the hits to a chat model, as in [`02-rag-notebook`](../02-rag-notebook).

## Clean up

Delete the `docling_chunks` collection in the AstraDB console.

## Notes

- Keep `"chunker": "hybrid"` in the chunking options. The SDK only sends fields
  you set, and the service rejects chunking options without a chunker (`422`).
- The service writes into an existing collection and does not create one. `ingest.py`
  creates it with the same vectorize provider and model as the target. If you change
  the model, delete the collection or use a new `ASTRA_DB_COLLECTION`.
- The tokenizer in `ingest.py` matches the embedding model (`nvidia/nv-embedqa-e5-v5`),
  and `max_tokens` is 384, below the model's 512-token limit: the headings are added to
  each chunk, and AstraDB rejects chunks that are too long. Change them together.
