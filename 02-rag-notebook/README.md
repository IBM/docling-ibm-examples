# 02 · RAG in a notebook

Answer questions over your documents with Docling for IBM watsonx and watsonx.ai,
with no database, queue or server to deploy.

| Step | Runs where | What it does |
|---|---|---|
| Convert | Docling for IBM watsonx | Layout analysis, OCR and table structure. No models or GPU on your machine |
| Chunk | Your machine | `HybridChunker` splits each document along its sections, sized for the embedding model |
| Embed | watsonx.ai | `ibm/granite-embedding-278m-multilingual`, 768 dimensions |
| Store and retrieve | Your machine | A NumPy matrix and a cosine similarity |
| Answer | watsonx.ai | `ibm/granite-4-h-small`, prompted with the retrieved chunks, with citations |

| File | What it is |
|---|---|
| [`python/rag.ipynb`](python/rag.ipynb) | The Jupyter notebook, one cell per step |
| [`python/rag.py`](python/rag.py) | The same pipeline as a script |

This folder accompanies the [RAG in a Notebook](https://developer.dcls.saas.ibm.com/tutorials/notebook-rag) tutorial of the
[Docling for IBM watsonx documentation](https://developer.dcls.saas.ibm.com/).

## Prerequisites

- **Docling for IBM watsonx**: the Service URL and an API key (Workbench, **API examples**).
- **watsonx.ai**: a project, its project ID, an IBM Cloud API key that can use it, and
  the URL of its region (for example `https://us-south.ml.cloud.ibm.com`).
- [`uv`](https://docs.astral.sh/uv/) (or Python 3.10+ with `pip` and Jupyter).

## Setup

```bash
git clone https://github.com/IBM/docling-ibm-examples
cd docling-ibm-examples
cp .env.example .env        # fill in the DOCLING_SERVICE_* and WATSONX_* values
cd 02-rag-notebook/python
```

The notebook and the script read `.env` from their folder or any parent folder, up to the repository root:

| Variable | Value |
|---|---|
| `DOCLING_SERVICE_URL`, `DOCLING_SERVICE_API_KEY` | Docling for IBM watsonx Service URL and API key |
| `WATSONX_URL` | watsonx.ai region URL |
| `WATSONX_APIKEY` | IBM Cloud API key (`rag.py` also accepts `WATSONX_API_KEY`) |
| `WATSONX_PROJECT_ID` | watsonx.ai project ID |

## Run

Notebook:

```bash
uv run --group notebook jupyter lab rag.ipynb
```

Run the cells top to bottom. The first cell installs the packages only if they are missing, for example when you open the
notebook in another environment (VS Code, watsonx.ai Studio, ...).

Script:

```bash
uv run rag.py
uv run rag.py --question "How many pages does DocLayNet contain?"
uv run rag.py --source path/to/my.pdf --source https://example.com/report.pdf --question "..."
```

Without `uv`: `pip install docling-client "docling-core[chunking]" ibm-watsonx-ai numpy python-dotenv`.

The first run downloads the tokenizer files of the embedding model (a few MB) from
Hugging Face. No model weights are downloaded.

## Expected output

```text
success    https://arxiv.org/pdf/2408.09869
success    https://arxiv.org/pdf/2206.01062
<n> chunks
Index: (<n>, 768)

Q: How many pages does DocLayNet contain, and how were they annotated?

A: ... [1] ... [3]

  [1] https://arxiv.org/pdf/2206.01062, 3 The DocLayNet dataset, pages [2, 3]
  ...
```

Each answer is followed by its sources, with section headings and page numbers. For
the question the documents do not cover ("What is the capital of France?"), the
model says it cannot answer from the documents.

## Notes

- The example chunks the documents itself, because the service does not yet return
  chunks as a conversion output. It will be updated when it does.
- To index a whole document library, let the service chunk and deliver the chunks to
  a vector database: see [`03-vector-db-ingestion`](../03-vector-db-ingestion).
- The index is in memory and rebuilt on every run.
