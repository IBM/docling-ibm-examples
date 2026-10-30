# Docling for IBM watsonx: examples

Runnable, end-to-end examples for **[Docling for IBM watsonx](https://www.ibm.com/products/docling)**, the managed service that turns PDFs, Office files, scans and web pages into clean, structured, AI-ready data.

Each folder is self-contained and has its own README. The matching walkthroughs are in the [Docling for IBM watsonx documentation](https://developer.dcls.saas.ibm.com/).

| Example | What it shows | Docs |
|---|---|---|
| [`01-quickstart/`](01-quickstart/) | Convert one document with the API (curl) and the Python SDK; get Markdown and DocLang | [Quick Start](https://developer.dcls.saas.ibm.com/quick-start) |
| [`02-rag-notebook/`](02-rag-notebook/) | The smallest RAG application: convert with the service, chunk, embed and answer with watsonx.ai | [Tutorial: RAG in a notebook](https://developer.dcls.saas.ibm.com/tutorials/notebook-rag) |
| [`03-vector-db-ingestion/`](03-vector-db-ingestion/) | One batch request from public URLs straight into an AstraDB vector collection | [Tutorial: Ingest a document library into a vector database](https://developer.dcls.saas.ibm.com/tutorials/vector-db-ingestion) |
| [`04-bob-agent-skill/`](04-bob-agent-skill/) | An agent skill that gives IBM Bob (or any skill-aware assistant) document understanding | [Tutorial: Docling in IBM Bob](https://developer.dcls.saas.ibm.com/tutorials/bob-agent-skill) |
| [`05-confluent-cloud/`](05-confluent-cloud/) | Stream document chunks into Confluent Cloud and clean them with Flink SQL, no scripts | [Tutorial: Streaming pipelines with Confluent Cloud](https://developer.dcls.saas.ibm.com/tutorials/confluent-cloud) |

## Repository layout

```
NN-example-name/
  README.md        # the scenario, prerequisites and how to run it (language-neutral)
  python/          # one folder per implementation
  curl/
  java/            # (future)
  typescript/      # (future)
```

Each example lives in a numbered folder, ordered from the first steps to complete
integrations. Its README describes the scenario once, and every implementation sits in a
subfolder named after its language or tool, so new languages can be added side by side.
Files that are not code in a programming language (the Bob agent skill, SQL statements)
sit directly in the example folder or in a folder named after what they are, such as `sql/`.

| Example | Implementations |
|---|---|
| 01 Quick start | `python/`, `curl/` |
| 02 RAG in a notebook | `python/` |
| 03 Vector database ingestion | `python/` |
| 04 Docling in IBM Bob | agent skill (Markdown) |
| 05 Confluent Cloud | Flink SQL, `python/` (optional sender) |

A new language goes in a folder next to `python/`, linked from the example README.
Pull requests are accepted from the repository collaborators only; everyone is welcome
to report problems and suggest examples in an issue. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Prerequisites

1. **A Docling for IBM watsonx instance.** Sign up at [ibm.com/products/docling](https://www.ibm.com/products/docling); a free trial is available.
2. **Your Service URL and API key.** In the Workbench, open **API examples** and copy both values.
3. **[uv](https://docs.astral.sh/uv/)** (recommended) or any Python 3.10+ environment.

Some examples need more: a watsonx.ai project, an AstraDB database, IBM Bob, or a Confluent Cloud account. Each README lists exactly what it needs.

## Configure your credentials once

```bash
git clone https://github.com/IBM/docling-ibm-examples.git
cd docling-ibm-examples
cp .env.example .env      # then edit .env
```

All examples read the same variables:

| Variable | Used for |
|---|---|
| `DOCLING_SERVICE_URL` | Your Docling for IBM watsonx instance |
| `DOCLING_SERVICE_API_KEY` | Authenticates every request (`X-Api-Key` header) |
| `WATSONX_URL`, `WATSONX_APIKEY`, `WATSONX_PROJECT_ID` | Embeddings and chat in the RAG examples |

`.env` is git-ignored. Never commit credentials.

## The Python SDK in one minute

```bash
pip install docling-client
```

```python
import os
from docling.service_client import DoclingServiceClient

with DoclingServiceClient(
    url=os.environ["DOCLING_SERVICE_URL"],
    api_key=os.environ["DOCLING_SERVICE_API_KEY"],
) as client:
    result = client.convert(source="https://arxiv.org/pdf/2408.09869")
    print(result.document.export_to_markdown())
```

`docling-client` is lightweight: it installs no models, because conversion runs on the service. It also provides the `docling convert-remote` command line.

## Documentation

- **Product page:** [ibm.com/products/docling](https://www.ibm.com/products/docling)
- **Docling for IBM watsonx documentation:** [developer.dcls.saas.ibm.com](https://developer.dcls.saas.ibm.com/): guides, [API reference](https://developer.dcls.saas.ibm.com/api), [connectors](https://developer.dcls.saas.ibm.com/connectors) and the [tutorials](https://developer.dcls.saas.ibm.com/tutorials) that go with these examples
- **Open-source Docling:** [github.com/docling-project/docling](https://github.com/docling-project/docling)
- **DocLang Viewer:** [doclang.ai/viewer](https://doclang.ai/viewer/)

## Support

- Product questions and issues with the service: [IBM Support](https://www.ibm.com/mysupport/)
- Problems with an example in this repository: [open an issue](https://github.com/IBM/docling-ibm-examples/issues)
- Security issues: see [SECURITY.md](SECURITY.md)

## License

[Apache License 2.0](LICENSE)
