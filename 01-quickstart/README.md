# 01 · Quick start

Convert a document with Docling for IBM watsonx and save it as **Markdown** and as
**DocLang** (`.dclx`), the structured output that keeps sections, tables and page
positions.

| Folder | How |
|---|---|
| [`python/`](python/) | Python SDK (`docling-client`) |
| [`curl/`](curl/) | Plain REST API with `curl` and `jq` |

## Setup

```bash
git clone https://github.com/IBM/docling-ibm-examples
cd docling-ibm-examples
cp .env.example .env        # set DOCLING_SERVICE_URL and DOCLING_SERVICE_API_KEY
```

## Run

Python:

```bash
cd 01-quickstart/python
uv run convert.py                       # the Docling technical report
uv run convert.py path/to/report.pdf    # a local file, or any URL
```

curl:

```bash
cd 01-quickstart/curl
./convert.sh                            # same document
./convert.sh path/to/report.pdf
```

Both write the results to `output/`. Open the `.dclx` file in the
[DocLang Viewer](https://doclang.ai/viewer/), or query it from the terminal as shown in
[`doclang.md`](doclang.md).

This example accompanies the [Quick Start](https://developer.dcls.saas.ibm.com/quick-start) page of the
[Docling for IBM watsonx documentation](https://developer.dcls.saas.ibm.com/).
