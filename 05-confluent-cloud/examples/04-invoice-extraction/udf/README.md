# invoice-udf

Example 04's Python UDF artifact. Two registered functions:

| function | SQL name | what it is |
|---|---|---|
| `synthetic_extract(docling_conn, artifact_conn, url, target)` | `${NS}_ex04_extract_synthetic` | Docling's extraction contract, answered from the ground truth — what invoices released in *synthetic* mode get. Same signature as the real one (`DoclingExtract`, Java, in `common/udf-java`); the connection names are ignored. |
| `invoice(extraction, submission)` | `${NS}_ex04_invoice` | per-page items → one invoice record, with `status` ok / review — for either extraction. |

    src/invoice_udf/synthetic.py      ExtractionTarget in, ExtractionDocumentResult out
    src/invoice_udf/invoice.py        page merge and the ok/review checks
    src/invoice_udf/schema.py         the JSON Schema subset the synthetic extraction validates with
    src/invoice_udf/_truth.py         generated from ../data/manifest.jsonl — not committed

    ./build.sh          -> compiles _truth.py, then dist/invoice_udf-0.1.0.zip
    uv run pytest       -> the logic, without a cluster, against the real target
                           (read out of ../flink/20_view_submitted.sql) and the
                           real manifest

The wire format is docling-project/docling#4218's: an `ExtractionTarget`
(`output_schema`, `template`, `instructions`) in; an `ExtractionDocumentResult`
(`source_uri`, `filename`, `status`, `errors`, `items[]` with page `scope`,
`extracted_data`, `raw_text`, `validation_status`, `inference_metadata`) out —
as Docling for IBM watsonx actually answers it, checked on all 174 invoices
with `../scripts/extract.py`: a page that breaks the schema keeps its answer
only in `raw_text`, dates come back as printed, errors in jsonschema's words.
`synthetic.py`'s docstring lists what it reproduces.

The pinned dependency set and the artifact shape (a zip holding the sdist) are
the same as `common/udf`'s, for the same reasons — see its README.
