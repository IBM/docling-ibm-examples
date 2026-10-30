# 05 · Confluent Cloud streaming pipeline

Docling for IBM watsonx converts and chunks your documents and writes one
message per chunk to a Confluent Cloud topic. A Flink SQL statement cleans the
chunks, redacts personal data and removes duplicates, so they are ready for
search or RAG.

```
documents ──▶ Docling for IBM watsonx ──▶ docling_chunks
              batch job, Kafka target           │
                                                ▼
                                Flink SQL (one running statement)
                                normalize · redact PII · dedup
                                                │
                ┌───────────────────────────────┼───────────────────────────────┐
                ▼                               ▼                               ▼
        docling_enriched              docling_quarantine               docling_rejected
        clean chunks                  original text with PII           too short
```

Everything runs in Confluent Cloud and in Docling for IBM watsonx. There are no
scripts to set up: you paste SQL into the Flink SQL workspace and create one
batch job.

| File | What it is |
|---|---|
| [`sql/01_tables.sql`](sql/01_tables.sql) | The four tables (and topics) |
| [`sql/02_pipeline.sql`](sql/02_pipeline.sql) | Two views and the long-running statement |
| [`sql/03_sample_chunks.sql`](sql/03_sample_chunks.sql) | Optional: five test chunks, to try the pipeline without Docling |
| [`sql/04_results.sql`](sql/04_results.sql) | Queries to look at the results |
| [`sql/99_cleanup.sql`](sql/99_cleanup.sql) | Remove everything |
| [`python/send.py`](python/send.py) | Optional: send documents with the Docling Python SDK instead of the Workbench |

## Prerequisites

- **Confluent Cloud**: a Kafka cluster and a Flink compute pool in the **same
  cloud and region**.
- **Docling for IBM watsonx**: access to the Workbench (and, for `send.py`, the
  Service URL and an API key).

## Steps

1. **Collect your Kafka connection details.** In the Confluent Cloud console,
   open your cluster:
   - **Cluster settings → Endpoints**: the **bootstrap server**
     (`pkc-xxxxx.<region>.<cloud>.confluent.cloud:9092`).
   - **API keys → Add key**: a Kafka **API key and secret** that can write to
     the cluster. Docling uses them to publish the chunks.

2. **Create the tables.** Open the **Flink SQL workspace** on your compute pool,
   select your environment and cluster as catalog and database, and run each
   statement of [`sql/01_tables.sql`](sql/01_tables.sql). This creates the
   topics `docling_chunks`, `docling_enriched`, `docling_quarantine` and
   `docling_rejected`.

3. **Start the pipeline.** Run the statements of
   [`sql/02_pipeline.sql`](sql/02_pipeline.sql): the two `CREATE VIEW`, then
   the `EXECUTE STATEMENT SET`. The statement keeps running.

4. **Optional: try it without Docling.** Run
   [`sql/03_sample_chunks.sql`](sql/03_sample_chunks.sql) and look at the
   results (step 6).

5. **Send documents.** In the Docling for IBM watsonx **Workbench**, open
   **Tasks**, create a **Batch** task and set:

   | Field | Value |
   |---|---|
   | Source type | **URL**, with one public document URL per line, for example `https://arxiv.org/pdf/2408.09869` |
   | Target type | **Kafka Chunks** |
   | Bootstrap servers | your bootstrap server from step 1 |
   | Topic | `docling_chunks` |
   | SASL authentication | on, mechanism **PLAIN** |
   | Username / Password | your Kafka API key / secret |

   Or, from code, with the Docling Python SDK:

   ```bash
   cd python
   cp ../.env.example ../.env        # bootstrap server, Kafka API key and secret
   uv run send.py https://arxiv.org/pdf/2408.09869
   ```

6. **Look at the results.** Run the queries in
   [`sql/04_results.sql`](sql/04_results.sql) in the workspace.

7. **Clean up.** Stop the running statement in the Flink statements list (it
   uses compute while it runs), then run
   [`sql/99_cleanup.sql`](sql/99_cleanup.sql).

## Notes

- **Create the tables before sending documents.** Docling writes to an existing
  topic; it does not create one.
- **Results can wait for more data.** The deduplication releases rows when the
  stream's time moves forward. If `docling_enriched` stays empty after a small
  input, send another document or run the sample chunks again.
- **Names.** In a shared environment, add a prefix to the table names in all
  SQL files (and to the topic in the batch job) to avoid clashes.
- **Embeddings.** This example stops at clean chunks. Index `docling_enriched`
  into a vector database with a Confluent sink connector, or see the
  `03-vector-db-ingestion` example for a database that embeds chunks itself.
- **Costs.** You pay for Flink compute while the statement runs, for the
  Docling conversions, and for a little Kafka storage (topics keep data for one
  day).

## Links

- [Docling for IBM watsonx](https://www.ibm.com/products/docling)
- [Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/overview.html)
