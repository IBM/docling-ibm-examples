# 05 · Streaming pipelines with Confluent Cloud

**From unstructured documents to live insight, with
[Docling](https://github.com/docling-project/docling) and [Confluent Cloud](https://confluent.io) for
Apache Flink.**

> [!IMPORTANT]
> **Start here: [set up your Confluent Cloud account](../docs/05-confluent-cloud/ACCOUNT.md).**
> You do this once, before any example. The guide covers the tools and keys you
> need, every command, what is billed, and how to fix common errors.
>
> ```bash
> cd 05-confluent-cloud
> ./account.sh init     # asks for each value and says where to get it
> ./account.sh plan     # shows what would be created; creates nothing
> ./account.sh apply    # asks first, then creates it, and that is billed
> ./account.sh check    # confirms the keys work
> ```

Five complete examples. Each is a different way to get documents into a
stream: three turn them into clean, searchable chunks and share one
enrichment; the fourth extracts invoice fields and drives a live dashboard;
the fifth converts SEC filings as they are published, and flags what changed
in them.

<p align="center">
  <img src="../docs/05-confluent-cloud/diagrams/overview.animated.svg" alt="01: Docling for IBM watsonx converts outside Confluent Cloud and streams chunks in. 02: a topic of URLs, converted by Docling called from Flink. 03: uploads to a private S3 bucket, converted the same way. All three feed one shared Flink enrichment (normalize, PII guard, dedup, quality gate) into enriched, quarantine and rejected topics. 04: supplier invoices; Docling extracts fields, Flink converts to EUR and keeps reports for a live dashboard and lightning queries. 05: SEC filings as they are published; Docling converts each report, Flink reads its headline figures into a metrics topic and detects financial events from them." width="100%">
</p>

| example | documents come from |
|---|---|
| [01 — Docling outside](examples/01-docling-outside/) | Docling converts documents outside of Confluent Cloud and writes document chunks to a topic. |
| [02 — URLs](examples/02-docling-in-flink-urls/) | You write URLs to a topic and Flink calls Docling for IBM watsonx on each one. |
| [03 — S3 bucket](examples/03-docling-in-flink-s3/) | You upload files to a private S3 bucket and a managed Confluent connector consumes from the bucket. |
| [04 — Invoices](examples/04-invoice-extraction/) | Six months of invoices on a simulated clock: extract vendor, total and line items, convert to EUR, watch a live spend dashboard. [▶ 1-min demo](examples/04-invoice-extraction/video/ex04-docling-confluent.mp4) |
| [05 — SEC filings](examples/05-xbrl/) | 10-K and 10-Q filings stream in from SEC EDGAR as they are published; Docling converts each report, and Flink reads its headline figures and flags what changed (net income turning negative, cash falling sharply, …). |

## Shared enrichment

Examples 01–03 end the same way: one Flink statement, the same files
([`common/flink/`](common/flink/)), turns raw chunks into three topics.

| step | what happens |
|---|---|
| **normalize** | fixes text such as words split across lines; keeps the original as `raw_text` |
| **PII guard** | redacts e-mail addresses and card numbers (by default); the unredacted original goes to a separate *quarantine* topic |
| **dedup** | drops a chunk it has already seen, by content fingerprint, across documents |
| **quality gate** | drops chunks too short to be useful and drops documents on a block list |

| output topic | contains |
|---|---|
| `chunks.enriched` | the finished chunks, which are redacted and deduplicated |
| `pii.quarantine` | the unredacted originals of anything the PII guard matched |
| `quality.rejected` | what the quality gate dropped |

[Example 01](examples/01-docling-outside/#the-sample-document) shows what each
step does to five sample chunks.

## Run an example

Once you have [set up your account](../docs/05-confluent-cloud/ACCOUNT.md), go
to the example you want from `05-confluent-cloud/`:

```bash
cd examples/01-docling-outside
./setup.sh info           # every name this example will use; read it
./setup.sh apply          # tables (and their topics), functions, connections, artifacts
./pipeline.sh deploy      # start the Flink statements
```

Then follow that example's README to send documents through it.

## Sharing an account

On an account other people use, set `CC_ENVIRONMENT_ID`, `CC_CLUSTER_ID`,
`CC_COMPUTE_POOL_ID` (`./account.sh check` lists them) and `CC_PRINCIPAL_ID`
(the user or service account that should own the keys). Terraform then only
looks those up — it can never plan to destroy them — and a Cloud key with
EnvironmentAdmin and FlinkAdmin on that environment is enough.

**Choose a distinct `NS`.** In Confluent Cloud, Flink objects are not private
to you: a Flink catalog *is* an environment and a database *is* a cluster, so
everyone in the environment shares one namespace. Name collisions fail silently:
two pipelines writing one topic interleave their results, and two sharing a consumer
group each see half the data.

| object | shared with |
|---|---|
| topics, tables | everyone with access to the cluster |
| functions, views, connections, statements | everyone in the environment |
| artifacts | everyone in the environment |
| compute pool | everyone using it |

What the scripts delete — always by exact name, always under your `NS`:

- `./pipeline.sh deploy` replaces the example's running statements and views.
- `./setup.sh apply` drops and recreates a function or connection when it
  changes.
- **Topics and their data are only deleted when you ask**, with
  `./setup.sh destroy --topics`.

`./setup.sh info` lists every name an example touches.

## Cleaning up
Be sure to clean your examples from the clusters to save on usage costs.

| command | removes | cost of undoing it |
|---|---|---|
| `./pipeline.sh delete` | running statements and views | none, it stops the bill |
| `./setup.sh destroy` | functions, connections, artifacts, connector, Context Engine | `./setup.sh apply` |
| `./setup.sh destroy --topics` | every topic of the example | the data is gone |
| `./account.sh destroy` | the API keys, and whatever `apply` created — including the cluster and all its topics | run it last, after every example's `destroy` |

`--dry-run` shows what would be removed. Otherwise you type a word to confirm.
Always destroy through `./setup.sh`, not `terraform destroy`: Terraform doesn't
know about the views the pipeline creates, and a function still used by a view
can't be dropped.

## Repository structure

```
.env, account.sh, terraform/account/    your account, once
terraform/modules/example/              one example's setup, shared by all five
common/flink/                           the enrichment statements
common/udf/                             Python UDFs: normalize, PII guard, chunking
common/udf-java/                        Java UDFs: the calls to Docling, S3 presigning
examples/<n>/                           env.sh, flink/, terraform/, setup.sh, pipeline.sh, README
../docs/05-confluent-cloud/ACCOUNT.md   setting up your account: every account.sh command
../docs/05-confluent-cloud/LIMITS.md    what Confluent Cloud Flink will and won't run — read this
```

An example adds names and SQL, not code, apart from the functions only it uses
(04's Python UDFs and 05's Java UDF, each in the example's own `udf/`).
`env.sh` derives every name from `NS`; `flink/` holds only the example's own
statements, and the shared ones from `common/flink/` are merged in by file
number (examples 04 and 05 use none of them). Its Terraform is the shared
module plus anything only it has (03's connector, 04's Context Engine).

<details>
<summary><b>Terraform state</b></summary>

One local state per root, gitignored because it holds secrets:
`terraform/account/terraform.tfstate` for the account, and
`examples/<n>/terraform/terraform.tfstate` for each example. Run any other
Terraform command with the configuration filled in through the scripts:
`./account.sh tf state list`, `./setup.sh tf output`.

</details>

<details>
<summary><b>Sizing</b></summary>

These pipelines are for experiments, so they are small on purpose:

| | | |
|---|---|---|
| partitions | 1 | keeps the output in input order, easy to compare by eye |
| retention | 1 day | reduces storage billing, because Flink's default is *infinite* |
| compute | one shared pool | it scales up while a statement runs, and to zero after |

Change them with `PARTITIONS` and `RETENTION` in `.env`. (Example 04 keeps its
own: one partition, infinite retention.)

</details>


## Where this came from

The enrichment logic is based on [LAB-2775 from IBM/txc2026-docling](https://github.com/IBM/txc2026-docling/tree/main/lab-2775) (Apache 2.0).
