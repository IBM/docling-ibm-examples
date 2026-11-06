# What the managed service will and will not run

Confluent Cloud for Apache Flink is not Flink with a hosted control plane. It
is a narrower product, and most of the shape of these examples is a consequence
of that. This file is the list — with, where it matters, how each item was
established rather than merely asserted.

Everything below was checked against a live account in `aws/us-east-1` in
September and October 2026. Confluent moves; re-check before you plan around a
"no".

## The one fact everything follows from

**There is no container and no DataStream API.** The managed service exposes:

- **Flink SQL** — the statement is the deployable unit.
- **Table API**, Java and Python, which compiles to the same statements.
- **User-defined functions**, uploaded as *artifacts*: a JAR for Java, a ZIP
  for Python. This is the only way your own code gets in.
- **`CREATE MODEL` / `AI_EMBEDDING`** for remote inference, against a fixed set
  of providers.
- **`CREATE CONNECTION`**, which holds an endpoint and a secret and is the only
  route out of Confluent Cloud a UDF has.

DataStream, PyFlink DataStream, Flink ML, Stateful Functions and queryable
state are all unsupported.

## Python UDFs cannot make network calls. At all.

This is the constraint that decides the architecture of examples 02 and 03, and
it is stronger than the documentation's "UDFs can't make external network calls
directly" suggests. Three things were tested, in this order:

| what | result |
|---|---|
| `CREATE FUNCTION ... LANGUAGE PYTHON ... USING CONNECTIONS (…)` | **accepted** — the DDL completes |
| calling that function | **statement FAILED**, with no exception in `/exceptions` |
| a Python UDF fetching an `https://` URL with no connection bound | `ERR URLError: <urlopen error unknown url type: https>` |

The third line is the interesting one. "Unknown url type: https" is what
`urllib` says when Python was built **without the `ssl` module** — the runtime
has no TLS at all, so no HTTPS client can work in it, whatever library you
reach for. `requests`, `httpx` and Docling's own `service_client` all fail the
same way, because they all end at the same missing socket.

And the first two lines together are a trap: the DDL is accepted, so a Python
UDF bound to a connection looks configured right up until the first row goes
through it.

Confluent's own documentation agrees, in one line under UDF regional
availability: *"External connectivity — Generally Available. Java-only."* and
under limitations: *"Flink supports external connectivity only for Java
stateless scalar and table functions."*

**So: anything that calls out is a Java UDF.** That is why `common/udf-java/`
exists. Its `S3Presign` calls nothing, but reading a connection's credential is
Java-only too.

## What a UDF may do

| | Python | Java |
|---|---|---|
| scalar functions | yes (including vectorized `func_type="pandas"`) | yes |
| table functions (UDTF) | **no** | yes |
| external connectivity (`USING CONNECTIONS`) | **no** | yes |
| artifact | ZIP of an sdist | shaded JAR |
| runtime | Python 3.10–3.11, `apache-flink==2.0.0` exactly | JDK 21 |

Both share these:

| | |
|---|---|
| artifact size | 100 MB, and 100 artifacts per org/cloud/region/environment |
| filesystem | read-only. No temp files, no model download at first call |
| native code | no JNI, no native binaries |
| row size | 4 MB in and out |
| per statement | at most 10 UDFs |
| dependencies | the runtime's versions win — match them or get `NoSuchMethodError` |

The no-UDTF line is why `dc_chunks` returns `ARRAY<STRING>` and the SQL uses
`CROSS JOIN UNNEST`: one document becomes many chunks, and a scalar function
that returns an array is the only way a Python UDF can say that.

The no-native-code line is why the chunking in
`docling_udf.docling_response` is hand-written rather than Docling's own
`HybridChunker`: that one needs a tokenizer, and a tokenizer is native code.

### How a Python UDF gets uploaded

You upload a **ZIP containing a Python sdist**, and Confluent Cloud unpacks it
into its own UDF runtime. `common/udf/build.sh` and an example's
`./setup.sh apply` do all of this:

1. `uv build --sdist` → `dist/docling_udf-0.1.0.tar.gz`
2. `zip -FS -j dist/docling_udf-0.1.0.zip dist/docling_udf-0.1.0.tar.gz`
3. `POST /artifact/v1/presigned-upload-url` → a URL, `PUT` the ZIP at it
4. `POST /artifact/v1/flink-artifacts` with the upload id → `cfa-xxxxxx`
5. `CREATE FUNCTION dc_prepare AS 'docling_udf.functions.dc_prepare'
   LANGUAGE PYTHON USING JAR 'confluent-artifact://cfa-xxxxxx/ver-xxxxxx';`

`USING JAR` for a Python ZIP is not a mistake: the grammar reuses the keyword
and `LANGUAGE PYTHON` selects the runtime.

## External calls are synchronous, and the timeout is per batch

From the documentation, and worth repeating because it is the operational
constraint of examples 02 and 03:

> External endpoint calls are synchronous: each call blocks the task thread
> until the endpoint responds. […] Flink processes records in batches, and the
> platform applies a timeout to the batched invocation rather than to an
> individual endpoint call, so a single slow call consumes part of the budget
> shared by the other records in its batch. A batch that doesn't complete
> before the timeout is terminated without returning values.

A Docling conversion takes seconds to minutes. Example 02's first
`DoclingConvert` was a synchronous `ScalarFunction`, and this is what it cost:
a batch that overran was cut off and run again — every conversion in it billed
again. Both Java UDFs are asynchronous now (below). And still:

- keep the URL table modest — these are examples, not an ingestion tier;
- scale the compute pool and the partition count together if you want
  parallelism, because a partition is the unit of it;
- for real volume, use Docling's batch endpoint instead of a per-row UDF.

Only `endpoint` and `token` from a connection reach the function, whatever
else the connection carries. A service that wants its key in some other header
— Docling for IBM watsonx wants `X-Api-Key` — gets it because the UDF builds
the request itself.

### …and an async UDF gets about a minute, per call

The way around the batch timeout (measured while building example 04, October 2026) is an
`AsyncScalarFunction` (Confluent documents it for external calls): Flink keeps
several calls in flight and the timeout becomes per call. Measured, not
documented:

- the timeout is **about 60 s** — an ad hoc `SELECT` on one slow call went
  `RUNNING` at +51 s and `FAILED` at +118 s with *"Async UDF invocation
  timeout"*;
- a call that hits it fails the **statement** (phase `FAILED`, the message in
  `status.detail`), not the row;
- it is not configurable: `table.exec.async-scalar.timeout`,
  `…max-concurrent-operations` and `…buffer-capacity` are all refused as
  statement properties (*"Unsupported configuration options"*; the supported
  list is the `sql.*` ones).

So the function has to enforce its own deadline, from the moment it is handed
the record, and answer before that minute with a value — see
`common/udf-java/…/DoclingExtract.java` and `DoclingConvert.java` (45 s each).
A backlog (a table deployed onto a topic that already has hundreds of records)
is the worst case: the records queue for the function's workers, and the
queueing counts.

### A materialized table pins its artifact version

A changed UDF is a new artifact, with a new id, on the next `./setup.sh
apply`, which re-registers the functions on it and deletes the old one. A
*suspended* materialized table created before that does not follow: resuming it
fails with `plugin id cfa-… version id ver-… … not_found`. The table has to be
dropped and created again (its topic goes with it).

## A connection is per host, and one API can be two hosts

Docling for IBM watsonx answers a conversion with a **presigned URL on Amazon
S3**, not with the document. There is no `inbody` target to ask for otherwise;
the service rejects one:

```
target kind 'inbody' is not allowed. Allowed values: ['astradb_chunks',
'azure_blob', 'google_cloud_storage', 'google_drive', 'kafka_chunks',
'opensearch_chunks', 'opensearch_doc', 'presigned_url', 's3', 'sharepoint']
```

So the result lives on a host that is not the service's, and a UDF may only
reach hosts it is bound to. Example 02 therefore creates **two** connections:
one for the service, carrying the API key, and one for the artifact host,
carrying nothing — a presigned URL is its own credential. See
`examples/02-docling-in-flink-urls/flink/05_connection_artifacts.sql`.

That list is also worth reading for another reason: `kafka_chunks` is in it.
Docling can write chunks straight to a Kafka topic, which is exactly what
example 01 uses and is why example 01 needs no UDF that calls anything.

## The dedup holds rows until the watermark moves

Not a Confluent Cloud limitation — plain Flink semantics — but it is the thing
most likely to make you think an example is broken, so it belongs here.

`common/flink/30_pipeline.sql` deduplicates with

```sql
ROW_NUMBER() OVER (PARTITION BY fingerprint ORDER BY event_time)
```

and `event_time` is `$rowtime`, an *event-time* attribute. Flink's deduplicate
operator cannot emit row 1 of a group the moment it sees it, because a later
record could still arrive with an earlier timestamp — so it waits for the
watermark to pass. On an idle topic the watermark does not move, and rows sit
in the operator.

What that looks like:

- produce five chunks, once;
- the **quarantine** and **rejected** branches emit immediately, because
  neither goes through the dedup;
- the **enriched** branch emits nothing, for as long as the topic stays idle;
- produce anything else, and all of it appears at once.

So a first run of `produce_chunks.py --sample` may leave the enriched topic
empty while quarantine already has a record. Run it a second time and both
batches land. The same thing is invisible in example 02, where a single paper
produces a hundred chunks and the watermark advances on its own.

The way out in plain Flink, ordering by processing time
(`ORDER BY PROCTIME()`, first arrival wins, emitted at once), is not available:
`PROCTIME()` is on Confluent Cloud's list of unsupported features. Example 03
needed exactly that for its object events, and avoids the dedup instead —
see its `flink/22_view_objects.sql`.

The pipeline would keep event time anyway: the property being demonstrated is
that a redelivery collapses regardless of when it arrives.

## Namespaces: there are none

    Flink catalog   = Confluent Cloud environment
    Flink database  = Kafka cluster

so a UDF, a view and a table are *shared, flat namespaces* per
environment+cluster, and a statement name is unique per environment. There is
no per-user schema.

Two consequences worth stating plainly:

- **A statement is as global as its principal.** "A Flink SQL statement doesn't
  have its own permissions. It uses the permissions of the principal that runs
  it", and "the principal running the statement determines data access, not the
  compute pool". Nothing in this repo writes a cross-environment name — every
  table reference is bare, resolved by `sql.current-catalog` /
  `sql.current-database` — but the boundary is RBAC, not the tool.
- **A compute pool is shared.** All statements using a pool share its
  resources, and a long-running `INSERT` holds CFUs for as long as it runs.

What this repo does about it is `NS`: every topic, view, function, connection,
statement and artifact name is derived from it, so `deploy`'s DELETE and DROP
reach only your objects. Each example's `./setup.sh info` prints the whole list
before you create any of it.

## Things that will bite you

- **Three API keys, three scopes.** A Kafka cluster key cannot list an
  environment; a Cloud key cannot submit a statement; a Flink key cannot upload
  an artifact. `common/lib.sh` says which is which.
- **The `confluent` CLI cannot log in with an API key.** It takes an email and
  a password, or SSO in a browser. Everything in this repo therefore goes
  through the REST API directly — which is also what makes it runnable from CI.
- **A Cloud API key inherits its owner's RBAC.** Made under a service account
  with no role bindings, it authenticates and then lists zero environments,
  which is indistinguishable from an empty account. `./account.sh check` looks
  for it.
- **`sql.current-catalog` and `sql.current-database` take display names**, not
  `env-xxxxxx` and `lkc-xxxxxx`. The ids are accepted syntactically and then
  nothing resolves.
- **An existing topic is already a table.** Confluent Cloud infers one for
  every topic it can see, and for a schemaless topic that is `key BYTES, val
  BYTES` — so `CREATE TABLE` on a topic Docling wrote fails, and what you want
  is the `ALTER`s in `examples/01-docling-outside/flink/00_alter_chunks.sql`.
- **`DROP TABLE` deletes the topic and its data.** Every `92_drop_table_*.sql`
  says so at the top, and only `./setup.sh destroy --topics` runs them.
- **Artifact names are unique per cloud/region/environment, and DELETE is not
  immediate.** Re-registering a name straight after deleting it answers *"name
  should be unique per Cloud/Region/Environment"* and leaves you with no
  artifact at all. `terraform/modules/example` sidesteps it: each artifact's
  name carries a hash of its sources, and the new one is created before the
  old one is deleted.
- **A failed one-shot DDL statement is a dead resource.** It holds no state and
  will never run, but it keeps its name and the environment fills up with the
  history of every typo. `await_statement` deletes them.

## Embedding: not in these pipelines

Embedding with watsonx.ai means a call over HTTP. A Python UDF cannot make that
call, and watsonx.ai is not one of the providers `CREATE MODEL` knows (AWS Bedrock, AWS SageMaker, Azure ML, Azure
OpenAI, Google AI, Vertex AI, OpenAI, and Confluent's own managed models).

So these examples ship **without an embedding stage**, and the enriched topic
carries everything except the vector. Three ways to add it, in increasing order
of what they cost:

1. **A Confluent-managed model.** No external account, no key, no connection.

   ```sql
   CREATE MODEL `chunk_embedding`
     INPUT  (`text` STRING)
     OUTPUT (`embedding` ARRAY<FLOAT>)
     WITH ('provider' = 'confluent',
           'task' = 'embedding',
           'confluent.model' = 'BAAI/bge-large-en-v1.5');
   ```

   and then, in place of the enriched INSERT's projection:

   ```sql
   SELECT JSON_VALUE(`guarded`, '$.chunk_id') AS `key`,
          JSON_QUERY(`guarded`, '$') AS `payload`,
          `embedding`
   FROM `dc_guarded`, LATERAL TABLE(AI_EMBEDDING('chunk_embedding',
                                     JSON_VALUE(`guarded`, '$.text')))
   ```

   The enriched table needs an `embedding ARRAY<FLOAT>` column for this, so it
   stops being a pure raw-JSON topic.

2. **Your own provider, through a `CONNECTION`.** `CREATE MODEL` with
   `'provider' = 'openai'` (or bedrock, vertex, …). It needs a key from that
   provider.

3. **Keep watsonx.ai, in a Java UDF.** Now that `common/udf-java/` exists and
   proves the pattern, an embedding UDF is the same shape as `DoclingConvert`:
   a REST connection, an async call with its own deadline, a value back. The
   timeout notes above apply twice over — embedding is per chunk, not per
   document.

## What a managed statement gives up

- **A live policy.** A UDF has no state, so the PII and quality policy is
  configuration baked into the view at deploy time, and changing a rule means
  running `./pipeline.sh deploy` again. To change it while a statement runs,
  make the policy an upsert table and read it through a temporal join:

  ```sql
  CREATE TABLE `docling.policy` (
    `key` STRING, `detectors` STRING, `redact` BOOLEAN, `min_chars` INT,
    PRIMARY KEY (`key`) NOT ENFORCED
  ) DISTRIBUTED BY HASH(`key`) INTO 1 BUCKETS
    WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw', 'value.format' = 'json-registry');

  -- and in the guarded view, instead of literals:
  SELECT p.`prepared` AS `original`,
         dc_guard(p.`prepared`, r.`detectors`, r.`redact`, r.`min_chars`, FALSE, '') AS `guarded`,
         p.`event_time`
  FROM `dc_prepared` AS p
  LEFT JOIN `docling.policy` FOR SYSTEM_TIME AS OF p.`event_time` AS r
    ON r.`key` = 'current';
  ```

- **Metrics counters.** A UDF has no metrics group, so there are no counters
  for dropped records, duplicates or redactions. The evidence is the topics
  themselves: the rejected, quarantine and conversion topics exist partly for
  this.
- **Parallelism, TaskManager size, restart strategy.** A compute pool
  autoscales in CFUs and none of those knobs is yours.
