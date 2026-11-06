# docling-flink-udf

The Java UDF artifact shared by examples 02 to 04: the code that calls Docling
(and signs S3 links) from inside Confluent Cloud. Example 05 has its own.

| class | SQL function | used by | what it does |
|---|---|---|---|
| [`DoclingConvert`](src/main/java/ai/docling/confluent/DoclingConvert.java) | `dc_convert` | 02, 03 | converts a document by URL with Docling |
| [`DoclingExtract`](src/main/java/ai/docling/confluent/DoclingExtract.java) | `extract_real` | 04 | Docling's structured extraction |
| [`S3Presign`](src/main/java/ai/docling/confluent/S3Presign.java) | `dc_presign` | 03 | signs a temporary S3 download link; calls nothing |

Example 05 has a Java artifact of its own, [`examples/05-xbrl/udf`](../../examples/05-xbrl/udf/),
for `dc_metrics`. It does not use `dc_convert`, because a Confluent Cloud UDF
has a row-size limit of 4 MB on both its inputs and its outputs, and the
converted document of an SEC report (14 MB for a 10-Q) is far over it. So
`dc_metrics` makes the same Docling calls as `DoclingConvert` and reads the
figures from the converted document inside the function; only the figures are
returned.

```bash
./build.sh          # -> target/docling-flink-udf.jar, ready to upload
./build.sh test     # the unit tests
```

It needs a JDK and nothing else: with no `mvn` on your PATH, `build.sh`
downloads a pinned, checksummed Apache Maven into `../../.tools` and uses it.

## Why Java

Not a preference: a Confluent Cloud **Python** UDF can't make an HTTPS call.
Its interpreter is built without `ssl` (`urlopen` answers `unknown url type:
https`), and binding a Python function to a `CONNECTION` is accepted by
`CREATE FUNCTION` but fails at runtime. Reading a connection's credential is
Java-only too, which is why `S3Presign` is here although it calls nothing.
[`docs/05-confluent-cloud/LIMITS.md`](../../../docs/05-confluent-cloud/LIMITS.md) records how this was established on a
live account.

So Java makes the calls, and Python does everything else.

## How the two Docling calls work

`DoclingConvert` and `DoclingExtract` are both an `AsyncScalarFunction` that
submits, polls, fetches the result and downloads it, with the JDK's own HTTP
client and Jackson. Each answers within **45 s** of getting the record — with a
failure value if it must. The reasons, in the order they were found:

- A synchronous UDF runs a batch's calls one after another, and Confluent
  Cloud's timeout is per *batch*: a batch that overruns is cut off, rerun, and
  billed again.
- An async UDF's timeout is per call, but it is about **60 s**, can't be
  changed, and a call that passes it fails the whole *statement*. So each
  function answers before that, and caps every HTTP timeout by the time left.
- That is also why there is no client library: docling-java's polling loop
  owns its own timeouts, and its release predates the extraction API
  ([docling-project/docling#4218](https://github.com/docling-project/docling/pull/4218)).

They retry what is worth retrying — a task failed as `retryable`, a 5xx, and
for extraction a page the model never answered — and nothing else: a 4xx or a
wrong answer isn't worth paying for twice. Their tests run against a fake
docling-serve on localhost.

## Dependencies

Few, and the two that matter are pinned on purpose:

| | |
|---|---|
| `flink-table-api-java` | `provided`: the runtime has it, and bundling it causes `NoSuchMethodError` |
| `jackson-databind` 2.22.2 | the version on Confluent's runtime classpath, which takes precedence. A different one works locally and conflicts only in production |

The shade plugin drops `module-info.class` and the signature files.

## Registering the functions

```sql
CREATE FUNCTION `dc_convert`
  AS 'ai.docling.confluent.DoclingConvert'
  USING JAR 'confluent-artifact://<artifact-id>/<version>'
  USING CONNECTIONS (`docling_connection`, `artifact_connection`);
```

No `LANGUAGE` clause: Java is the default. A connection makes its host
reachable and passes the function two values, as job parameters
`<connection>.endpoint` and `<connection>.token`. Nothing else a connection
holds is visible.

`S3Presign` is a plain synchronous `ScalarFunction`: AWS Signature Version 4
with `javax.crypto`, no request and no AWS SDK. Its connection is the bucket:
`endpoint` is the bucket URL (whose host names the region) and `token` is the
key pair as `<id>:<secret>`. Its test checks the signature against the worked
example in AWS's documentation.

```sql
CREATE FUNCTION `dc_presign`
  AS 'ai.docling.confluent.S3Presign'
  USING JAR 'confluent-artifact://<artifact-id>/<version>'
  USING CONNECTIONS (`bucket_connection`);

SELECT dc_presign('bucket_connection', 'inbox/report.pdf', 600);
```

The connection name is an argument rather than hardcoded because every name in
this repo is derived from `NS`, and a hardcoded one would be someone else's.
