# docling-udf

The Python UDF artifact, shared by examples 01–03: normalizing, PII guarding,
and turning a Docling conversion into chunks. Each example uploads its own copy
under its own name and registers the functions it needs.

    src/docling_udf/chunk_record.py      the chunk record: accessors and ids
    src/docling_udf/enrichment.py        normalizing and the derived fields
    src/docling_udf/pii.py               PII detection and redaction
    src/docling_udf/docling_response.py  a conversion, as chunk records  (02, 03)
    src/docling_udf/functions.py         the adapter: the `udf(...)` entry points

    ./build.sh          -> dist/docling_udf-0.1.0.zip, ready to upload
    uv run pytest       -> the logic, without a cluster

Every decision is a pure function, with the Flink mechanics kept to the thin
adapter in `functions.py`, so the logic can be tested without a cluster.

| function | registered by | what it is |
|---|---|---|
| `dc_is_chunk` | every example | the cheap `WHERE` that keeps non-chunks out of the expensive call |
| `dc_prepare` | every example | drops non-chunks and empty chunks, normalizes, adds the derived fields |
| `dc_guard` | every example | redacts PII, and applies the quality gate and the block list |
| `dc_chunks` | 02, 03 | a Docling conversion → an `ARRAY` of chunk records |

`dc_chunks` returns an array rather than being a table function because
Confluent Cloud's Python UDFs are scalar-only; the SQL does the rest with
`CROSS JOIN UNNEST`. And it is here, in Python, rather than in the Java
artifact, so that the chunk record has exactly one definition in the repo.

Nothing in this package makes a network call, and nothing in it can: see
`docs/05-confluent-cloud/LIMITS.md`.

## The pinned dependency set

`constraint-dependencies` in `pyproject.toml` is not ours to choose. Confluent
Cloud's Python UDF runtime pins those versions, and a wheel resolved outside
them fails at `CREATE FUNCTION` rather than at build time. They are copied from
`github.com/confluentinc/flink-udf-python-examples`.

## The artifact shape

Confluent Cloud takes a Python UDF as a **zip containing the sdist tarball** —
not a wheel, and not a zip of the source tree. That is all `build.sh` does.
