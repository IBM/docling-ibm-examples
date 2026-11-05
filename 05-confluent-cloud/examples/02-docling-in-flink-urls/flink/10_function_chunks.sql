-- dc_chunks(envelope, url) -> ARRAY<STRING>, one chunk record per element.
--
-- The other half of the seam. `dc_convert` returns a document; this returns
-- example 01's chunk records, built by the same
-- `docling_udf.chunk_record.build`, so that everything downstream is example
-- 01's pipeline unchanged.
--
-- An ARRAY rather than a table function: Confluent Cloud's Python UDFs are
-- scalar-only, and a UDTF would have to be Java. `CROSS JOIN UNNEST` in
-- `31_convert.sql` does what the UDTF would have done, and says it in SQL.
--
-- The chunking is *not* Docling's. The SaaS has no chunk endpoint, and
-- Docling's own HybridChunker needs a tokenizer, which is native code a UDF
-- runtime will not load. `docling_udf.docling_response` splits the Markdown on
-- headings and packs paragraphs — structural chunking, stdlib only, and
-- documented as such.
CREATE FUNCTION `${FN_CHUNKS}`
  AS 'docling_udf.functions.dc_chunks'
  LANGUAGE PYTHON
  USING JAR 'confluent-artifact://${ARTIFACT_ID}/${ARTIFACT_VERSION}';
