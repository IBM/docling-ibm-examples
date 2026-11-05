-- dc_prepare(payload, min_chars, drop_low_quality) -> the enriched record, or
-- '' for a record the stage dropped.
--
-- Drop control records and empty chunks, normalize the text *before* anything
-- hashes it, then add the derived fields. Configuration arrives as arguments,
-- because a UDF has no `open()` to read it in.
CREATE FUNCTION `${FN_PREPARE}`
  AS 'docling_udf.functions.dc_prepare'
  LANGUAGE PYTHON
  USING JAR 'confluent-artifact://${ARTIFACT_ID}/${ARTIFACT_VERSION}';
