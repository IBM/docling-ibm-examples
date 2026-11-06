-- The three CREATE FUNCTIONs point at one uploaded artifact — the zip built by
-- `udf/build.sh` and uploaded by `./setup.sh apply`, which is where
-- ARTIFACT_ID and ARTIFACT_VERSION in .env come from.
--
-- `USING JAR` for a Python artifact is not a typo: the grammar reuses the
-- keyword, and `LANGUAGE PYTHON` is what selects the runtime.
CREATE FUNCTION `${FN_IS_CHUNK}`
  AS 'docling_udf.functions.dc_is_chunk'
  LANGUAGE PYTHON
  USING JAR 'confluent-artifact://${ARTIFACT_ID}/${ARTIFACT_VERSION}';
