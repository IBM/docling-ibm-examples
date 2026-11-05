-- dc_convert(docling_connection, artifact_connection, url, format) -> the
--   conversion, as a JSON envelope.
--
-- Example 02's function, unchanged — the same class from the same JAR. It is
-- handed a presigned URL instead of a public one, and cannot tell the
-- difference: Docling fetches both the same way. Read example 02's copy of
-- this file for why it is Java and why it is asynchronous.
CREATE FUNCTION `${FN_CONVERT}`
  AS 'ai.docling.confluent.DoclingConvert'
  USING JAR 'confluent-artifact://${JAVA_ARTIFACT_ID}/${JAVA_ARTIFACT_VERSION}'
  USING CONNECTIONS (`${DOCLING_CONNECTION}`, `${DOWNLOAD_CONNECTION}`);
