-- dc_presign(bucket_connection, key, ttl_seconds) -> a presigned GET URL.
--
-- AWS Signature Version 4, query-string form, computed in Java from the
-- connection's endpoint and key pair and the clock — no request to AWS, no
-- AWS SDK (tens of megabytes, against a 100 MB artifact cap, and it wants a
-- credential chain a UDF does not have). See common/udf-java/…/S3Presign.java.
--
-- The same artifact as dc_convert: one JAR, two CREATE FUNCTIONs. A scalar
-- function, not async — it is a hundred microseconds of HMAC.
--
-- A missing key is NULL. A connection that is missing or malformed throws, so
-- the statement fails at the first record with the reason in its detail —
-- that is a setup mistake, the same for every row, and a thousand identical
-- failure rows would hide it.
CREATE FUNCTION `${FN_PRESIGN}`
  AS 'ai.docling.confluent.S3Presign'
  USING JAR 'confluent-artifact://${JAVA_ARTIFACT_ID}/${JAVA_ARTIFACT_VERSION}'
  USING CONNECTIONS (`${BUCKET_CONNECTION}`);
