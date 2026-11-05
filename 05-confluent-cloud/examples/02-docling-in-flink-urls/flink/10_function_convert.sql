-- dc_convert(docling_connection, artifact_connection, url, format) -> the
--   conversion, as a JSON envelope.
--
-- The one Java function in this repo, and the only code anywhere in it that
-- makes a network call. Java is not a preference: a Confluent Cloud **Python**
-- UDF has no TLS in its runtime — `urlopen` answers "unknown url type: https",
-- because the interpreter is built without `ssl` — and binding a Python
-- function to a CONNECTION is accepted by CREATE FUNCTION and then fails at
-- runtime. External connectivity is Java-only. docs/05-confluent-cloud/LIMITS.md records how that
-- was established, against a live account, rather than asserting it.
--
-- An AsyncScalarFunction: submit / poll / result / download over plain
-- java.net.http, several conversions in flight, each answered within 45 s —
-- Confluent Cloud fails the statement when an async call passes about a
-- minute. A failure is a value, never an exception. See common/udf-java/.
--
-- USING CONNECTIONS is what makes the two connections reachable *and* what
-- makes their endpoint and token visible to the function, as job parameters
-- named `<connection>.endpoint` and `<connection>.token`. The names are passed
-- as arguments rather than hardcoded because every name here is derived from
-- NS, and a hardcoded one would be somebody else's.
CREATE FUNCTION `${FN_CONVERT}`
  AS 'ai.docling.confluent.DoclingConvert'
  USING JAR 'confluent-artifact://${JAVA_ARTIFACT_ID}/${JAVA_ARTIFACT_VERSION}'
  USING CONNECTIONS (`${DOCLING_CONNECTION}`, `${DOWNLOAD_CONNECTION}`);
