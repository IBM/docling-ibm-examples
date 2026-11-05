-- dc_metrics(docling_connection, artifact_connection, url, user_agent)
--   -> ARRAY<STRING>, one JSON object per metric.
--
-- Converts a filing with Docling and reads its headline figures — revenue, net
-- income, total assets, … — out of the tables of the DoclingDocument, in one
-- call. The document itself is 14 MB for a 10-Q and a row is 4 MB, so it is
-- never a value in SQL: it is downloaded, read and dropped inside the function,
-- and only the figures leave. See udf/src/main/java/ai/docling/confluent/xbrl/DoclingMetrics.java.
--
-- Java, because it calls Docling (docs/05-confluent-cloud/LIMITS.md says why a Python UDF cannot),
-- and asynchronous, because a conversion takes seconds and the platform times
-- out a call at about a minute (example 02's 10_function_convert.sql has the
-- longer version). It returns an array because a scalar function is one row
-- out; `CROSS JOIN UNNEST` in 31_metrics.sql makes the rows.
CREATE FUNCTION `${FN_METRICS}`
  AS 'ai.docling.confluent.xbrl.DoclingMetrics'
  USING JAR 'confluent-artifact://${JAVA_ARTIFACT_ID}/${JAVA_ARTIFACT_VERSION}'
  USING CONNECTIONS (`${DOCLING_CONNECTION}`, `${DOWNLOAD_CONNECTION}`);
