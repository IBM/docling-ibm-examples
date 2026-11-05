-- The Docling service, as a Flink CONNECTION.
--
-- A CONNECTION keeps the endpoint and the token in Confluent Cloud,
-- injects them into the UDF's runtime, and never puts either in the text of a statement.
CREATE CONNECTION `${DOCLING_CONNECTION}`
WITH (
  'type' = 'REST',
  'endpoint' = '${DOCLING_SERVICE_URL}',
  'token' = '${DOCLING_API_KEY}'
);
