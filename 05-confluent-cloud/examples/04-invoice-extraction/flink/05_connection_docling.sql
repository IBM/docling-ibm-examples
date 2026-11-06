-- The Docling service, as a Flink CONNECTION — example 02's, under this
-- example's name.
--
-- The only object here that holds a credential: the connection keeps the
-- endpoint and the key in Confluent Cloud and injects them into the UDF's
-- runtime as job parameters (`<name>.endpoint`, `<name>.token`); neither ever
-- appears in a statement. Docling for IBM watsonx reads the key from an
-- `X-Api-Key` header, which DoclingExtract sets from `token`.
--
-- Created even with EXTRACTION_MODE=fake, because both extract functions are
-- registered and the real one is bound to it.
CREATE CONNECTION `${DOCLING_CONNECTION}`
WITH (
  'type' = 'REST',
  'endpoint' = '${DOCLING_SERVICE_URL}',
  'token' = '${DOCLING_API_KEY}'
);
