-- The Docling service, as a Flink CONNECTION.
--
-- This is the only object in the repo that holds a credential, and holding it
-- is the point: a CONNECTION keeps the endpoint and the token in Confluent
-- Cloud, injects them into the UDF's runtime, and never puts either in the text
-- of a statement. `SHOW CREATE CONNECTION` returns the secret redacted.
--
-- 'type' = 'REST' with a 'token' is the shape the UDF runtime understands —
-- only `endpoint` and `token` are exposed to a function, whatever else the
-- connection carries. Docling for IBM watsonx authenticates with an `X-Api-Key`
-- header rather than a bearer token, so the key travels in `token` and
-- `DoclingConvert` puts it in the right header itself.
CREATE CONNECTION `${DOCLING_CONNECTION}`
WITH (
  'type' = 'REST',
  'endpoint' = '${DOCLING_SERVICE_URL}',
  'token' = '${DOCLING_API_KEY}'
);
