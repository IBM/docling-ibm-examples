-- The bucket the documents are in, as a Flink CONNECTION.
--
-- Nothing ever connects to it. `dc_presign` reads its two fields and signs a
-- URL; Docling, outside Confluent Cloud, is what fetches it. It is a
-- connection because a connection is the only place a UDF can read a secret
-- from without the secret being in the text of a statement — and a connection
-- exposes exactly two fields to a function, `endpoint` and `token`, so:
--
--   endpoint   the bucket, virtual-hosted style — the host also says the region
--   token      the reader key pair, as <access-key-id>:<secret-access-key>
--
-- Confluent also documents a `type = 's3'` connection with separate
-- `aws-access-key` / `aws-secret-key` fields, but only `endpoint` and `token`
-- reach a UDF, whatever the type. One string, split in the Java, is the shape
-- that works.
--
-- The key pair is the IAM user scripts/aws.sh created: s3:GetObject on the
-- prefix, the SQS calls the connector makes, and nothing else. A presigned URL
-- can do no more than the key that signed it.
--
-- A bucket name with dots breaks virtual-hosted TLS; for one of those, use
-- the path style, 'https://s3.${S3_REGION}.amazonaws.com/${S3_BUCKET}' —
-- dc_presign handles both.
CREATE CONNECTION `${BUCKET_CONNECTION}`
WITH (
  'type' = 'REST',
  'endpoint' = 'https://${S3_BUCKET}.s3.${S3_REGION}.amazonaws.com',
  'token' = '${AWS_READER_KEY_ID}:${AWS_READER_SECRET}'
);
