-- `${CONVERSIONS_TOPIC}` — what Docling answered, one row per object.
--
--     key         s3://my-bucket/inbox/2408.09869.pdf             (raw bytes)
--     value       {"doc_id": "inbox/2408.09869.pdf", "url": "s3://my-bucket/inbox/2408.09869.pdf",
--                  "status": "success", "conversion": "{\"status\":\"success\",\"content\":\"# …\",…}"}
--
-- Example 02's table, with one difference in what goes in it: `url` is the
-- object's s3:// URI, not the URL Docling fetched. That one was presigned, and
-- although it expires in PRESIGN_TTL seconds, a column called `url` is what
-- people copy. The presigned URL does survive inside `conversion` — the Java
-- envelope echoes what it was given — which is why the TTL is short.
--
-- Failures are rows here too: an object Docling could not read, a presigned
-- URL that had expired by the time it was fetched. Kept for good, because the
-- chunk topic is rebuilt from it on every redeploy (24_view_converted_all.sql).
-- Typed, so not pre-created; see example 02's copy of this file for why.
CREATE TABLE IF NOT EXISTS `${CONVERSIONS_TOPIC}` (
  `key`        BYTES,
  `doc_id`     STRING,
  `url`        STRING,
  `status`     STRING,
  `conversion` STRING
)
DISTRIBUTED BY HASH(`key`) INTO ${PARTITIONS} BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'json-registry',
  'value.fields-include' = 'except-key',
  'changelog.mode' = 'append',
  'kafka.retention.time' = '${CONVERSIONS_RETENTION}',
  'scan.startup.mode' = 'earliest-offset'
);
