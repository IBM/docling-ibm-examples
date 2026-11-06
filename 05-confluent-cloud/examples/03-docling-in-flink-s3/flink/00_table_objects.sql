-- `${OBJECTS_TOPIC}` — what the SQS Source connector writes.
--
-- One record per SQS message. The connector's value is a struct, and with
-- 'output.data.format' = 'JSON' it arrives as schemaless JSON:
--
--     {"Body": "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",
--                \"s3\":{\"bucket\":{\"name\":\"…\"},\"object\":{\"key\":\"inbox/a.pdf\",…}}}]}",
--      "SenderId": "…", "SentTimestamp": 1759752000000, "ApproximateReceiveCount": 1, …}
--
-- The S3 event is a JSON *string* inside the record — the connector does not
-- know or care what is in a message — so it is parsed twice, in
-- 22_view_objects.sql. scripts/backfill.py writes the same shape into the
-- queue for the objects that were already in the bucket, so the two are one
-- stream by the time they get here.
--
-- Raw bytes, like every input topic in this repo, rather than a registered
-- schema: the connector writes plain JSON, and a CREATE TABLE that matched its
-- struct column for column would be a second copy of a schema we do not own.
-- This creates the topic, and the connector waits for it: terraform/main.tf
-- starts the connector only once the tables exist.
CREATE TABLE IF NOT EXISTS `${OBJECTS_TOPIC}` (
  `key` BYTES,
  `val` BYTES
)
DISTRIBUTED BY HASH(`key`) INTO ${PARTITIONS} BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'raw',
  'changelog.mode' = 'append',
  'kafka.retention.time' = '${RETENTION}',
  'scan.startup.mode' = 'earliest-offset'
);
