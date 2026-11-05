-- `${QUARANTINE_TOPIC}` — the *untouched original* of any chunk the guard
-- found PII in. Restrict its ACLs: this is the only topic that still carries
-- the matched values.
--
-- Same shape as the chunk topic on purpose: the record stays a JSON string all
-- the way through, so a consumer written against the chunk topic reads this one
-- too, and the enrichment shows up as fields that were not there before.
CREATE TABLE IF NOT EXISTS `${QUARANTINE_TOPIC}` (
  -- Both the names and the types are the raw format's, not ours. The names:
  -- "If a value is provided with a 'raw' format it must use 'val' as the
  -- column name", and likewise `key`. The types: raw carries bytes, and with
  -- no Schema Registry entry to say otherwise there is nothing to infer a
  -- STRING from — declaring one anyway is accepted and then resolves to BYTES,
  -- which surfaces much later as a UDF called with the wrong argument type.
  -- So the records are decoded and encoded explicitly at the boundaries; see
  -- 20_view_prepared.sql and 30_pipeline.sql.
  `key` BYTES,
  `val` BYTES
)
DISTRIBUTED BY HASH(`key`) INTO ${PARTITIONS} BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'raw',
  'changelog.mode' = 'append',
  -- Explicit because the default is 0, and 0 here means *infinite* retention.
  'kafka.retention.time' = '${RETENTION}',
  'scan.startup.mode' = 'earliest-offset'
);
