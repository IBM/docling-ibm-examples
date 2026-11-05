-- The chunk topic: what Docling's `kafka_chunks` target writes, one message
-- per chunk. Flink only ever reads it.
--
-- Raw, not `json-registry`, and that is the whole reason this file exists
-- rather than letting Confluent Cloud infer the table. Docling writes plain
-- JSON with nothing in Schema Registry, so an inferred table would be
-- `key BYTES, val BYTES` and every statement would open with a DECODE. Raw
-- with a STRING column gives the same bytes already decoded — and it keeps the
-- record opaque to SQL, so the UDFs alone decide its shape.
--
-- If the topic already exists — because Docling wrote it before you ran this —
-- this statement fails, and the two ALTERs in `00_alter_chunks.sql` are what
-- you want instead.
CREATE TABLE IF NOT EXISTS `${CHUNKS_TOPIC}` (
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
  -- Read from the earliest offset, so a document uploaded before deploying is
  -- still seen.
  'scan.startup.mode' = 'earliest-offset'
);
