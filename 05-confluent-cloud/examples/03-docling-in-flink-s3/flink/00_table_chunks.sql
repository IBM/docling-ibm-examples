-- `${CHUNKS_TOPIC}` — the seam.
--
-- In example 01 this topic is written by Docling itself, from outside Confluent
-- Cloud. Here it is written by `31_convert.sql`, from inside a Flink statement.
-- The records are the same records — `docling_udf.chunk_record.build` produces
-- both — which is why everything downstream of this table is shared with
-- example 01 rather than copied: `common/flink/20_view_prepared.sql` onwards
-- does not know or care which of the two filled it.
--
-- Same declaration as example 01's, for the same reasons: raw bytes rather than
-- json-registry, because the record is Docling's and Docling writes it
-- schemalessly.
CREATE TABLE IF NOT EXISTS `${CHUNKS_TOPIC}` (
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
