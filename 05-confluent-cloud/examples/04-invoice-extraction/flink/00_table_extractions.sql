-- What Docling answered, one row per submission — the expensive artefact.
--
--     submissions ──▶ 30_pipeline.sql ──▶ extractions
--
-- Append-only, and kept for good: every extraction is a billed call. The
-- pipeline carries its new extractions on to the reports in memory, and reads
-- this topic only when it is replaced — to rebuild every report from what was
-- already extracted, for free (22_view_extracted_all.sql). It is also what the
-- dashboard's trace and the Context Engine show. `extraction` is Docling's
-- ExtractionDocumentResult (or the synthetic one, in the same shape) as JSON.
--
-- `event_time` is the Kafka timestamp, *written*: the submission's simulated
-- time, carried onto this topic so the clock survives.
CREATE TABLE IF NOT EXISTS `${EXTRACTIONS_TOPIC}` (
  `key`          BYTES,
  `doc_id`       STRING,
  `url`          STRING,
  `department`   STRING,
  `category`     STRING,
  `submitted_at` STRING,
  `mode`         STRING,
  `extraction`   STRING,
  `event_time`   TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
)
DISTRIBUTED BY HASH(`key`) INTO ${PARTITIONS} BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'json-registry',
  'value.fields-include' = 'except-key',
  'changelog.mode' = 'append',
  'kafka.retention.time' = '${RETENTION}',
  'scan.startup.mode' = 'earliest-offset'
);
