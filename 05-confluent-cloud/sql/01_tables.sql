-- 1. Tables (and their topics).
--
-- Run each statement in the Confluent Cloud Flink SQL workspace, in your
-- environment and cluster (the workspace's catalog and database).
-- Creating a table also creates its Kafka topic.

-- The input: Docling for IBM watsonx writes one JSON message per chunk here.
-- Docling writes plain JSON (no Schema Registry), so the value is read as raw
-- bytes and parsed with JSON_VALUE in the pipeline.
CREATE TABLE `docling_chunks` (
  `key` BYTES,
  `val` BYTES
)
DISTRIBUTED BY HASH(`key`) INTO 1 BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'raw',
  'changelog.mode' = 'append',
  'kafka.retention.time' = '1 d',
  'scan.startup.mode' = 'earliest-offset'
);

-- Clean, redacted, deduplicated chunks: ready for embedding and search.
CREATE TABLE `docling_enriched` (
  `chunk_id`     STRING,
  `doc_id`       STRING,
  `chunk_index`  INT,
  `headings`     STRING,
  `text`         STRING,
  `char_count`   INT,
  `pii_found`    BOOLEAN,
  `fingerprint`  STRING
)
WITH ('changelog.mode' = 'append', 'kafka.retention.time' = '1 d');

-- The original text of every chunk in which personal data was found (audit).
CREATE TABLE `docling_quarantine` (
  `chunk_id`  STRING,
  `doc_id`    STRING,
  `text`      STRING
)
WITH ('changelog.mode' = 'append', 'kafka.retention.time' = '1 d');

-- Chunks dropped by the quality gate, with the reason.
CREATE TABLE `docling_rejected` (
  `chunk_id`  STRING,
  `doc_id`    STRING,
  `reason`    STRING,
  `text`      STRING
)
WITH ('changelog.mode' = 'append', 'kafka.retention.time' = '1 d');
