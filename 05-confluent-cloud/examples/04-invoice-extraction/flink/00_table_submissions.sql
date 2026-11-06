-- The input: one row per invoice an employee submits for approval.
--
--     key    lumen-inv-0042                                  (raw bytes)
--     value  {"doc_id": "lumen-inv-0042", "url": "https://…/lumen-inv-0042.pdf",
--             "department": "Sales", "category": "Travel",
--             "submitted_at": "2026-02-03T09:14:07Z",
--             "mode": "real"}                                 (JSON Schema)
--
-- `mode` is chosen per release, live, on the control app's page: `synthetic`
-- answers from the ground truth (free, instant), `real` calls Docling; the
-- pipeline sends each to its function (21_view_extracted.sql).
--
-- Typed, and the type is registered: `json-registry` makes Flink register a
-- JSON Schema subject for the value when this statement runs, and the control
-- app produces against that subject (Confluent's wire format: a magic byte,
-- the schema id, the JSON). Every topic in this example carries a schema —
-- the dashboard decodes by it, and the Real-Time Context Engine requires one.
-- The key stays raw bytes — the engine's upsert mode wants it that way.
--
-- The Kafka record timestamp is the point: the control app sets it to
-- `submitted_at`, the *simulated* time, so `$rowtime` is the simulation's
-- clock and everything event-time downstream runs on it.
--
-- Created by Flink, not ahead of it: this statement is what creates the topic
-- and registers its schema, and the control app writes to it afterwards.
CREATE TABLE IF NOT EXISTS `${SUBMISSIONS_TOPIC}` (
  `key`          BYTES,
  `doc_id`       STRING,
  `url`          STRING,
  `department`   STRING,
  `category`     STRING,
  `submitted_at` STRING,
  `mode`         STRING
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
