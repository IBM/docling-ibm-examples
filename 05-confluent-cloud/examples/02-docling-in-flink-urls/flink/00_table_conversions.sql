-- `${CONVERSIONS_TOPIC}` — what Docling answered, one row per URL, before
-- anything is chunked.
--
--     key         https://arxiv.org/pdf/2408.09869               (raw bytes)
--     value       {"doc_id": "2408.09869", "url": "https://…", "status": "success",
--                  "conversion": "{\"status\":\"success\",\"content\":\"# …\",…}"}
--
-- It exists so that a conversion that failed is a *row* rather than an absence.
-- `dc_convert` never throws: a 404, a timeout, an unsupported format all come
-- back as `{"status":"failure","url":"...","error":"..."}`, and this is where
-- they land — `SELECT url, status FROM` this table in the SQL workspace and
-- you see every one. Without it, the only evidence that a URL was bad would be
-- chunks that never appeared, which is indistinguishable from a broken
-- statement.
--
-- It is also the expensive artefact of the whole example: a conversion costs
-- money. So it is kept for good (`${CONVERSIONS_RETENTION}`, whatever RETENTION
-- says), and the chunk topic is rebuilt from it — the convert statement re-reads
-- it, up to where it ended, each time it is replaced (23_view_converted_all.sql).
-- A change to the chunker costs a redeploy, never a second conversion.
--
-- Typed, unlike the chunk topics: `json-registry` makes Flink register a JSON
-- Schema for the value when this runs, so every consumer — the SQL workspace,
-- scripts/report.py — reads columns instead of re-parsing JSON. Which is also
-- why nothing creates the topic ahead of this statement: a topic that exists
-- first is inferred as `key BYTES, val BYTES` and IF NOT EXISTS keeps that.
-- The key stays raw: the URL.
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
