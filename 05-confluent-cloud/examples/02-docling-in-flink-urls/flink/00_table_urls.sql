-- The input: one row per document to convert.
--
-- This is the table the user of this example fills in — `scripts/produce_urls.py`
-- writes a handful of public PDFs into it. Raw JSON like every other topic here,
-- so the same DECODE/ENCODE convention holds and there is no Schema Registry
-- subject to keep in step:
--
--     {"doc_id": "2408.09869", "url": "https://arxiv.org/pdf/2408.09869"}
--
-- `doc_id` is optional; when it is absent the chunker derives one from the
-- URL's last path segment. It is here because a URL is a poor key — it changes
-- when a document moves, and the key is what every keyed stage partitions on.
--
-- Each row costs one Docling conversion, billed per page — once: the convert
-- statement reads this topic from where it ended when it was deployed, so a
-- redeploy never converts a URL again. Produce a URL again to re-convert it.
CREATE TABLE IF NOT EXISTS `${URLS_TOPIC}` (
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
