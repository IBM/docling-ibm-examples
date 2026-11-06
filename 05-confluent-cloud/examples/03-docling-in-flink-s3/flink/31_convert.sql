-- The front half — the statement that presigns and calls Docling.
--
--     objects topic ──▶ objects ──▶ converted ──┬──────────────────────────────▶ conversions
--                       (decoded)   dc_presign  │
--                                   dc_convert  └──▶ converted_all ──▶ dc_chunks ──▶ UNNEST ──▶ chunks
--     conversions (history) ───────────────────────┘
--
-- and then `30_pipeline.sql`, example 01's, reads the chunk topic and knows
-- nothing about buckets.
--
-- Example 02's statement with a different source view. Everything said in
-- its copy of this file holds — the seam at the chunk topic, the async call
-- and its 45 s answer, nothing converted twice on a redeploy — so it is not
-- repeated here.
--
-- What is different is latency at the front: an upload reaches this statement
-- through S3's notification (usually under a second), SQS, and the
-- connector's poll, so expect a few seconds before the conversion starts,
-- then the minute-ish commit of each statement as in example 02.
EXECUTE STATEMENT SET
BEGIN

-- 1. every conversion, successful or not, keyed by the object's s3:// URI.
INSERT INTO `${CONVERSIONS_TOPIC}` (`key`, `doc_id`, `url`, `status`, `conversion`)
SELECT
  ENCODE(`url`, 'UTF-8')               AS `key`,
  `doc_id`,
  `url`,
  JSON_VALUE(`converted`, '$.status')  AS `status`,
  `converted`                          AS `conversion`
FROM `${VIEW_CONVERTED}`;

-- 2. the chunks — of the new conversions and of the ones already on the topic.
--    dc_chunks is given the s3:// URI, so that is the chunk's `uri`, and the
--    key as the doc_id.
INSERT INTO `${CHUNKS_TOPIC}`
SELECT
  ENCODE(JSON_VALUE(`chunk`, '$.chunk_id'), 'UTF-8') AS `key`,
  ENCODE(`chunk`, 'UTF-8')                           AS `val`
FROM `${VIEW_CONVERTED_ALL}`
CROSS JOIN UNNEST(${FN_CHUNKS}(`conversion`, `url`, `doc_id`)) AS `t`(`chunk`);

END;
