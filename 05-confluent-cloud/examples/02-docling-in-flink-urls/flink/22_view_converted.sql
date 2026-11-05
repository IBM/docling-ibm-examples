-- One Docling conversion per *new* URL.
--
--     urls (from the deploy on) ──▶ dc_convert ──▶ converted ──┬──▶ conversions
--                                                               └──▶ converted_all ──▶ chunks
--
-- Read from where the URL topic ended when the convert statement was deployed
-- (`${URLS_FROM_OFFSETS}`, set by ./pipeline.sh deploy), not from the
-- beginning: a redeploy converts only what is produced from then on, and never
-- a URL twice. What was converted before comes back from the conversions topic
-- instead — see 23_view_converted_all.sql. To convert a URL again, produce it
-- again.
--
-- A view rather than an inline call because `31_convert.sql` reads it twice —
-- into the conversions topic, and on to the chunks — and a statement set plans
-- its branches together and reuses the shared sub-plan. Inline, `dc_convert`
-- would run twice per URL: not a wasted CPU cycle but a second billed
-- conversion.
--
-- The connection names are literals from env.sh rather than constants in the
-- Java: every name in this repo is derived from NS, and a UDF that hardcoded
-- one would only work for whoever wrote it.
CREATE VIEW `${VIEW_CONVERTED}` AS
SELECT
  `doc_id`,
  `url`,
  ${FN_CONVERT}(
    '${DOCLING_CONNECTION}',
    '${DOWNLOAD_CONNECTION}',
    `url`,
    '${DOCLING_FORMAT}'
  ) AS `converted`
FROM (
  SELECT
    -- doc_id is optional in the record; NULL here is fine, and the chunker
    -- falls back to the URL's last path segment.
    JSON_VALUE(`payload`, '$.doc_id') AS `doc_id`,
    JSON_VALUE(`payload`, '$.url')    AS `url`
  FROM (
    SELECT DECODE(`val`, 'UTF-8') AS `payload`
    FROM `${URLS_TOPIC}` /*+ OPTIONS('scan.startup.mode' = 'specific-offsets',
      'scan.startup.specific-offsets' = '${URLS_FROM_OFFSETS}') */
  )
)
-- A row with no URL is not worth a conversion, and `dc_convert` would only
-- return a failure envelope for it.
WHERE `url` IS NOT NULL AND `url` <> '';
