-- Every conversion the chunks are made from: those of earlier deploys, from the
-- conversions topic, and the new ones, as they happen.
--
--     conversions (up to the deploy) ──┐
--                                      ├──▶ converted_all ──▶ dc_chunks ──▶ chunks
--     converted   (from the deploy) ───┘
--
-- Example 02's view, word for word in effect: a redeploy re-chunks every
-- object already converted, for free, with whatever chunker is deployed now.
-- Read example 02's 23_view_converted_all.sql for the reasoning.
CREATE VIEW `${VIEW_CONVERTED_ALL}` AS
SELECT `doc_id`, `url`, `conversion`
FROM `${CONVERSIONS_TOPIC}` /*+ OPTIONS('scan.bounded.mode' = 'specific-offsets',
  'scan.bounded.specific-offsets' = '${CONVERSIONS_UNTIL_OFFSETS}') */
WHERE `status` = 'success'
UNION ALL
SELECT `doc_id`, `url`, `converted` AS `conversion`
FROM `${VIEW_CONVERTED}`;
