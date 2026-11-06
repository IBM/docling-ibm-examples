-- Every conversion the chunks are made from: those of earlier deploys, from the
-- conversions topic, and the new ones, as they happen.
--
--     conversions (up to the deploy) ──┐
--                                      ├──▶ converted_all ──▶ dc_chunks ──▶ chunks
--     converted   (from the deploy) ───┘
--
-- The topic is read *bounded*: up to where it ended when the convert statement
-- was deployed (`${CONVERSIONS_UNTIL_OFFSETS}`), and not past it — past it are
-- this statement's own writes, which arrive here directly. So a redeploy
-- re-chunks every document already converted, for free, with whatever chunker
-- is deployed now. That is what makes the conversions topic, not the chunk
-- topic, the thing worth keeping — the lesson of example 04, where the
-- extractions topic plays the same part for the reports.
--
-- Re-chunking an unchanged document produces the same records — `chunk_id` and
-- `fingerprint` are hashes of the content — and the enrichment's dedup collapses
-- them (within DEDUP_TTL). The quarantine topic, an audit of deliveries, sees
-- them again.
CREATE VIEW `${VIEW_CONVERTED_ALL}` AS
SELECT `doc_id`, `url`, `conversion`
FROM `${CONVERSIONS_TOPIC}` /*+ OPTIONS('scan.bounded.mode' = 'specific-offsets',
  'scan.bounded.specific-offsets' = '${CONVERSIONS_UNTIL_OFFSETS}') */
WHERE `status` = 'success'
UNION ALL
SELECT `doc_id`, `url`, `converted` AS `conversion`
FROM `${VIEW_CONVERTED}`;
