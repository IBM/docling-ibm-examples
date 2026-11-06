-- The front half — the statement that calls Docling.
--
--     urls ──▶ converted ──┬──────────────────────────────▶ conversions   (what Docling answered)
--              dc_convert  │
--                          └──▶ converted_all ──▶ dc_chunks ──▶ UNNEST ──▶ chunks
--     conversions (history) ──┘
--
-- and then `30_pipeline.sql`, which is example 01's, reads that chunk topic and
-- knows nothing about any of this.
--
-- Two statements rather than one, and that is a deliberate seam. A conversion
-- is the slow, billed part; the enrichment is cheap and is the part you will
-- change. Splitting them at the chunk topic means editing a PII rule redeploys
-- the second statement and leaves this one running — `./pipeline.sh deploy`
-- replaces only a statement whose fingerprint changed — and it makes examples
-- 01 and 02 differ in exactly one place.
--
-- ## What replacing this statement costs: no conversion
--
-- It reads the URL topic from where it ended at the deploy, so no URL is
-- converted twice; and it rebuilds the chunks from the conversions already on
-- their topic (23_view_converted_all.sql), so a new chunker reaches every
-- document without calling Docling. The same move as example 04's pipeline,
-- which rebuilds its reports from the extractions.
--
-- ## Latency
--
-- `dc_convert` is an *asynchronous* function: several conversions in flight,
-- results in input order, and each answered within 45 s — because Confluent
-- Cloud fails the whole statement when an async call passes about a minute. A
-- conversion takes one to four seconds on Docling for IBM watsonx (2026-10-06),
-- so that is headroom, not a target; one that does run out is a failure row on
-- the conversions topic, not a failed statement. The first, synchronous
-- version had its batches cut off by the per-batch timeout and run again —
-- every conversion in them billed again. docs/05-confluent-cloud/LIMITS.md has both limits.
--
-- Then the commit: rows written to Kafka become visible at the statement's
-- checkpoint, about once a minute. URL → enriched is two hops (this statement,
-- then the pipeline), so a minute or two, plus the conversion.
--
-- ## Why UNNEST
--
-- `dc_chunks` returns ARRAY<STRING> because Confluent Cloud's Python UDFs are
-- scalar-only — a table function would have to be Java. `CROSS JOIN UNNEST`
-- turns the array into rows, which is what a UDTF would have done, and says so
-- in plain SQL.
EXECUTE STATEMENT SET
BEGIN

-- 1. every conversion, successful or not, kept as it came back.
--    Keyed by URL so a re-conversion of the same document lands on the same
--    partition.
INSERT INTO `${CONVERSIONS_TOPIC}` (`key`, `doc_id`, `url`, `status`, `conversion`)
SELECT
  ENCODE(`url`, 'UTF-8')               AS `key`,
  `doc_id`,
  `url`,
  JSON_VALUE(`converted`, '$.status')  AS `status`,
  `converted`                          AS `conversion`
FROM `${VIEW_CONVERTED}`;

-- 2. the chunks, which is what example 01's pipeline reads — of the new
--    conversions and of the ones already on the topic.
--    A failed conversion contributes an empty array here, and UNNEST of an
--    empty array is no rows — so the failure shows up on the conversions topic
--    above and nowhere else, which is what you want.
INSERT INTO `${CHUNKS_TOPIC}`
SELECT
  ENCODE(JSON_VALUE(`chunk`, '$.chunk_id'), 'UTF-8') AS `key`,
  ENCODE(`chunk`, 'UTF-8')                           AS `val`
FROM `${VIEW_CONVERTED_ALL}`
CROSS JOIN UNNEST(${FN_CHUNKS}(`conversion`, `url`, `doc_id`)) AS `t`(`chunk`);

END;
