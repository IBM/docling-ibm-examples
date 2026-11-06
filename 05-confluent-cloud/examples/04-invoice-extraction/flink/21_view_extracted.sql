-- The extraction of every newly released invoice, by its mode.
--
--     submitted ──┬─ mode real ──────▶ extract_real(…)      ──┐
--                 └─ otherwise ──────▶ extract_synthetic(…) ──┴─▶ extracted
--
-- Two branches of one UNION ALL, so a synthetic invoice never reaches the
-- Docling call and a real one never the synthetic answer. Both functions take
-- the same arguments and return Docling's ExtractionDocumentResult.
--
-- Read twice in 30_pipeline.sql — written to `extractions`, and carried on to
-- the invoices and reports — and computed once: the statement set reuses the
-- sub-plan, so each invoice is extracted (and billed) once.
CREATE VIEW `${VIEW_EXTRACTED}` AS
SELECT `key`, `doc_id`, `url`, `department`, `category`, `submitted_at`, `mode`,
       ${FN_EXTRACT_REAL}('${DOCLING_CONNECTION}', '${DOWNLOAD_CONNECTION}', `url`, `extraction_target`) AS `extraction`,
       `event_time`
FROM `${VIEW_SUBMITTED}`
WHERE `mode` = 'real'
UNION ALL
SELECT `key`, `doc_id`, `url`, `department`, `category`, `submitted_at`, `mode`,
       ${FN_EXTRACT_SYNTHETIC}('${DOCLING_CONNECTION}', '${DOWNLOAD_CONNECTION}', `url`, `extraction_target`) AS `extraction`,
       `event_time`
FROM `${VIEW_SUBMITTED}`
WHERE `mode` <> 'real';
