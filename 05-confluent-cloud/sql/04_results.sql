-- 4. Look at the results. Each query keeps running and shows new rows as they
--    arrive; stop it when you have seen enough.

SELECT `doc_id`, `chunk_index`, `char_count`, `pii_found`, `text`
FROM `docling_enriched`;

SELECT `doc_id`, `chunk_id`, `text`
FROM `docling_quarantine`;

SELECT `doc_id`, `chunk_id`, `reason`, `text`
FROM `docling_rejected`;

-- The raw messages Docling wrote:
SELECT DECODE(`val`, 'UTF-8') AS `message`
FROM `docling_chunks`;
