-- 2. The pipeline: two views and one long-running statement.
--
-- Run the two CREATE VIEW statements, then the EXECUTE STATEMENT SET.
-- The statement keeps running and processes every new chunk as it arrives.

-- Parse each Docling chunk and normalize its text.
CREATE VIEW `docling_prepared` AS
SELECT
  JSON_VALUE(`payload`, '$.chunk_id')                       AS `chunk_id`,
  JSON_VALUE(`payload`, '$.doc_id')                         AS `doc_id`,
  CAST(JSON_VALUE(`payload`, '$.chunk_index') AS INT)       AS `chunk_index`,
  COALESCE(JSON_QUERY(`payload`, '$.headings'),
           JSON_QUERY(`payload`, '$.metadata.headings'))     AS `headings`,
  TRIM(REGEXP_REPLACE(JSON_VALUE(`payload`, '$.text'), '\s+', ' ')) AS `text`,
  `event_time`
FROM (
  SELECT DECODE(`val`, 'UTF-8') AS `payload`, `$rowtime` AS `event_time`
  FROM `docling_chunks`
)
WHERE JSON_VALUE(`payload`, '$.text') IS NOT NULL;

-- Detect and redact personal data, measure the chunk, and fingerprint it.
-- The patterns are deliberately simple: e-mail addresses, phone numbers
-- (international with a leading +, or 555-010-2030 style) and IBANs.
-- Tune them for your documents.
CREATE VIEW `docling_guarded` AS
SELECT
  `chunk_id`,
  `doc_id`,
  `chunk_index`,
  `headings`,
  `text` AS `original_text`,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(`text`,
        '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', '[EMAIL]'),
      '\+[0-9][0-9 ().-]{7,}[0-9]|\(?[0-9]{3}\)?[ .-][0-9]{3}[ .-][0-9]{4}', '[PHONE]'),
    '\b[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}\b', '[IBAN]')        AS `text`,
  REGEXP(`text`, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
    OR REGEXP(`text`, '\+[0-9][0-9 ().-]{7,}[0-9]|\(?[0-9]{3}\)?[ .-][0-9]{3}[ .-][0-9]{4}')
    OR REGEXP(`text`, '\b[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}\b') AS `pii_found`,
  CHAR_LENGTH(`text`)                                       AS `char_count`,
  MD5(LOWER(`text`))                                        AS `fingerprint`,
  `event_time`
FROM `docling_prepared`;

-- One statement, three outputs.
EXECUTE STATEMENT SET
BEGIN

  -- Clean chunks, without duplicates: identical text is kept once.
  INSERT INTO `docling_enriched`
  SELECT `chunk_id`, `doc_id`, `chunk_index`, `headings`, `text`,
         `char_count`, `pii_found`, `fingerprint`
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY `fingerprint` ORDER BY `event_time`) AS `rn`
    FROM `docling_guarded`
    WHERE `char_count` >= 40
  )
  WHERE `rn` = 1;

  -- The original text of chunks that contained personal data.
  INSERT INTO `docling_quarantine`
  SELECT `chunk_id`, `doc_id`, `original_text`
  FROM `docling_guarded`
  WHERE `pii_found`;

  -- Chunks too short to be useful for retrieval.
  INSERT INTO `docling_rejected`
  SELECT `chunk_id`, `doc_id`, 'too_short', `text`
  FROM `docling_guarded`
  WHERE `char_count` < 40;

END;
