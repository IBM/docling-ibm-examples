-- One Docling conversion per *new* filing, read for its figures.
--
--     filings (from the deploy on) ──▶ dc_metrics ──▶ extracted ──▶ UNNEST ──▶ metrics
--
-- Read from where the filings topic ended when the statement was deployed
-- (`${FILINGS_FROM_OFFSETS}`, set by ./pipeline.sh deploy), not from the
-- beginning: a redeploy handles only what is produced from then on, and never
-- converts a filing twice. Nothing but the figures is kept, so there is nothing
-- to rebuild them from: to read a filing again, produce it again.
--
-- What is converted is the filing's *inline XBRL* report (`ixbrl_url`, the
-- `.htm` the producer found in the filing's index): the same file a person
-- reads on sec.gov, with the XBRL tagged inside the HTML. Docling for IBM
-- watsonx fetches it itself — `${SEC_USER_AGENT}` is the User-Agent it is told
-- to fetch with, because the SEC answers 403 without one — and converts it as
-- HTML, as there is no way to give the service a taxonomy to read the tags
-- with. So no 6 MB report, and no 14 MB DoclingDocument, ever passes through
-- Flink, where a row is 4 MB.
--
-- A view rather than an inline call so that the async function is a plain
-- projection, and the statement only unnests a column.
CREATE VIEW `${VIEW_EXTRACTED}` AS
SELECT
  `doc_id`,
  `company_name`,
  `cik`,
  `form`,
  `accession_number`,
  `filing_date`,
  `ixbrl_url` AS `url`,
  ${FN_METRICS}(
    '${DOCLING_CONNECTION}',
    '${DOWNLOAD_CONNECTION}',
    `ixbrl_url`,
    '${SEC_USER_AGENT}'
  ) AS `figures`
FROM `${FILINGS_TOPIC}` /*+ OPTIONS('scan.startup.mode' = 'specific-offsets',
  'scan.startup.specific-offsets' = '${FILINGS_FROM_OFFSETS}') */
-- A filing with no inline XBRL report (an older one, or a company that files
-- plain HTML) has nothing to convert.
WHERE `ixbrl_url` IS NOT NULL AND `ixbrl_url` <> '';
