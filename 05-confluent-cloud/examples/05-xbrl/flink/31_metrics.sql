-- The pipeline — one statement, from a filing to its figures.
--
--     filings ──▶ dc_metrics ──▶ UNNEST ──▶ metrics
--                 (Docling, then the tables of the DoclingDocument)
--
-- No conversion is stored: `dc_metrics` converts the filing, reads the figures
-- from its tables and returns only those (22_view_extracted.sql). The
-- DoclingDocument of a 10-Q is 14 MB and a row is 4 MB, so it could not be
-- handed from one function to the next, nor written to a topic.
--
-- ## What replacing this statement costs
--
-- It reads the filings topic from where it ended at the deploy, so no filing
-- is converted twice. The price of keeping nothing: an improved `dc_metrics`
-- reaches only the filings produced after it, and a filing is read again by
-- producing it again — and paying for it again.
--
-- ## A failure is a row
--
-- `dc_metrics` always returns at least one record. A filing that could not be
-- converted (a 403 from the SEC, a timeout) or that has none of the known
-- rows comes out as one row whose `metric` is `conversion_failed` or
-- `no_figures_found`, and whose `label` says why:
--
--     SELECT company_name, form, label FROM metrics WHERE metric = 'conversion_failed'
--
-- ## Latency
--
-- `dc_metrics` is an *asynchronous* function: several conversions in flight,
-- results in input order, and each answered within 45 s — Confluent Cloud fails
-- the whole statement when an async call passes about a minute. A filing of a
-- few hundred kB converts in 3–5 s on Docling for IBM watsonx (2026-11-04); the
-- largest ones, 6 MB, take longer, and one that runs out is a
-- `conversion_failed` row, not a failed statement.
--
-- Rows written to Kafka become visible at the statement's checkpoint, about
-- once a minute.
--
-- ## Why UNNEST
--
-- `dc_metrics` returns ARRAY<STRING> because a scalar function is one row out.
-- `CROSS JOIN UNNEST` turns the array into rows, which is what a table function
-- would have done, and says so in plain SQL.
INSERT INTO `${METRICS_TOPIC}`
  (`key`, `doc_id`, `company_name`, `cik`, `form`, `accession_number`, `filing_date`, `period_end`,
   `metric`, `label`, `metric_value`, `prior_value`, `value_column`, `prior_column`, `scale`, `table_ref`,
   `source_url`)
SELECT
  ENCODE(CONCAT(`doc_id`, ':', JSON_VALUE(`m`, '$.metric')), 'UTF-8')   AS `key`,
  `doc_id`,
  `company_name`,
  `cik`,
  `form`,
  `accession_number`,
  `filing_date`,
  JSON_VALUE(`m`, '$.period_end')                                       AS `period_end`,
  JSON_VALUE(`m`, '$.metric')                                           AS `metric`,
  JSON_VALUE(`m`, '$.label')                                            AS `label`,
  CAST(JSON_VALUE(`m`, '$.value') AS DOUBLE)                            AS `metric_value`,
  CAST(JSON_VALUE(`m`, '$.prior_value') AS DOUBLE)                      AS `prior_value`,
  JSON_VALUE(`m`, '$.value_column')                                     AS `value_column`,
  JSON_VALUE(`m`, '$.prior_column')                                     AS `prior_column`,
  CAST(JSON_VALUE(`m`, '$.scale') AS INT)                               AS `scale`,
  JSON_VALUE(`m`, '$.table')                                            AS `table_ref`,
  `url`                                                                 AS `source_url`
FROM `${VIEW_EXTRACTED}`
CROSS JOIN UNNEST(`figures`) AS `t`(`m`);
