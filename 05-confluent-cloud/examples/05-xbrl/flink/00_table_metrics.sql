-- `${METRICS_TOPIC}` — the figures read from the filings, one row per metric.
-- This is all the pipeline keeps of a conversion: the DoclingDocument itself is
-- 14 MB for a 10-Q, over the 4 MB of a row, so it is read and dropped.
--
--     key    0001023298_000164033426001664:total_assets             (raw bytes)
--     value  {"doc_id": "…", "company_name": "alpha-En Corp", "form": "10-K", "period_end": "2025-12-31",
--             "metric": "total_assets", "label": "Total Assets",
--             "metric_value": 24007.0, "prior_value": 70297.0, "scale": 1, "table_ref": "#/tables/54", …}
--
-- `metric_value` is as the statement prints it; multiply by `scale` (1, 1000 or
-- 1000000, from "in thousands" above the table) for dollars. `prior_value` is the
-- next figure of the row, a comparison period, and `value_column` and
-- `prior_column` say which period each is, in the words of the table's head
-- ("September 30, 2026", "December 31, 2025"): a 10-Q's balance sheet compares
-- with the last year-end, its income statement with the same quarter of last
-- year. `label` is the row's own text, and `table_ref` the table of the
-- DoclingDocument it was read from — so every number can be traced back. See
-- udf/src/main/java/ai/docling/confluent/xbrl/DoclingMetrics.java for what is read and how.
--
-- A filing that gave no figure is one row all the same, since nothing else
-- records it: `metric` is `conversion_failed` (the label says why: a 403, a
-- timeout, …) or `no_figures_found`, and the numbers are NULL.
--
-- Typed (json-registry), so `SELECT metric, metric_value FROM …` works in the
-- SQL workspace. The key is the filing and the metric: a filing produced again
-- lands on the same key.
CREATE TABLE IF NOT EXISTS `${METRICS_TOPIC}` (
  `key`              BYTES,
  `doc_id`           STRING,
  `company_name`     STRING,
  `cik`              STRING,
  `form`             STRING,
  `accession_number` STRING,
  `filing_date`      STRING,
  `period_end`       STRING,
  `metric`           STRING,
  `label`            STRING,
  `metric_value`     DOUBLE,
  `prior_value`      DOUBLE,
  `value_column`     STRING,
  `prior_column`     STRING,
  `scale`            INT,
  `table_ref`        STRING,
  `source_url`       STRING
)
DISTRIBUTED BY HASH(`key`) INTO ${PARTITIONS} BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'json-registry',
  'value.fields-include' = 'except-key',
  'changelog.mode' = 'append',
  'kafka.retention.time' = '${RETENTION}',
  'scan.startup.mode' = 'earliest-offset'
);
