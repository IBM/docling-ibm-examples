-- ECB euro reference rates, one record per currency per *calendar* day.
--
--     key    USD
--     value  {"currency": "USD", "rate_date": "2026-02-07", "rate": 1.1798,
--             "ecb_date": "2026-02-06"}
--
-- `rate` is units of the currency per euro, as the ECB publishes it, so an
-- amount converts as `amount / rate`. The ECB publishes on TARGET business days
-- only; the control app fills weekends and holidays forward from the last
-- publication and keeps the real date in `ecb_date`. That is what makes the
-- conversion a plain equi-join on (currency, invoice date) — "the reference
-- rate applicable on that day" — instead of an as-of lookup. EUR itself is a
-- row too, at 1.0, so EUR invoices take the same path.
--
-- Released by the control app on the same clock as the submissions, a day at a
-- time and *before* that day's submissions. The source is the real ECB series
-- for November 2025 to June 2026, in `data/ecb_rates.csv`.
CREATE TABLE IF NOT EXISTS `${FX_TOPIC}` (
  `key`       BYTES,
  `currency`  STRING,
  `rate_date` STRING,
  `rate`      DOUBLE,
  `ecb_date`  STRING
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
