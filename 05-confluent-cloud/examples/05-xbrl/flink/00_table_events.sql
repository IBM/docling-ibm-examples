-- `${EVENTS_TOPIC}` — what changed in a filing, one row per event. Upsert.
--
-- The live feed of the example: a filing comes in, its figures are read, and
-- the changes worth a look are rows here, each with the sentence that says it:
--
--     event_type   net_income_negative | operating_cash_flow_negative | gross_margin_compression
--                  | costs_outpacing_revenue | cash_drop
--     severity     high | medium
--     description  "Cash fell 41% to $12.3M from $20.8M (September 30, 2026 vs December 31, 2025)"
--
-- `value_now` and `value_before` are in `unit` — dollars, or per cent for a
-- margin — and `delta` is the change: per cent of the earlier value for
-- dollars, percentage points for a margin or for the gap between the growth of
-- costs and of revenue. `comparison` names the two periods compared, from the
-- head of the statement the figures were read from.
--
-- Keyed by the filing and the event, so a filing read again replaces its
-- events, and one that no longer has the event — a corrected figure — loses
-- the row (a tombstone). See 32_events.sql for what each event is.
CREATE TABLE IF NOT EXISTS `${EVENTS_TOPIC}` (
  `key`          BYTES NOT NULL,
  `doc_id`       STRING,
  `company_name` STRING,
  `cik`          STRING,
  `form`         STRING,
  `period_end`   STRING,
  `filing_date`  STRING,
  `event_type`   STRING,
  `severity`     STRING,
  `description`  STRING,
  `value_now`    DOUBLE,
  `value_before` DOUBLE,
  `unit`         STRING,
  `delta`        DOUBLE,
  `comparison`   STRING,
  `source_url`   STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
)
DISTRIBUTED BY HASH(`key`) INTO ${PARTITIONS} BUCKETS
WITH (
  'key.format' = 'raw',
  'value.format' = 'json-registry',
  'value.fields-include' = 'except-key',
  'changelog.mode' = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'scan.startup.mode' = 'earliest-offset'
);
