-- An ok invoice in EUR, at the ECB rate of its invoice date: the feed on the
-- dashboard, and what the three reports sum. Upsert, keyed by document id.
CREATE TABLE IF NOT EXISTS `${SPEND_TOPIC}` (
  `key`            BYTES NOT NULL,
  `doc_id`         STRING,
  `department`     STRING,
  `category`       STRING,
  `vendor_name`    STRING,
  `invoice_number` STRING,
  `invoice_date`   STRING,
  `invoice_month`  STRING,
  `submitted_at`   STRING,
  `mode`           STRING,
  `currency`       STRING,
  `amount`         DOUBLE,
  `fx_rate`        DOUBLE,
  `fx_date`        STRING,
  `amount_eur`     DOUBLE,
  `url`            STRING,
  `event_time`     TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
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
