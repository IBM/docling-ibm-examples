-- EUR per submission day and department. Upsert: a day's row is rewritten as
-- its invoices arrive (a running total, not a closed window).
CREATE TABLE IF NOT EXISTS `${DAILY_TOPIC}` (
  `key`        BYTES NOT NULL,
  `day`        STRING,
  `department` STRING,
  `amount_eur` DOUBLE,
  `invoices`   BIGINT,
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
