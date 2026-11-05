-- EUR per invoice month, department and category. Upsert.
CREATE TABLE IF NOT EXISTS `${MONTHLY_TOPIC}` (
  `key`        BYTES NOT NULL,
  `month`      STRING,
  `department` STRING,
  `category`   STRING,
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
