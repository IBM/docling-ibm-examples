-- EUR per vendor, with the latest invoice date. Upsert.
CREATE TABLE IF NOT EXISTS `${VENDORS_TOPIC}` (
  `key`               BYTES NOT NULL,
  `vendor_name`       STRING,
  `amount_eur`        DOUBLE,
  `invoices`          BIGINT,
  `last_invoice_date` STRING,
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
