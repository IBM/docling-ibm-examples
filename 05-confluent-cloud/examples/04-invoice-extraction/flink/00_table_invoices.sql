-- One row per invoice: the pages merged, the fields typed, `status` ok or
-- review with `review_reasons`. The review queue is `status = 'review'`.
--
-- Upsert, keyed by the document id: written by 30_pipeline.sql, and when that
-- statement is replaced it rebuilds this table from the extractions already
-- made, overwriting each invoice instead of adding a second copy.
CREATE TABLE IF NOT EXISTS `${INVOICES_TOPIC}` (
  `key`             BYTES NOT NULL,
  `doc_id`          STRING,
  `status`          STRING,
  `review_reasons`  STRING,
  `department`      STRING,
  `category`        STRING,
  `submitted_at`    STRING,
  `mode`            STRING,
  `vendor_name`     STRING,
  `vendor_vat_id`   STRING,
  `invoice_number`  STRING,
  `invoice_date`    STRING,
  `currency`        STRING,
  `subtotal`        DOUBLE,
  `tax_amount`      DOUBLE,
  `total_amount`    DOUBLE,
  `line_item_count` INT,
  `pages`           INT,
  `line_items`      STRING,
  `warnings`        STRING,
  `url`             STRING,
  `event_time`      TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
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
