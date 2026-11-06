-- The input filings topic: one row per discovered SEC EDGAR filing report.
--
-- Typed and registered in Schema Registry using json-registry format.
CREATE TABLE IF NOT EXISTS `${FILINGS_TOPIC}` (
  `key`                   BYTES,
  `doc_id`                STRING,
  `company_name`          STRING,
  `cik`                   STRING,
  `cik_short`             STRING,
  `form`                  STRING,
  `accession_number`      STRING,
  `accession_number_raw`  STRING,
  `filing_date`           STRING,
  `updated`               STRING,
  `size`                  STRING,
  `filing_details_url`    STRING,
  `filing_index_url`      STRING,
  `filing_index_json_url` STRING,
  `xbrl_zip_url`          STRING,
  `ixbrl_file`            STRING,
  `ixbrl_url`             STRING,
  `ixbrl_size`            BIGINT,
  `xbrl_instance_file`    STRING,
  `xbrl_instance_url`     STRING,
  `xbrl_instance_size`    BIGINT,
  `taxonomy_files`        ARRAY<ROW<`name` STRING, `type` STRING, `url` STRING, `size` BIGINT>>,
  `taxonomy_urls`         ARRAY<STRING>
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
