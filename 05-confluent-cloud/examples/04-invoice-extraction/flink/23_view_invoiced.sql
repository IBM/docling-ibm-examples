-- One invoice record per extraction: invoice() merges the pages and decides ok
-- or review; this unpacks its JSON into columns.
--
--     extracted_all ──▶ invoiced ──▶ converted
--
-- A view, inside 30_pipeline.sql's job: the invoices table and every report
-- are computed from it in one statement, and invoice() runs once per row
-- (the statement set reuses the sub-plan).
CREATE VIEW `${VIEW_INVOICED}` AS
SELECT
  COALESCE(`key`, X'')                                         AS `key`,
  `doc_id`,
  JSON_VALUE(`record`, '$.status')                             AS `status`,
  JSON_QUERY(`record`, '$.review_reasons')                     AS `review_reasons`,
  `department`,
  `category`,
  `submitted_at`,
  `mode`,
  JSON_VALUE(`record`, '$.vendor_name')                        AS `vendor_name`,
  JSON_VALUE(`record`, '$.vendor_vat_id')                      AS `vendor_vat_id`,
  JSON_VALUE(`record`, '$.invoice_number')                     AS `invoice_number`,
  JSON_VALUE(`record`, '$.invoice_date')                       AS `invoice_date`,
  JSON_VALUE(`record`, '$.currency')                           AS `currency`,
  JSON_VALUE(`record`, '$.subtotal' RETURNING DOUBLE)          AS `subtotal`,
  JSON_VALUE(`record`, '$.tax_amount' RETURNING DOUBLE)        AS `tax_amount`,
  JSON_VALUE(`record`, '$.total_amount' RETURNING DOUBLE)      AS `total_amount`,
  JSON_VALUE(`record`, '$.line_item_count' RETURNING INT)      AS `line_item_count`,
  JSON_VALUE(`record`, '$.pages' RETURNING INT)                AS `pages`,
  JSON_QUERY(`record`, '$.line_items')                         AS `line_items`,
  JSON_QUERY(`record`, '$.warnings')                           AS `warnings`,
  `url`,
  `event_time`
FROM (
  SELECT
    `key`, `doc_id`, `department`, `category`, `submitted_at`, `mode`, `url`, `event_time`,
    ${FN_INVOICE}(`extraction`, JSON_OBJECT(
      KEY 'doc_id'       VALUE `doc_id`,
      KEY 'url'          VALUE `url`,
      KEY 'department'   VALUE `department`,
      KEY 'category'     VALUE `category`,
      KEY 'submitted_at' VALUE `submitted_at`
    )) AS `record`
  FROM `${VIEW_EXTRACTED_ALL}`
);
