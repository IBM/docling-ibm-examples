-- The whole pipeline, as one statement: extraction, invoices, EUR, reports.
--
--     submissions ─▶ submitted ─▶ extracted ─┬─▶ extractions
--                    (synthetic | real)       ▼
--     extractions (history) ─────────▶ extracted_all ─▶ invoiced ─┬─▶ invoices
--                                                                  ▼
--     fx.rates ─────────────────────────────────────────▶ converted ─┬─▶ spend
--                                                                    ├─▶ spend.daily
--                                                                    ├─▶ spend.monthly
--                                                                    └─▶ spend.vendors
--
-- One job, six sinks, steps chained through views — in memory, with no Kafka
-- hop between them — and every topic committed at the same checkpoint: an
-- invoice is in the reports one commit (~a minute) after its release, plus
-- the extraction itself. The price of one statement: anything that fails it —
-- a Docling call past Confluent's ~60 s async limit (DoclingExtract answers
-- within 45 s so that it does not), an edit to a report — restarts all of it.
--
-- `./pipeline.sh deploy` starts it reading `submissions` from where the topic
-- ends and `extractions` up to where that one ends: a replaced statement
-- extracts nothing twice and rebuilds every upsert table below from the
-- extractions already made.
--
-- spend.daily is a running total per submission day, not a closed window: a
-- window needs a watermarked time attribute, and after the FX join there is
-- none.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO `${EXTRACTIONS_TOPIC}`
  (`key`, `doc_id`, `url`, `department`, `category`, `submitted_at`, `mode`, `extraction`, `event_time`)
SELECT `key`, `doc_id`, `url`, `department`, `category`, `submitted_at`, `mode`, `extraction`, `event_time`
FROM `${VIEW_EXTRACTED}`;

INSERT INTO `${INVOICES_TOPIC}`
  (`key`, `doc_id`, `status`, `review_reasons`, `department`, `category`, `submitted_at`, `mode`,
   `vendor_name`, `vendor_vat_id`, `invoice_number`, `invoice_date`, `currency`, `subtotal`, `tax_amount`,
   `total_amount`, `line_item_count`, `pages`, `line_items`, `warnings`, `url`, `event_time`)
SELECT
  `key`, `doc_id`, `status`, `review_reasons`, `department`, `category`, `submitted_at`, `mode`,
  `vendor_name`, `vendor_vat_id`, `invoice_number`, `invoice_date`, `currency`, `subtotal`, `tax_amount`,
  `total_amount`, `line_item_count`, `pages`, `line_items`, `warnings`, `url`, `event_time`
FROM `${VIEW_INVOICED}`;

INSERT INTO `${SPEND_TOPIC}`
  (`key`, `doc_id`, `department`, `category`, `vendor_name`, `invoice_number`, `invoice_date`, `invoice_month`,
   `submitted_at`, `mode`, `currency`, `amount`, `fx_rate`, `fx_date`, `amount_eur`, `url`, `event_time`)
SELECT
  `key`, `doc_id`, `department`, `category`, `vendor_name`, `invoice_number`, `invoice_date`, `invoice_month`,
  `submitted_at`, `mode`, `currency`, `amount`, `fx_rate`, `fx_date`, `amount_eur`, `url`, `event_time`
FROM `${VIEW_CONVERTED}`;

INSERT INTO `${DAILY_TOPIC}` (`key`, `day`, `department`, `amount_eur`, `invoices`)
SELECT
  COALESCE(ENCODE(CONCAT(`day`, '|', `department`), 'UTF-8'), X''),
  `day`, `department`, ROUND(SUM(`amount_eur`), 2), COUNT(*)
FROM (
  SELECT SUBSTRING(`submitted_at` FROM 1 FOR 10) AS `day`,
         COALESCE(`department`, 'Unassigned')     AS `department`,
         `amount_eur`
  FROM `${VIEW_CONVERTED}`
)
GROUP BY `day`, `department`;

INSERT INTO `${MONTHLY_TOPIC}` (`key`, `month`, `department`, `category`, `amount_eur`, `invoices`)
SELECT
  COALESCE(ENCODE(CONCAT(`month`, '|', `department`, '|', `category`), 'UTF-8'), X''),
  `month`, `department`, `category`, ROUND(SUM(`amount_eur`), 2), COUNT(*)
FROM (
  SELECT COALESCE(`invoice_month`, 'unknown')  AS `month`,
         COALESCE(`department`, 'Unassigned')  AS `department`,
         COALESCE(`category`, 'Unassigned')    AS `category`,
         `amount_eur`
  FROM `${VIEW_CONVERTED}`
)
GROUP BY `month`, `department`, `category`;

INSERT INTO `${VENDORS_TOPIC}` (`key`, `vendor_name`, `amount_eur`, `invoices`, `last_invoice_date`)
SELECT
  COALESCE(ENCODE(`vendor_name`, 'UTF-8'), X''),
  `vendor_name`, ROUND(SUM(`amount_eur`), 2), COUNT(*), MAX(`invoice_date`)
FROM (
  SELECT COALESCE(`vendor_name`, '(unknown)') AS `vendor_name`, `amount_eur`, `invoice_date`
  FROM `${VIEW_CONVERTED}`
)
GROUP BY `vendor_name`;

END;
