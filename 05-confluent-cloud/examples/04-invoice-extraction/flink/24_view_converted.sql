-- An ok invoice in EUR, at the ECB rate of its invoice date.
--
--     invoiced ──┐
--                ├─▶ converted ──▶ spend, spend.daily, spend.monthly, spend.vendors
--     fx.rates ──┘
--
-- A regular join on (currency, invoice date): the control app releases a day's
-- rates before that day's invoices, so the rate is there when the invoice
-- arrives. Its state is bounded by sql.state-ttl (DEDUP_TTL in env.sh).
CREATE VIEW `${VIEW_CONVERTED}` AS
SELECT
  i.`key`,
  i.`doc_id`,
  i.`department`,
  i.`category`,
  i.`vendor_name`,
  i.`invoice_number`,
  i.`invoice_date`,
  SUBSTRING(i.`invoice_date` FROM 1 FOR 7)   AS `invoice_month`,
  i.`submitted_at`,
  i.`mode`,
  i.`currency`,
  i.`total_amount`                           AS `amount`,
  r.`rate`                                   AS `fx_rate`,
  r.`ecb_date`                               AS `fx_date`,
  ROUND(i.`total_amount` / r.`rate`, 2)      AS `amount_eur`,
  i.`url`,
  i.`event_time`
FROM `${VIEW_INVOICED}` AS i
JOIN (
  -- One rate per currency and day, however often that day was released: a
  -- rate sent twice would otherwise match every invoice of that day twice,
  -- and every report would count it twice.
  SELECT `currency`, `rate_date`, MAX(`rate`) AS `rate`, MAX(`ecb_date`) AS `ecb_date`
  FROM `${FX_TOPIC}`
  GROUP BY `currency`, `rate_date`
) AS r
  ON i.`currency` = r.`currency` AND i.`invoice_date` = r.`rate_date`
WHERE i.`status` = 'ok';
