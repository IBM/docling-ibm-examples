-- One row per filing, from its metric rows: what the events are detected on.
--
--     metrics (one row per metric) ──▶ latest per filing and metric ──▶ pivot ──▶ ratios ──▶ financials
--
-- Four steps, none of which calls Docling — the metrics topic is the paid
-- asset, and this is a re-reading of it:
--
-- 1. **Latest row per filing and metric.** A filing produced twice writes its
--    rows twice, and an improved `dc_metrics` may read different figures the
--    second time; the newest wins and replaces the earlier one downstream, as
--    in example 04 (`arrived` is CURRENT_TIMESTAMP, per row in a streaming
--    query — Confluent has no PROCTIME()). Ordering by `$rowtime` instead
--    would wait for a watermark, which an idle topic never moves
--    (docs/05-confluent-cloud/LIMITS.md).
--
-- 2. **Pivot.** `MAX(CASE WHEN metric = … THEN …)` makes the rows of a filing
--    its columns. The failure rows (`conversion_failed`, `no_figures_found`)
--    have no figures and are left out: a filing that gave none has no row here.
--    Every amount is multiplied by its `scale`, so they are dollars and
--    companies can be compared; `<metric>_prior` is the figure beside it, and
--    `<metric>_cmp` says which two periods those are ("September 30, 2026 vs
--    December 31, 2025", from the head of the table).
--
-- 3. **Costs.** `operating_costs` is revenue less operating income: everything
--    the company spent to earn its revenue, cost of sales included. It is a
--    proxy for "operating expenses", which are not read separately.
--
-- 4. **Ratios**, as fractions (0.25 is 25 %), and NULL — not 0, not an error —
--    when an input is missing or the divisor is not positive. A bank or a BDC
--    has no revenue, so it has no margins, and says so by having none.
--
-- Because step 1 retracts the row it replaces, this view is an *updating*
-- result, and what reads it (32_events.sql) writes to an upsert table.
CREATE VIEW `${VIEW_FINANCIALS}` AS
SELECT
  `doc_id`, `company_name`, `cik`, `form`, `accession_number`, `filing_date`, `period_end`, `source_url`,
  `revenue`, `revenue_prior`, `revenue_cmp`,
  `gross_profit`, `gross_profit_prior`, `gross_profit_cmp`,
  `operating_income`, `operating_income_prior`,
  `operating_costs`, `operating_costs_prior`,
  `net_income`, `net_income_prior`, `net_income_cmp`,
  `operating_cash_flow`, `operating_cash_flow_prior`, `operating_cash_flow_cmp`,
  `cash`, `cash_prior`, `cash_cmp`,
  `total_assets`, `total_current_assets`, `total_liabilities`, `total_current_liabilities`, `stockholders_equity`,
  CASE WHEN `revenue_prior` > 0 AND `revenue` IS NOT NULL
       THEN (`revenue` - `revenue_prior`) / `revenue_prior` END                          AS `revenue_growth`,
  CASE WHEN `revenue` > 0 AND `gross_profit` IS NOT NULL
       THEN `gross_profit` / `revenue` END                                               AS `gross_margin`,
  CASE WHEN `revenue_prior` > 0 AND `gross_profit_prior` IS NOT NULL
       THEN `gross_profit_prior` / `revenue_prior` END                                   AS `gross_margin_prior`,
  CASE WHEN `revenue` > 0 AND `net_income` IS NOT NULL
       THEN `net_income` / `revenue` END                                                 AS `net_margin`,
  CASE WHEN `operating_costs_prior` > 0 AND `operating_costs` IS NOT NULL
       THEN (`operating_costs` - `operating_costs_prior`) / `operating_costs_prior` END  AS `cost_growth`,
  CASE WHEN `net_income_prior` <> 0 AND `net_income` IS NOT NULL
       THEN (`net_income` - `net_income_prior`) / ABS(`net_income_prior`) END            AS `net_income_growth`,
  CASE WHEN `operating_cash_flow_prior` <> 0 AND `operating_cash_flow` IS NOT NULL
       THEN (`operating_cash_flow` - `operating_cash_flow_prior`) / ABS(`operating_cash_flow_prior`) END
                                                                                         AS `operating_cash_flow_growth`,
  CASE WHEN `cash_prior` > 0 AND `cash` IS NOT NULL
       THEN (`cash` - `cash_prior`) / `cash_prior` END                                   AS `cash_change`,
  CASE WHEN `stockholders_equity` > 0 AND `total_liabilities` IS NOT NULL
       THEN `total_liabilities` / `stockholders_equity` END                              AS `leverage`,
  CASE WHEN `total_current_liabilities` > 0 AND `total_current_assets` IS NOT NULL
       THEN `total_current_assets` / `total_current_liabilities` END                     AS `current_ratio`
FROM (
  SELECT *,
    CASE WHEN `revenue` > 0 AND `operating_income` IS NOT NULL
         THEN `revenue` - `operating_income` END                                         AS `operating_costs`,
    CASE WHEN `revenue_prior` > 0 AND `operating_income_prior` IS NOT NULL
         THEN `revenue_prior` - `operating_income_prior` END                             AS `operating_costs_prior`
  FROM (
    SELECT
      `doc_id`,
      MAX(`company_name`)      AS `company_name`,
      MAX(`cik`)               AS `cik`,
      MAX(`form`)              AS `form`,
      MAX(`accession_number`)  AS `accession_number`,
      MAX(`filing_date`)       AS `filing_date`,
      MAX(`period_end`)        AS `period_end`,
      MAX(`source_url`)        AS `source_url`,

      MAX(CASE WHEN `metric` = 'revenue'                    THEN `metric_value` * COALESCE(`scale`, 1) END) AS `revenue`,
      MAX(CASE WHEN `metric` = 'revenue'                    THEN `prior_value`  * COALESCE(`scale`, 1) END) AS `revenue_prior`,
      MAX(CASE WHEN `metric` = 'revenue'                    THEN NULLIF(CONCAT_WS(' vs ', `value_column`, `prior_column`), '') END) AS `revenue_cmp`,
      MAX(CASE WHEN `metric` = 'gross_profit'               THEN `metric_value` * COALESCE(`scale`, 1) END) AS `gross_profit`,
      MAX(CASE WHEN `metric` = 'gross_profit'               THEN `prior_value`  * COALESCE(`scale`, 1) END) AS `gross_profit_prior`,
      MAX(CASE WHEN `metric` = 'gross_profit'               THEN NULLIF(CONCAT_WS(' vs ', `value_column`, `prior_column`), '') END) AS `gross_profit_cmp`,
      MAX(CASE WHEN `metric` = 'operating_income'           THEN `metric_value` * COALESCE(`scale`, 1) END) AS `operating_income`,
      MAX(CASE WHEN `metric` = 'operating_income'           THEN `prior_value`  * COALESCE(`scale`, 1) END) AS `operating_income_prior`,
      MAX(CASE WHEN `metric` = 'net_income'                 THEN `metric_value` * COALESCE(`scale`, 1) END) AS `net_income`,
      MAX(CASE WHEN `metric` = 'net_income'                 THEN `prior_value`  * COALESCE(`scale`, 1) END) AS `net_income_prior`,
      MAX(CASE WHEN `metric` = 'net_income'                 THEN NULLIF(CONCAT_WS(' vs ', `value_column`, `prior_column`), '') END) AS `net_income_cmp`,
      MAX(CASE WHEN `metric` = 'operating_cash_flow'        THEN `metric_value` * COALESCE(`scale`, 1) END) AS `operating_cash_flow`,
      MAX(CASE WHEN `metric` = 'operating_cash_flow'        THEN `prior_value`  * COALESCE(`scale`, 1) END) AS `operating_cash_flow_prior`,
      MAX(CASE WHEN `metric` = 'operating_cash_flow'        THEN NULLIF(CONCAT_WS(' vs ', `value_column`, `prior_column`), '') END) AS `operating_cash_flow_cmp`,
      MAX(CASE WHEN `metric` = 'cash'                       THEN `metric_value` * COALESCE(`scale`, 1) END) AS `cash`,
      MAX(CASE WHEN `metric` = 'cash'                       THEN `prior_value`  * COALESCE(`scale`, 1) END) AS `cash_prior`,
      MAX(CASE WHEN `metric` = 'cash'                       THEN NULLIF(CONCAT_WS(' vs ', `value_column`, `prior_column`), '') END) AS `cash_cmp`,
      MAX(CASE WHEN `metric` = 'total_assets'               THEN `metric_value` * COALESCE(`scale`, 1) END) AS `total_assets`,
      MAX(CASE WHEN `metric` = 'total_current_assets'       THEN `metric_value` * COALESCE(`scale`, 1) END) AS `total_current_assets`,
      MAX(CASE WHEN `metric` = 'total_liabilities'          THEN `metric_value` * COALESCE(`scale`, 1) END) AS `total_liabilities`,
      MAX(CASE WHEN `metric` = 'total_current_liabilities'  THEN `metric_value` * COALESCE(`scale`, 1) END) AS `total_current_liabilities`,
      MAX(CASE WHEN `metric` = 'stockholders_equity'        THEN `metric_value` * COALESCE(`scale`, 1) END) AS `stockholders_equity`
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY `doc_id`, `metric` ORDER BY `arrived` DESC) AS `rn`
      FROM (
        SELECT `doc_id`, `company_name`, `cik`, `form`, `accession_number`, `filing_date`, `period_end`, `source_url`,
               `metric`, `metric_value`, `prior_value`, `value_column`, `prior_column`, `scale`,
               CURRENT_TIMESTAMP AS `arrived`
        FROM `${METRICS_TOPIC}`
        WHERE `metric` NOT IN ('conversion_failed', 'no_figures_found')
      )
    )
    WHERE `rn` = 1
    GROUP BY `doc_id`
  )
);
